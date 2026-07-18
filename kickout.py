#!/usr/bin/env python3
"""kickout.py
==============
Disk-space guard for Transmission pumps. Runs forever, polling each pump
every N seconds (default 15). When a pump's free space drops below the
reserve (default 50 GB), it evicts one torrent to make room. Otherwise
it just watches.

Per-pass behavior:
  - For each pump: ping the RPC, then print a one-line status
    (free space, torrent count, reserve threshold; flags LOW when below).
  - For each alive pump below the reserve: run evict_if_needed.

Pump health tracking:
  - A pump is marked degraded after DEGRADED_FAIL_THRESHOLD (3)
    consecutive RPC failures, and dead after DEGRADED_DEAD_S (24h) in
    degraded state. Dead pumps are skipped; alive pumps are polled.

Free-space cache:
  - Each pump's free-space reading is cached for 60s to avoid hammering
    the RPC. The cache is invalidated immediately after a successful
    removal so the next poll sees the new figure.

Victim selection (ported from orchestrator.py, simplified — no
self.active tracking since kickout runs standalone and doesn't know
which torrents are "being propagated"):

  1. Stopped torrents (status 0) are the strongest candidates — a torrent
     that isn't running isn't serving anyone. Oldest stopped wins.
  2. Otherwise, among running torrents, pick the one with the lowest
     average upload rate (uploadedEver / secondsSeeding) — old and not
     pulled anymore — tiebroken by oldest dateAdded.
  3. Torrents younger than EVICT_MIN_AGE_S (6h) are never evicted, so a
     freshly added release can't be yanked before it has had a chance.

A per-pump cooldown (EVICT_COOLDOWN_S = 120s) prevents cascading
evictions within one pass: Transmission's download-dir-free-space figure
lags for seconds-to-minutes after a large delete, so without a cooldown
each poll would still see <50 GB free and re-evict. The free-space cache
is invalidated immediately after a successful removal.

The actual stop+remove runs in a background thread (guarded by
pump._evicting) so a slow multi-GB delete — which can block the RPC for
>30s on remote pumps — doesn't stall polls of other pumps. Removal uses
a 600s timeout and is preceded by a stop+2s wait so transmission can
flush .resume state (a straight remove+delete on a 0-byte disk can wedge
the daemon).

Usage:
    python kickout.py pumps.txt
    python kickout.py pumps.txt --interval 30 --reserve 80

pumps.txt format (same as orchestrator / fan-out):
    # user,pass,IP,region,port
    customer,PASSWORD,IP,NA,443
    ubuntu,PASSWORD,IP,EU,443

Exit codes:
    0  clean shutdown (Ctrl-C) after at least one pump was loaded
    1  pumps file missing, unreadable, or contained no valid entries
"""

import argparse
import sys
import threading
import time

from transrpc import TransClient, TransRpcError


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

FREE_RESERVE_GB = 50           # evict when free space drops below this
EVICT_COOLDOWN_S = 120         # min time between evictions on one pump
EVICT_MIN_AGE_S = 6 * 3600     # never evict a torrent younger than 6h
POLL_INTERVAL_S = 15           # poll interval
DEGRADED_FAIL_THRESHOLD = 3
DEGRADED_DEAD_S = 24 * 3600

# Per-torrent fields fetched for eviction candidate selection.
EVICT_FIELDS = [
    "id", "name", "hashString", "status", "uploadedEver", "totalSize",
    "percentDone", "addedDate", "secondsSeeding",
]

# Transmission status enum:
#   0 stopped, 1 check-wait, 2 checking, 3 download-wait (queued),
#   4 downloading, 5 seed-wait, 6 seeding
STATUS_STOPPED = 0


# ---------------------------------------------------------------------------
# ANSI colors
# ---------------------------------------------------------------------------

class C:
    R = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    CYAN = "\033[36m"
    GRAY = "\033[90m"


def cprint(color, msg):
    print(f"{color}{msg}{C.R}")
    sys.stdout.flush()


def ts():
    return time.strftime("%H:%M:%S")


NAME_W = 22   # column width for [pump.name]


def tag(name):
    """Fixed-width [name] column so all log lines align."""
    return f"[{name}]".ljust(NAME_W + 2)


# ---------------------------------------------------------------------------
# Pump model
# ---------------------------------------------------------------------------

class Pump:
    def __init__(self, user, password, host, region="NA", port=443,
                 https=True, name=None):
        self.host = host
        self.user = user
        self.password = password
        self.region = region
        self.port = port
        self.https = https
        self.name = name or f"{host}:{port}"
        self.client = TransClient(host, user, password, port=port, https=https)
        self.fails = 0
        self.degraded = False
        self.degraded_since = None
        self._free_cache = None    # None = never fetched / fetch failed
        self._free_ts = 0
        self._last_evict_ts = 0
        self._evicting = False     # True while a remove thread is running

    def rpc_ok(self):
        try:
            self.client.session_get(fields=["version"])
            if self.degraded:
                cprint(C.GREEN, f"  {ts()} {tag(self.name)} recovered")
            self.fails = 0
            self.degraded = False
            self.degraded_since = None
            return True
        except Exception as e:
            self.fails += 1
            cprint(C.RED, f"  {ts()} {tag(self.name)} unreachable: {e}")
            if self.fails >= DEGRADED_FAIL_THRESHOLD and not self.degraded:
                self.degraded = True
                self.degraded_since = time.time()
            return False

    def is_dead(self):
        return (self.degraded and self.degraded_since
                and time.time() - self.degraded_since > DEGRADED_DEAD_S)

    def free_bytes(self):
        if time.time() - self._free_ts < 60:
            return self._free_cache
        try:
            self._free_cache = self.client.free_space()
            self._free_ts = time.time()
            return self._free_cache
        except Exception:
            # leave _free_ts untouched so next poll retries; return last
            # known value (None on the very first fetch).
            return self._free_cache

    def free_gb(self):
        b = self.free_bytes()
        return b / (1024 ** 3) if b is not None else None


def load_pumps(path):
    pumps = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = [p.strip() for p in line.split(",")]
            if len(parts) < 3:
                continue
            user, password, host = parts[0], parts[1], parts[2]
            region = parts[3] if len(parts) > 3 else "NA"
            port = int(parts[4]) if len(parts) > 4 else 443
            https = port == 443
            pumps.append(Pump(user, password, host, region, port, https))
    return pumps


# ---------------------------------------------------------------------------
# Eviction
# ---------------------------------------------------------------------------

def _do_evict(pump, victim, free_gb):
    """Background removal: stop, wait, then delete. Runs in a thread so
    the long-blocking remove (600s timeout while transmission unlinks
    multi-GB files) doesn't stall polls of other pumps."""
    name = victim.get("name", "?")[:40]
    tid = victim["id"]
    victim_age = time.time() - victim.get("addedDate", time.time())
    try:
        # Stop first: releases open file handles and lets transmission
        # flush .resume state before we delete. On a 0-byte disk a
        # straight remove+delete can wedge the daemon — it can't write
        # session state, so metadata removal succeeds but data-file
        # deletion aborts, leaving the bytes on disk.
        pump.client.torrent_stop([tid])
        time.sleep(2)
        # Long timeout: deleting multi-GB data files blocks the RPC
        # until unlink completes — easily >30s on remote pumps.
        pump.client.torrent_remove([tid], delete_local_data=True, timeout=600)
        pump._free_ts = 0
        pump._last_evict_ts = time.time()
        cprint(C.GREEN,
               f"  {ts()} {tag(pump.name)} evict done '{name}' "
               f"(removed, free was {free_gb:6.1f} GB)")
    except Exception as e:
        cprint(C.RED, f"  {ts()} {tag(pump.name)} evict failed: {e}")
    finally:
        pump._evicting = False


def evict_if_needed(pump, reserve_gb=FREE_RESERVE_GB):
    """If pump free space < reserve, evict one torrent to make room.

    Victim selection:
      1. Stopped torrents (status 0) older than EVICT_MIN_AGE_S — oldest
         wins. A stopped torrent isn't serving anyone; this covers the
         disk-full case where Transmission auto-stops a torrent that
         can't allocate.
      2. Otherwise, running torrents older than EVICT_MIN_AGE_S — pick
         the one with the lowest average upload rate
         (uploadedEver / secondsSeeding), tiebreak oldest addedDate.

    A per-pump cooldown prevents cascading evictions while
    Transmission's free-space figure catches up after a delete.

    The actual stop+remove runs in a background thread (guarded by
    _evicting) so a slow multi-GB delete doesn't block polls of other
    pumps.
    """
    now = time.time()
    if pump._evicting:
        return
    if now - pump._last_evict_ts < EVICT_COOLDOWN_S:
        return

    free_gb = pump.free_gb()
    if free_gb is None:
        # free-space fetch failed — don't act on stale/unknown data
        cprint(C.RED,
               f"  {ts()} {tag(pump.name)} free space unknown "
               f"(RPC timeout?) — skip")
        return
    if free_gb >= reserve_gb:
        return

    try:
        torrents = pump.client.torrent_get(fields=EVICT_FIELDS)
    except Exception:
        return

    if not torrents:
        return

    stopped_candidates = []
    running_candidates = []
    for t in torrents:
        age = now - t.get("addedDate", now)
        if age < EVICT_MIN_AGE_S:
            continue
        if t.get("status", 0) == STATUS_STOPPED:
            stopped_candidates.append((age, t))
        else:
            running_candidates.append(t)

    victim = None
    victim_avg = float("inf")
    victim_age = -1

    if stopped_candidates:
        stopped_candidates.sort(key=lambda x: x[0], reverse=True)
        victim_age, victim = stopped_candidates[0]
        victim_avg = 0
    elif running_candidates:
        for t in running_candidates:
            age = now - t.get("addedDate", now)
            up = t.get("uploadedEver", 0) or 0
            secs = t.get("secondsSeeding", 0) or 0
            # +1 so never-seeded torrents (still downloading, secs==0)
            # don't blow up to inf — they sort to worst naturally on rate
            # since their uploadedEver is also ~0, giving avg≈0.
            avg = up / (secs + 1)
            if (avg < victim_avg
                    or (avg == victim_avg and age > victim_age)):
                victim = t
                victim_avg = avg
                victim_age = age

    if not victim:
        # log candidate counts so it's clear why nothing was evicted
        young = len(torrents) - len(stopped_candidates) - len(running_candidates)
        cprint(C.GRAY,
               f"  {ts()} {tag(pump.name)} low space ({free_gb:6.1f} GB) "
               f"no evictable "
               f"(stopped={len(stopped_candidates)} "
               f"running={len(running_candidates)} "
               f"too_young={young}) — skip")
        return

    name = victim.get("name", "?")[:40]
    if victim.get("status", 0) == STATUS_STOPPED:
        cprint(C.YELLOW,
               f"  {ts()} {tag(pump.name)} evict '{name}' "
               f"(stopped, age {victim_age / 3600:5.1f}h, "
               f"free {free_gb:6.1f} GB)")
    else:
        cprint(C.YELLOW,
               f"  {ts()} {tag(pump.name)} evict '{name}' "
               f"(avg up {victim_avg / 1024:6.0f} KB/s, "
               f"age {victim_age / 3600:5.1f}h, "
               f"free {free_gb:6.1f} GB)")

    # Run the slow stop+remove in a background thread so the main poll
    # loop keeps checking other pumps.
    pump._evicting = True
    pump._last_evict_ts = now
    t = threading.Thread(target=_do_evict, args=(pump, victim, free_gb),
                         daemon=True)
    t.start()


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run_loop(pumps, interval, reserve_gb):
    cprint(C.BOLD,
           f"kickout started: {len(pumps)} pumps, "
           f"reserve {reserve_gb} GB, poll every {interval}s")
    for p in pumps:
        cprint(C.DIM, f"  {tag(p.name)} [{p.region}]")

    while True:
        try:
            for pump in pumps:
                pump.rpc_ok()

            alive = [p for p in pumps if not p.degraded and not p.is_dead()]
            if not alive:
                cprint(C.RED, f"  {ts()} {tag('')} no alive pumps")
                time.sleep(interval)
                continue

            # per-pass status line for each alive pump
            for pump in alive:
                free = pump.free_gb()
                # fetch torrent count (cheap call) for context
                try:
                    torrents = pump.client.torrent_get(fields=["id"])
                    n = len(torrents)
                except Exception:
                    n = "?"
                if free is None:
                    cprint(C.RED,
                           f"  {ts()} {tag(pump.name)} free unknown       "
                           f"torrents {n:<3}  "
                           f"reserve {reserve_gb} GB  (RPC timeout?)")
                else:
                    if free < reserve_gb:
                        color = C.RED
                    elif free < reserve_gb * 2:
                        color = C.YELLOW
                    else:
                        color = C.GREEN
                    cprint(color,
                           f"  {ts()} {tag(pump.name)} free {free:6.1f} GB  "
                           f"torrents {n:<3}  "
                           f"reserve {reserve_gb} GB"
                           + ("  *** LOW ***" if free < reserve_gb else ""))

            for pump in alive:
                evict_if_needed(pump, reserve_gb)
        except KeyboardInterrupt:
            cprint(C.YELLOW, "shutting down")
            break
        except Exception as e:
            cprint(C.RED, f"  {ts()} {tag('')} loop error: {e}")

        time.sleep(interval)


def main():
    p = argparse.ArgumentParser(
        description="Disk-space guard: evicts old unused torrents when "
                    "a pump runs low on free space.")
    p.add_argument("pumps_file",
                   help="text file: user,pass,IP[,region[,port]]")
    p.add_argument("--interval", type=int, default=POLL_INTERVAL_S,
                   help=f"poll interval in seconds (default: {POLL_INTERVAL_S})")
    p.add_argument("--reserve", type=int, default=FREE_RESERVE_GB,
                   help=f"free-space reserve in GB that triggers eviction "
                        f"(default: {FREE_RESERVE_GB})")
    args = p.parse_args()

    pumps = load_pumps(args.pumps_file)
    if not pumps:
        cprint(C.RED, f"no pumps loaded from {args.pumps_file}")
        return 1

    run_loop(pumps, args.interval, args.reserve)
    return 0


if __name__ == "__main__":
    sys.exit(main())

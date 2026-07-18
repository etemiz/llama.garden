#!/usr/bin/env python3
"""fan-out.py
==============
One-shot torrent fanout. Adds a single .torrent file to every pump listed
in pumps.txt with all files wanted (no striping, no phases, no muscle
gate, no health monitoring). Just add it everywhere and exit.

Usage:
    python fan-out.py pumps.txt path/to/model.torrent
    python fan-out.py pumps.txt model.torrent --paused

pumps.txt format (same as orchestrator):
    # user,pass,IP,region,port
    customer,PASSWORD,IP,NA,443
    ubuntu,PASSWORD,IP,EU,443
"""

import argparse
import base64
import sys
import time

from transrpc import TransClient, TransRpcError


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


# ---------------------------------------------------------------------------
# Pump loading (minimal — no health tracking, no caches)
# ---------------------------------------------------------------------------

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
            port = int(parts[4]) if len(parts) > 4 else 443
            https = port == 443
            name = f"{host}:{port}"
            client = TransClient(host, user, password, port=port, https=https)
            pumps.append((name, client))
    return pumps


# ---------------------------------------------------------------------------
# Fanout
# ---------------------------------------------------------------------------

def fanout(pumps, torrent_path, paused=False):
    with open(torrent_path, "rb") as f:
        metainfo_b64 = base64.b64encode(f.read()).decode()

    ok = 0
    fail = 0
    for name, client in pumps:
        try:
            result = client.torrent_add(
                metainfo_b64=metainfo_b64,
                download_dir=None,
                paused=paused)
            # torrent-add returns torrent-added (new) or torrent-duplicate
            # (already present). Both are success from our perspective.
            if "torrent-duplicate" in result:
                cprint(C.YELLOW,
                       f"  {ts()} [{name}] already present — left untouched")
            else:
                state = "paused" if paused else "downloading"
                cprint(C.GREEN,
                       f"  {ts()} [{name}] added ({state})")
            ok += 1
        except TransRpcError as e:
            cprint(C.RED, f"  {ts()} [{name}] add failed: {e}")
            fail += 1
        except Exception as e:
            cprint(C.RED, f"  {ts()} [{name}] error: {e}")
            fail += 1

    cprint(C.BOLD, f"\nfan-out done: {ok} ok, {fail} failed")
    return 0 if fail == 0 else 1


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser(
        description="One-shot torrent fanout to all pumps (all files, no stripe).")
    p.add_argument("pumps_file",
                   help="text file: user,pass,IP[,region[,port]]")
    p.add_argument("torrent_file", help="path to the .torrent file to add")
    p.add_argument("--paused", action="store_true",
                   help="add torrents paused (default: start downloading)")
    args = p.parse_args()

    pumps = load_pumps(args.pumps_file)
    if not pumps:
        cprint(C.RED, f"no pumps loaded from {args.pumps_file}")
        return 1

    try:
        with open(args.torrent_file, "rb") as f:
            pass
    except FileNotFoundError:
        cprint(C.RED, f"torrent file not found: {args.torrent_file}")
        return 1

    cprint(C.BOLD, f"fan-out: {args.torrent_file} -> {len(pumps)} pump(s)")
    return fanout(pumps, args.torrent_file, paused=args.paused)


if __name__ == "__main__":
    sys.exit(main())

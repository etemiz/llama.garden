#!/usr/bin/env python3
"""report.py
==============
Health report across all Transmission instances listed in pumps.txt
and muscles.txt (same comma-CSV format as orchestrator).

For each server it checks:
  - reachability (RPC ping)
  - free disk space (flags <= LOW_SPACE_GB, default 30 GB)
  - active torrent count + aggregate download/upload rates
  - per-torrent table with status, % done, size, rates, ratio,
    peer count, and cluster-unique seeder count

Cluster-wide sections after the per-server tables:
  - SEEDERS (cluster-unique): for each infohash, the number of unique
    peer addresses (across all online daemons) that report progress 1.0
    — i.e. seeds known to the cluster.
  - PER-TORRENT PUMP COUNT: how many pumps/muscles hold each infohash.

Side effect:
  Writes pumps.lmdb — an LMDB map keyed by raw infohash bytes, whose
  values are JSON arrays of {"ip": <host>, "percent_done": <0-100>}
  for every online pump that has that torrent (muscles excluded, since
  their hosts are not public webseed endpoints).

Usage:
    python report.py
    python report.py pumps.txt
    python report.py pumps.txt --muscles-file muscles.txt
    python report.py --low-space 50

pumps.txt / muscles.txt format (identical):
    # user,pass,IP,region,port
    customer,PASSWORD,IP,NA,443
    ubuntu,PASSWORD,IP,EU,443

muscles.txt is read from the current folder by default; if missing it
is silently skipped (report covers pumps only). Muscle servers are
labeled muscle0, muscle1, ... in the output.

Exit codes:
    0  all servers online, none low-space, none with errored torrents
    1  pumps file missing or empty
    2  at least one server offline, low-space, or with errored torrents
"""

import argparse
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from lmdbm import Lmdb
from transrpc import TransClient, TransRpcError

LOW_SPACE_GB = 30
RPC_TIMEOUT = 15

GB = 1024 ** 3
MB = 1024 ** 2
KB = 1024


class JsonLmdb(Lmdb):
    def _pre_value(self, value):
        return json.dumps(value).encode("utf-8")
    def _post_value(self, value):
        return json.loads(value.decode("utf-8"))


def write_lmdb(gathered, path="pumps.lmdb"):
    pumps_by_hash = {}
    for g in gathered:
        if not g.get("online"):
            continue
        # Skip muscles — they're origin builders, not public HTTP webseed
        # hosts. Their entries would redirect BT clients to 127.0.0.1 or
        # an unroutable host. Only pumps serve /seed/ on port 80.
        if g["name"].startswith("muscle"):
            continue
        for t in g.get("torrents", []):
            h = t.get("hashString")
            if not h:
                continue
            raw_key = bytes.fromhex(h)
            pd = round((t.get("percentDone") or 0) * 100, 1)
            pumps_by_hash.setdefault(raw_key, []).append(
                {"ip": g["host"], "percent_done": pd})
    with JsonLmdb.open(path, "c") as db:
        for raw_key, pumps in pumps_by_hash.items():
            db[raw_key] = pumps


class C:
    R = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    MAGENTA = "\033[35m"
    CYAN = "\033[36m"
    GRAY = "\033[90m"


def hr(n):
    """Human-readable byte size."""
    if n is None:
        return "-"
    if n >= GB:
        return f"{n/GB:.2f} GB"
    if n >= MB:
        return f"{n/MB:.1f} MB"
    if n >= KB:
        return f"{n/KB:.0f} KB"
    return f"{n} B"


def hrs(n):
    """Human-readable rate (bytes/s)."""
    return hr(n) + "/s" if n else "0"


# Transmission status codes
STATUS_NAMES = {
    0: "stopped",
    1: "check-wait",
    2: "checking",
    3: "dl-wait",
    4: "downloading",
    5: "seed-wait",
    6: "seeding",
}


def status_name(t):
    s = t.get("status")
    return STATUS_NAMES.get(s, str(s))


def parse_servers(path, missing_ok=False):
    """Parse a comma-CSV server file -> list of (name, user, pass, host,
    region, port, https). missing_ok=True returns [] silently when the
    file is absent (used for muscles.txt).
    """
    out = []
    try:
        f = open(path)
    except FileNotFoundError:
        if missing_ok:
            return out
        raise
    with f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            parts = [p.strip() for p in line.split(",")]
            if len(parts) < 3:
                continue
            user, password, host = parts[0], parts[1], parts[2]
            region = parts[3] if len(parts) > 3 else "NA"
            port = int(parts[4]) if len(parts) > 4 else 443
            https = (port == 443)
            name = f"{host}:{port}"
            out.append((name, user, password, host, region, port, https))
    return out


def fmt_row(cols, widths):
    return "  ".join(str(c).ljust(w) for c, w in zip(cols, widths))


TORRENT_FIELDS = [
    "id", "name", "hashString", "status", "totalSize",
    "percentDone", "rateDownload", "rateUpload", "uploadRatio",
    "uploadedEver", "downloadedEver", "peersConnected",
    "peersGettingFromUs", "peersSendingToUs", "error", "errorString",
    "peers",
]


def gather_server(spec):
    """Fetch session + torrents from one daemon. Returns a result dict;
    does not print. offline daemons return online=False."""
    name, user, password, host, region, port, https = spec
    client = TransClient(host, user, password, port=port, https=https,
                         timeout=RPC_TIMEOUT)

    try:
        session = client.session_get(fields=[
            "version", "download-dir", "download-dir-free-space",
            "peer-port", "peer-limit"])
        stats = client.session_stats()
    except (TransRpcError, Exception) as e:
        return {
            "name": name, "host": host, "region": region,
            "online": False, "error": str(e), "https": https,
        }

    version = session.get("version", "?")
    free = session.get("download-dir-free-space", 0)
    free_gb = free / GB
    dl_dir = session.get("download-dir", "?")
    peer_port = session.get("peer-port", "?")
    peer_limit = session.get("peer-limit", "?")

    try:
        torrents = client.torrent_get(fields=TORRENT_FIELDS)
    except (TransRpcError, Exception) as e:
        torrents = []

    return {
        "name": name, "host": host, "region": region,
        "online": True, "version": version,
        "free": free, "free_gb": free_gb, "low": False,
        "dl_dir": dl_dir, "peer_port": peer_port, "peer_limit": peer_limit,
        "stats": stats, "torrents": torrents, "https": https,
    }


def compute_cluster_seeders(gathered):
    """infohash -> set of unique seeder IPs across all online daemons."""
    seeders_by_hash = {}
    for g in gathered:
        if not g.get("online"):
            continue
        for t in g.get("torrents", []):
            h = t.get("hashString")
            if not h:
                continue
            for p in t.get("peers", []) or []:
                # Transmission peers have no isSeed field; a peer is a seed
                # when it reports progress == 1.0 (it has the whole torrent).
                if p.get("progress", 0) >= 1.0:
                    addr = p.get("address")
                    if addr:
                        seeders_by_hash.setdefault(h, set()).add(addr)
    return seeders_by_hash


def print_server(g, low_gb, seeders_by_hash):
    """Render one server's section (mirrors the old report_server layout)."""
    name = g["name"]
    region = g["region"]
    https = g.get("https", False)
    if not g.get("online"):
        cprint(C.BOLD, f"\n=== {name}  [{region}]  ({'https' if https else 'http'}) ===")
        cprint(C.RED, f"  OFFLINE / unreachable: {g.get('error','')}")
        return {
            "name": name, "host": g["host"], "region": region,
            "online": False, "error": g.get("error", ""),
        }

    version = g["version"]
    free = g["free"]
    free_gb = g["free_gb"]
    dl_dir = g["dl_dir"]
    peer_port = g["peer_port"]
    peer_limit = g["peer_limit"]
    stats = g["stats"]
    torrents = g["torrents"]

    cprint(C.BOLD, f"\n=== {name}  [{region}]  ({'https' if https else 'http'}) ===")

    cur = stats.get("current-stats", {})
    cum = stats.get("cumulative-stats", {})

    low = free_gb <= low_gb
    space_color = C.RED if low else (C.YELLOW if free_gb <= low_gb * 2 else C.GREEN)
    cprint(C.GREEN, f"  online  transmission {version}")
    cprint(C.DIM, f"  download-dir: {dl_dir}")
    cprint(C.DIM, f"  peer-port: {peer_port}  peer-limit: {peer_limit}")
    cprint(space_color,
           f"  free space: {hr(free)}  ({free_gb:.1f} GB)"
           + ("  *** LOW ***" if low else ""))

    cprint(C.DIM,
           f"  session: up {hr(cur.get('uploadedBytes', 0))} "
           f"down {hr(cur.get('downloadedBytes', 0))} "
           f"ratio {cur.get('uploadRatio', 0):.2f} "
           f"| uptime {cur.get('secondsActive', 0)/3600:.1f}h")
    cprint(C.DIM,
           f"  cumul.:  up {hr(cum.get('uploadedBytes', 0))} "
           f"down {hr(cum.get('downloadedBytes', 0))} "
           f"ratio {cum.get('uploadRatio', 0):.2f}")

    n = len(torrents)
    downloading = [t for t in torrents if t.get("status") == 4]
    seeding = [t for t in torrents if t.get("status") == 6]
    stopped = [t for t in torrents if t.get("status") == 0]
    checking = [t for t in torrents if t.get("status") in (1, 2)]
    errored = [t for t in torrents if t.get("error", 0) != 0]

    total_dl = sum(t.get("rateDownload", 0) for t in torrents)
    total_ul = sum(t.get("rateUpload", 0) for t in torrents)

    cprint(C.CYAN,
           f"  torrents: {n} total  "
           f"(dl:{len(downloading)} seed:{len(seeding)} "
           f"stop:{len(stopped)} check:{len(checking)} err:{len(errored)})")
    cprint(C.CYAN,
           f"  rates: down {hrs(total_dl)}  up {hrs(total_ul)}")

    if errored:
        cprint(C.RED, f"  {len(errored)} torrent(s) with errors:")
        for t in errored[:10]:
            cprint(C.RED, f"    - {t.get('name','?')[:50]}  "
                    f"[{t.get('errorString','?')}]")

    if torrents:
        cprint(C.BOLD, "")
        widths = [28, 10, 10, 8, 11, 11, 7, 7, 6, 8]
        header = ["name", "infohash", "status", "%done", "down", "up",
                  "ratio", "peers", "seeders", "size"]
        cprint(C.DIM, "  " + fmt_row(header, widths))
        cprint(C.DIM, "  " + "-" * (sum(widths) + 2 * (len(widths) - 1)))
        torrents_sorted = sorted(torrents, key=lambda t: t.get("name", ""))
        for t in torrents_sorted:
            nm = (t.get("name", "?") or "?")[:28]
            st = status_name(t)
            pd = t.get("percentDone", 0)
            pd_s = f"{pd*100:.1f}" if pd is not None else "-"
            rd = t.get("rateDownload", 0) or 0
            ru = t.get("rateUpload", 0) or 0
            ratio = t.get("uploadRatio", 0) or 0
            peers = t.get("peersConnected", 0) or 0
            h = t.get("hashString")
            seeders = len(seeders_by_hash.get(h, ())) if h else 0
            sz = t.get("totalSize", 0) or 0
            row_color = C.GRAY
            if t.get("error", 0) != 0:
                row_color = C.RED
            elif st == "seeding":
                row_color = C.GREEN
            elif st == "downloading":
                row_color = C.CYAN
            elif st == "stopped":
                row_color = C.GRAY
            ih = (h or "?")[:10]
            cprint(row_color,
                   "  " + fmt_row(
                       [nm, ih, st, pd_s, hrs(rd), hrs(ru),
                        f"{ratio:.2f}", str(peers), str(seeders), hr(sz)],
                       widths))

    return {
        "name": name, "host": g["host"], "region": region,
        "online": True, "version": version,
        "free_gb": round(free_gb, 1), "low": low,
        "torrent_count": n,
        "downloading": len(downloading),
        "seeding": len(seeding),
        "stopped": len(stopped),
        "errored": len(errored),
        "total_dl": total_dl, "total_ul": total_ul,
    }


def cprint(color, msg, end="\n"):
    print(f"{color}{msg}{C.R}", end=end)
    sys.stdout.flush()


def main():
    p = argparse.ArgumentParser(
        description="Health report across all Transmission instances.")
    p.add_argument("pumps_file", nargs="?", default="pumps.txt",
                   help="pumps file: user,pass,IP,region,port (default: pumps.txt)")
    p.add_argument("--muscles-file", default="muscles.txt",
                   help="muscle servers file, same CSV format (default: "
                        "muscles.txt in cwd; missing = pumps only)")
    p.add_argument("--low-space", type=float, default=LOW_SPACE_GB,
                   help=f"flag servers with free space <= this GB "
                        f"(default: {LOW_SPACE_GB})")
    args = p.parse_args()

    if not Path(args.pumps_file).is_file():
        cprint(C.RED, f"pumps file not found: {args.pumps_file}")
        return 1

    pumps = parse_servers(args.pumps_file)
    if not pumps:
        cprint(C.RED, f"no pumps parsed from {args.pumps_file}")
        return 1

    # muscles.txt is optional — missing file is not an error.
    muscle_specs = parse_servers(args.muscles_file, missing_ok=True)
    # relabel muscle entries muscle0, muscle1, ... to distinguish them
    # from pumps in the report.
    muscles = [(f"muscle{i}", *spec[1:])
               for i, spec in enumerate(muscle_specs)]
    servers = pumps + muscles

    cprint(C.BOLD, f"Transmission cluster report  "
          f"{time.strftime('%Y-%m-%d %H:%M:%S')}")
    cprint(C.DIM, f"{len(pumps)} pump(s) in {args.pumps_file}  "
          f"+ {len(muscles)} muscle(s) in {args.muscles_file}  "
          f"= {len(servers)} server(s)  "
          f"(low-space threshold: {args.low_space} GB)")

    # Pass 1: gather all data in parallel so we can compute cluster-wide
    # unique seeders per infohash before rendering per-server tables.
    gathered = []
    with ThreadPoolExecutor(max_workers=len(servers)) as pool:
        fut = {pool.submit(gather_server, spec): spec for spec in servers}
        for f in as_completed(fut):
            gathered.append(f.result())
    seeders_by_hash = compute_cluster_seeders(gathered)

    write_lmdb(gathered)

    # Pass 2: render each server, using the cluster seeders map.
    results = [print_server(g, args.low_space, seeders_by_hash)
               for g in gathered]

    # summary
    cprint(C.BOLD + C.MAGENTA, "\n=== SUMMARY ===")
    online = [r for r in results if r.get("online")]
    offline = [r for r in results if not r.get("online")]
    low_space = [r for r in online if r.get("low")]
    errored = [r for r in online if r.get("errored", 0) > 0]

    cprint(C.GREEN if not offline else C.YELLOW,
           f"online:  {len(online)}/{len(results)}")
    if offline:
        cprint(C.RED, "offline / unreachable:")
        for r in offline:
            cprint(C.RED, f"  - {r['name']}  ({r.get('error','')[:60]})")

    if low_space:
        cprint(C.RED, f"low space (<= {args.low_space} GB):")
        for r in low_space:
            cprint(C.RED, f"  - {r['name']}: {r.get('free_gb','?')} GB free  "
                    f"({r.get('torrent_count','?')} torrents)")
    else:
        cprint(C.GREEN, f"all online servers have > {args.low_space} GB free")

    if errored:
        cprint(C.RED, "servers with errored torrents:")
        for r in errored:
            cprint(C.RED, f"  - {r['name']}: {r.get('errored')} errored")

    total_torrents = sum(r.get("torrent_count", 0) for r in online)
    total_dl = sum(r.get("total_dl", 0) for r in online)
    total_ul = sum(r.get("total_ul", 0) for r in online)
    cprint(C.CYAN, f"cluster totals: {total_torrents} torrents  "
          f"down {hrs(total_dl)}  up {hrs(total_ul)}")

    if seeders_by_hash:
        cprint(C.BOLD + C.MAGENTA, "\n=== SEEDERS (cluster-unique) ===")
        cprint(C.DIM, "  # seeders  torrent")
        cprint(C.DIM, "  ---------  -------")
        for h, ips in sorted(seeders_by_hash.items(),
                             key=lambda kv: len(kv[1]), reverse=True):
            nm = "?"
            for g in gathered:
                if not g.get("online"):
                    continue
                for t in g.get("torrents", []):
                    if t.get("hashString") == h:
                        nm = (t.get("name") or "?")[:50]
                        break
                if nm != "?":
                    break
            cprint(C.GREEN, f"  {len(ips):>7}    {nm}")

    # Per-infohash server count (how many pumps/muscles have each torrent)
    pumps_by_hash = {}
    for g in gathered:
        if not g.get("online"):
            continue
        for t in g.get("torrents", []):
            h = t.get("hashString")
            if h:
                pumps_by_hash.setdefault(h, set()).add(g["name"])

    if pumps_by_hash:
        cprint(C.BOLD + C.MAGENTA, "\n=== PER-TORRENT PUMP COUNT ===")
        cprint(C.DIM, "  pumps  infohash                                   torrent")
        cprint(C.DIM, "  -----  ----------------------------------------  -------")
        for h, servers in sorted(pumps_by_hash.items(),
                                 key=lambda kv: len(kv[1]), reverse=True):
            nm = "?"
            for g in gathered:
                if not g.get("online"):
                    continue
                for t in g.get("torrents", []):
                    if t.get("hashString") == h:
                        nm = (t.get("name") or "?")[:50]
                        break
                if nm != "?":
                    break
            cprint(C.CYAN, f"  {len(servers):>4}   {h}  {nm}")

    if offline or low_space or errored:
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())

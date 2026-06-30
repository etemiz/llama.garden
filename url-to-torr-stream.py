#!/usr/bin/env python3
"""url-to-torr-stream.py
=========================
Build a BitTorrent v1 `.torrent` (with webseeds) for files on HuggingFace,
without keeping a full local copy on disk.

Pull a whole repo, a subfolder, or just a handful of quant files — the
torrent's webseeds point back at HuggingFace's CDN so qBittorrent /
libtorrent / Transmission can download the content directly from HF
without any peer needing the original `.torrent`-downloaded bytes.

    ./url-to-torr-stream.py https://huggingface.co/org/repo
    ./url-to-torr-stream.py https://huggingface.co/org/repo/tree/main
    ./url-to-torr-stream.py https://huggingface.co/org/repo/tree/main/assets
    ./url-to-torr-stream.py https://huggingface.co/org/repo/tree/main --mask '*Q4*'
    ./url-to-torr-stream.py https://huggingface.co/org/repo/tree/main --mask '*Q2*' --mask '*Q4*'
    ./url-to-torr-stream.py https://huggingface.co/org/repo/tree/main/assets --mask '*.png'

Output
------
- `<out>/<repo>-<sha7>[.<subfolder>][.<mask>].torrent` (default `./torrents/`)
  The subfolder basename and/or cleaned mask glob are appended as dot-
  separated suffixes for identifiability, e.g.:
      repo-abc1234.torrent              # whole repo
      repo-abc1234.MTP.torrent           # subfolder "MTP"
      repo-abc1234.Q2.torrent            # --mask '*Q2*'
      repo-abc1234.MTP.Q4.torrent        # subfolder "MTP" + --mask '*Q4*'
- The magnet URI is printed to stdout.

Flags
-----
- `--mask GLOB`      Only include files whose basename matches GLOB (fnmatch).
                     Repeatable; a file is included if it matches ANY mask.
                     `*` and `?` are stripped from the suffix in the filename.
- `--out DIR`        Output directory (default `./torrents`).
- `--piece-length N` Piece length in bytes (default: auto, ~1024-2048 pieces).
- `--workers N`      Parallel download workers (default 4).
- `--mirror HOST`    Download from `huggingface.co` or `hf-mirror.com`
                     (default: huggingface.co). Both are embedded as webseeds
                     in the torrent regardless of this flag.

Why this downloads files (and why that's unavoidable)
-----------------------------------------------------
BitTorrent v1 piece hashes are SHA1 of fixed-size chunks of the
*concatenated* byte stream across all files in torrent order — a single
piece can span two files. HuggingFace's APIs only expose whole-file
checksums:

    git blob oid   SHA1("blob <size>\\0" + content)  — has a git header, not raw
    lfs.oid        SHA256(whole file)                 — wrong algo, not chunked,
                                                         and LFS files only
    xetHash        proprietary Xet hash               — not standard SHA1

None of these can be turned into BT v1 piece hashes, so the bytes must be
read. This script streams them once through memory/disk to compute pieces
and then discards them; the final `.torrent` is small (~10s of KB).

Pipelined architecture (low peak disk)
---------------------------------------
Downloads run concurrently in a thread pool while a single hasher consumes
files **in order** (piece hashes span file boundaries, so byte order
matters). Each file's temp file is deleted the moment it finishes hashing —
it does not wait for all downloads to complete:

    worker A: [dl file 0] [dl file 3] [dl file 5] ...
    worker B: [dl file 1] [dl file 4] ...
    worker C: [dl file 2] ...
    hasher:   ...[wait 0][hash+del 0][wait 1][hash+del 1][wait 2]...

Each file has a `threading.Event`; the downloader sets it on completion (or
failure), and the hasher blocks on `event[i]` before reading file `i`. A
slow file 0 therefore doesn't block hashing of files that are already done,
but pieces ARE emitted in file order, preserving BT v1 correctness.

Peak disk usage ≈ `(workers + 1) × largest file`, not the whole torrent.

Webseed strategy (single code path for whole-repo and subfolder URLs)
---------------------------------------------------------------------
libtorrent builds each file's webseed URL as:

    webseed_base + escape_path(info.name + "/" + <file.subpath joined>)

and HuggingFace serves files at:

    https://huggingface.co/<org>/<repo>/resolve/<sha>/<in-repo-path>

So we always set:

    info.name         = <commit_sha>    (single path segment)
    webseed_base      = <origin>/<org>/<repo>/resolve/
    info.files[].path = full in-repo path, split on "/"

→ URL = `.../resolve/<sha>/<in-repo-path>` for every file, whether the input
  URL pointed at the whole repo or a subfolder. The subfolder in the input
  URL only filters *which* files are included, not the torrent structure.

Why `info.name = <commit_sha>` (not `""` or a nice name):
- `info.name=""` → libtorrent rewrites it to `"_"` (2.0.x) or a SHA-1 hex
  (master) and prepends that to every file URL → 404 on HF.
- `info.name` with `/` (e.g. `"org/repo/resolve/<sha>"`) → libtorrent's
  `filter_path_character` (`torrent_info.cpp`) strips `/` from `info.name`
  on load → URL 404s.
- The commit SHA is the only single segment that (a) is a real HF path
  prefix and (b) pins the torrent to an immutable revision (so webseeds
  keep serving the exact bytes the piece hashes were computed from, even
  if the branch later moves). Side effect: the qBittorrent save folder is
  named after the 40-char SHA — rename it (F2) after adding if desired.

Dependencies
------------
- `huggingface_hub`  (HfApi for repo metadata + file listing)
- `torf`             (only `_flatbencode` for encoding the metainfo dict)
- Python 3.9+        (stdlib `urllib`, `threading`, `hashlib`, `fnmatch`)

License
-------
MIT. Uses HuggingFace's public read API and CDN; respect HF's terms of
service and the upstream model license when distributing the torrent.
"""

import argparse
import fnmatch
import hashlib
import shutil
import sys
import tempfile
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import quote, urlparse

from huggingface_hub import HfApi
from huggingface_hub.hf_api import RepoFile
from torf import _flatbencode as _bencode

TRACKERS = [
    "udp://tracker.opentrackr.org:1337/announce",
    "udp://tracker.openbittorrent.com:6969/announce",
    "udp://open.stealth.si:80/announce",
    "udp://explodie.org:6969/announce",
    "udp://tracker.torrent.eu.org:451/announce",
    "udp://exodus.desync.com:6969/announce",
    "https://tracker.tamersunion.org:443/announce",
    "https://tracker1.520.jp:443/announce",
    "https://tracker.gbitt.info:443/announce",
    "https://tracker2.520.jp:443/announce",
]

WEBSEED_BASES = (
    "https://huggingface.co/",
    "https://hf-mirror.com/",
)

STREAM_CHUNK = 256 * 1024


def parse_hf_url(url):
    """Parse a HF URL. Returns (org, repo, revision, subfolder_or_None)."""
    parsed = urlparse(url)
    if parsed.netloc not in ("huggingface.co", "www.huggingface.co"):
        raise ValueError(f"not a huggingface.co URL: {url}")
    parts = [p for p in parsed.path.split("/") if p]
    if len(parts) < 2:
        raise ValueError(f"URL must include at least org/repo: {url}")
    org, repo = parts[0], parts[1]
    revision = "main"
    subfolder = None
    if len(parts) >= 4 and parts[2] == "tree":
        revision = parts[3]
        if len(parts) > 4:
            subfolder = "/".join(parts[4:])
    return org, repo, revision, subfolder


def pick_piece_length(total_size):
    """Pick a power-of-two piece length targeting ~1024-2048 pieces."""
    if total_size <= 0:
        return 256 * 1024
    for pl in (16 * 1024, 32 * 1024, 64 * 1024, 128 * 1024, 256 * 1024,
              512 * 1024, 1024 * 1024, 2 * 1024 * 1024, 4 * 1024 * 1024,
              8 * 1024 * 1024, 16 * 1024 * 1024):
        if total_size / pl <= 2048:
            return pl
    return 16 * 1024 * 1024


def _download_one(url, tmp_path, idx, total, eprint):
    """Download one file to tmp_path. Raises on failure after retries."""
    label = url.split("/resolve/")[-1]
    eprint(f"  [dl] ({idx+1}/{total}) {label}")
    retries = 0
    while True:
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "url-to-torr-stream/1.0"})
            with urllib.request.urlopen(req, timeout=300) as r, open(tmp_path, "wb") as f:
                while True:
                    chunk = r.read(STREAM_CHUNK)
                    if not chunk:
                        break
                    f.write(chunk)
            return
        except Exception as exc:
            retries += 1
            if retries > 4:
                raise RuntimeError(f"failed to download {url} after {retries} tries: {exc}")
            eprint(f"    [retry {retries}] {label}: {exc}; sleeping {retries*2}s")
            time.sleep(retries * 2)


def compute_pieces_pipelined(matched, origin, org, repo, commit_sha,
                             piece_length, workers, eprint, tmp_dir):
    """Download files concurrently, hash pieces in file order, delete each
    temp file as soon as it's hashed.

    Returns the 20*N byte SHA1 piece hashes.

    Peak disk ≈ (workers + 1) files: at most `workers` files are downloading
    at any time (unhashed), plus the one currently being hashed.
    """
    n = len(matched)
    base_url = f"{origin}{org}/{repo}/resolve/{commit_sha}/"
    urls = [base_url + e.path for e in matched]
    tmp_paths = [tmp_dir / f"file_{i:06d}.bin" for i in range(n)]

    # Per-file completion signal + error slot. The hasher blocks on
    # events[i] before touching file i; errors[i] carries any exception.
    events = [threading.Event() for _ in range(n)]
    errors = [None] * n

    def dl_wrapper(i):
        try:
            _download_one(urls[i], tmp_paths[i], i, n, eprint)
        except Exception as exc:
            errors[i] = exc
        finally:
            events[i].set()

    eprint(f"[info] pipelining {n} file(s) with {workers} download worker(s) ...")
    pieces = bytearray()
    buf = bytearray()
    total_in = 0

    with ThreadPoolExecutor(max_workers=workers) as pool:
        # Submit all downloads. The pool's internal queue holds tasks that
        # haven't started yet, so at most `workers` run concurrently.
        for i in range(n):
            pool.submit(dl_wrapper, i)

        # Hash in order, blocking on each file's event. This is the pipeline:
        # file i is hashed (and deleted) as soon as it's downloaded, even if
        # later files are still in flight.
        for i in range(n):
            eprint(f"  [hash] ({i+1}/{n}) waiting for {matched[i].path}")
            events[i].wait()
            if errors[i] is not None:
                # Clean up any remaining temp files before bailing.
                for tp in tmp_paths:
                    tp.unlink(missing_ok=True)
                raise errors[i]
            with open(tmp_paths[i], "rb") as f:
                while True:
                    chunk = f.read(STREAM_CHUNK)
                    if not chunk:
                        break
                    buf += chunk
                    total_in += len(chunk)
                    while len(buf) >= piece_length:
                        pieces += hashlib.sha1(bytes(buf[:piece_length])).digest()
                        del buf[:piece_length]
            tmp_paths[i].unlink(missing_ok=True)
            eprint(f"  [hash] ({i+1}/{n}) done, temp deleted")

    if buf:
        pieces += hashlib.sha1(bytes(buf)).digest()
    eprint(f"[info] hashed {total_in} bytes, {len(pieces)//20} pieces")
    return bytes(pieces)


def main():
    p = argparse.ArgumentParser(
        description="Build a .torrent with webseeds for HF files (pipelined, low disk).",
    )
    p.add_argument(
        "url",
        help="HF URL, e.g. https://huggingface.co/org/repo/tree/main/subfolder",
    )
    p.add_argument(
        "--mask",
        action="append",
        default=None,
        metavar="GLOB",
        help="only include files whose basename matches this glob (e.g. '*Q4*'). "
             "Repeatable; a file matches if it matches ANY given mask.",
    )
    p.add_argument(
        "--out",
        default="./torrents",
        help="output directory for the .torrent (default: ./torrents)",
    )
    p.add_argument(
        "--piece-length",
        type=int,
        default=None,
        help="piece length in bytes (default: auto, targeting ~1024-2048 pieces)",
    )
    p.add_argument(
        "--mirror",
        default=None,
        choices=("huggingface.co", "hf-mirror.com"),
        help="which mirror to download from (default: huggingface.co)",
    )
    p.add_argument(
        "--workers",
        type=int,
        default=4,
        help="number of parallel download workers (default: 4). Peak disk "
             "usage is approximately (workers+1) times the largest file size.",
    )
    args = p.parse_args()

    eprint = lambda *a, **kw: print(*a, file=sys.stderr, **kw)

    try:
        org, repo, revision, subfolder = parse_hf_url(args.url)
    except ValueError as e:
        eprint(f"[fail] {e}")
        return 2
    eprint(f"[info] org={org}  repo={repo}  revision={revision}  subfolder={subfolder}")

    api = HfApi()
    try:
        commit_sha = api.repo_info(repo_id=f"{org}/{repo}", revision=revision).sha
    except Exception as exc:
        eprint(f"[fail] could not resolve repo: {exc}")
        return 2
    eprint(f"[info] commit: {commit_sha[:7]}")

    eprint(f"[info] scanning files in {org}/{repo}@{commit_sha[:7]} ...")
    try:
        tree = api.list_repo_tree(repo_id=f"{org}/{repo}", revision=revision, recursive=True)
    except Exception as exc:
        eprint(f"[fail] could not list repo files: {exc}")
        return 2

    masks = args.mask
    subfolder_prefix = subfolder.rstrip("/") + "/" if subfolder else None
    matched = []
    for entry in tree:
        if not isinstance(entry, RepoFile):
            continue
        path = entry.path
        if subfolder_prefix and not path.startswith(subfolder_prefix):
            continue
        if masks:
            basename = path.rsplit("/", 1)[-1]
            if not any(fnmatch.fnmatch(basename, m) for m in masks):
                continue
        matched.append(entry)

    if not matched:
        eprint("[fail] no files matched the filters")
        return 1

    matched.sort(key=lambda e: e.path)

    total_size = sum((f.size or 0) for f in matched)
    piece_length = args.piece_length or pick_piece_length(total_size)
    eprint(f"[info] matched {len(matched)} file(s), total {total_size} bytes, "
           f"piece length {piece_length} ({piece_length//1024} KiB)")
    if masks:
        eprint(f"[info] mask(s): {masks}")
    for f in matched:
        eprint(f"  - {f.path}  ({f.size or 0} bytes)")

    origin = f"https://{args.mirror}/" if args.mirror else "https://huggingface.co/"
    tmp_dir = Path(tempfile.mkdtemp(prefix="url-to-torr-stream-", dir="./tmp"))
    try:
        pieces = compute_pieces_pipelined(
            matched, origin, org, repo, commit_sha,
            piece_length, args.workers, eprint, tmp_dir,
        )
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    info = {
        b"name": commit_sha.encode(),
        b"piece length": piece_length,
        b"pieces": pieces,
        b"files": [
            {
                b"length": f.size,
                b"path": [seg.encode() for seg in f.path.split("/")],
            }
            for f in matched
        ],
    }
    webseeds = [f"{base}{org}/{repo}/resolve/" for base in WEBSEED_BASES]
    metainfo = {
        b"info": info,
        b"announce": TRACKERS[0].encode(),
        b"announce-list": [[tr.encode()] for tr in TRACKERS],
        b"url-list": [ws.encode() for ws in webseeds],
        b"creation date": int(time.time()),
        b"comment": f"HF: {org}/{repo}@{commit_sha[:7]}".encode(),
        b"created by": b"url-to-torr-stream.py",
    }

    infohash = hashlib.sha1(_bencode.encode(info)).hexdigest()
    torrent_stem = f"{repo}-{commit_sha[:7]}"
    # Reflect subfolder and/or mask in the torrent filename for identifiability,
    # e.g. repo-sha.MTP.torrent, repo-sha.Q2.torrent, repo-sha.MTP.Q2.torrent.
    suffix_parts = []
    if subfolder:
        suffix_parts.append(subfolder.rstrip("/").rsplit("/", 1)[-1])
    if masks:
        cleaned = []
        for m in masks:
            s = m.replace("*", "").replace("?", "")
            if s:
                cleaned.append(s)
        if cleaned:
            suffix_parts.append("_".join(cleaned))
    if suffix_parts:
        torrent_stem += "." + ".".join(suffix_parts)

    out_dir = Path(args.out).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    torrent_path = out_dir / f"{torrent_stem}.torrent"
    torrent_path.write_bytes(_bencode.encode(metainfo))
    eprint(f"[info] wrote: {torrent_path}  ({torrent_path.stat().st_size} bytes)")
    eprint(f"[info] infohash: {infohash}")
    eprint(f"[info] piece count: {len(pieces) // 20}")
    eprint(f"[info] webseeds: {len(webseeds)} entries")

    magnet_parts = [f"magnet:?xt=urn:btih:{infohash}",
                    f"dn={quote(torrent_stem)}",
                    f"xl={total_size}"]
    magnet_parts.extend(f"tr={quote(tr)}" for tr in TRACKERS)
    magnet_parts.extend(f"ws={quote(ws)}" for ws in webseeds)
    print("&".join(magnet_parts))
    return 0


if __name__ == "__main__":
    sys.exit(main())

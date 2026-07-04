#!/usr/bin/env python3
"""cache-to-torr-nostr.py
========================
Scan the local HuggingFace hub cache (~/.cache/huggingface/hub), generate a
.torrent for every cached model snapshot, upload each .torrent to Blossom
servers, sign a kind 30099 Nostr "torrent listing" event per model, and
fan it out to Nostr relays. Writes:

    ./torrents/<stem>.torrent
    ./events/<infohash>.json          (per successful event)
    ./events/run_<ts>.jsonl           (per-run summary log)
    ./log/cache_nostr_send_log_<ts>.jsonl

Disk usage: the generated .torrent files are small — estimated new disk
usage is in the megabyte range (a few MB per model; proportional to file
count, not model size). Piece hashes are BT v1 SHA-1 (20 bytes each),
piece length auto-scales so a multi-GB model typically yields only a few
hundred KB of .torrent.

The HF cache is keyed by commit SHA, not branch, so the torrent is always a
verifiable snapshot of the repo *as it was when you cached it* — even if
HF's main branch later moves on, the torrent stays internally consistent
forever (webseeds point at /resolve/<cached_sha>/<path>, immutable on HF's
CDN).

Webseeds: HuggingFace is used as the fallback webseed source. Four
webseed URLs are emitted per torrent:
    https://1@huggingface.co/<org>/<repo>/resolve/
    https://2@huggingface.co/<org>/<repo>/resolve/
    https://huggingface.co/<org>/<repo>/resolve/
    https://hf-mirror.com/<org>/<repo>/resolve/
All four resolve to the same HF CDN, but the `1@`/`2@` userinfo prefix
makes them look distinct to a BT client, so it opens 4 parallel
connections to HF for faster end-user downloads. If HF is unreachable
the torrent still works peer-to-peer over the trackers.

Classification: the kind (base / fine tune), quant type (gguf / mlx /
awq / gptq / fp8 / ...), and bits-per-weight detection are heuristic —
they are guesses, not exact science. The classifier inspects repo tags
and filenames and infers the most likely interpretation; misclassifications
are possible (e.g. a repo tagged both ways, or an unfamiliar naming
convention). Treat the file_class / model_kind / quant_* fields as best
effort, not ground truth.

Proof of work: before signing each kind 30099 event the script mines a
NIP-13 `nonce` tag for --pow-seconds seconds (default 10), varying the
nonce and keeping the event id with the most leading-zero bits. This
combats spam on relays that check PoW. Pass --pow-seconds 0 to disable.
Progress (best bits, h/s, seconds left) is printed to stderr.

Seeding: the torrent's info.name is the commit SHA and file paths are
relative to the snapshot root, so to actually seed the data files an end
user must place real files at <seed_dir>/<commit_sha>/<files>. The HF
cache layout uses symlinks (snapshots/<sha>/<file> -> blobs/<hash>) which
most BT clients refuse to follow when verifying/seeding, so re-download
real files with --local-dir:

    huggingface-cli download <org/repo> --revision <commit_sha> \\
        --local-dir ./seed/<commit_sha>

Then add the .torrent to your BT client with the download dir set to
./seed (the parent of <commit_sha>/). The client finds
<commit_sha>/<files> already complete, verifies the piece hashes, and
seeds.

Disk cost: `hf download --local-dir` COPIES files out of the cache
into the destination (shutil.copyfile; the deprecated
--local-dir-use-symlinks flag is ignored in huggingface_hub >= 0.23).
So while both copies exist the model occupies ~2x its size on disk:
once in the HF cache (~/.cache/huggingface/hub/.../blobs/<sha>) and
once in ./seed/<commit_sha>/. To reclaim the cache copy after seeding
starts, run `huggingface-cli delete-cache` (interactive TUI) or remove
the specific model dir:

    rm -rf ~/.cache/huggingface/hub/models--<org>--<repo>

Leaving only the ./seed copy as the seed source. Do NOT delete
./seed/<commit_sha>/ once seeding — the BT client needs those files
to serve pieces.

Flow:
  1. Detect cache folder (--cache-dir overrides; else HF_HUB_CACHE constant
     which honors HF_HUB_CACHE / HUGGINGFACE_HUB_CACHE env vars, default
     ~/.cache/huggingface/hub).
  2. Enumerate models--<org>--<repo>/ subdirs. For each: resolve commit SHA
     from refs/main (fallback: first ref, then first snapshots/<sha>/ subdir),
     walk snapshots/<sha>/ following symlinks into blobs/ to compute on-disk
     file set + total size.
  3. Print numbered table; prompt for selection (or --yes = all).
  4. Ask for NSEC (NSEC env var, or paste, or [r]andom mock). One nsec reused
     for every event in this run.
  5. For each selected model:
       - HfApi().repo_info / list_repo_tree at revision=cached_sha for the
         canonical file list + LFS sha256 / git blob sha1 + tags + metadata.
         On RevisionNotFoundError (garbage-collected commit) fall back to
         offline mode for this one model: walk disk only, no checksum verify.
       - Default: skip partial caches (on-disk set != HF tree set). Use
         --allow-partial to torrent only the files actually on disk.
       - Reuse the url-to-torr-combo.py classifier (base/fine-tune/quant,
         gguf/mlx/awq/gptq/fp8/...; bpw from filenames or num_parameters).
       - Hash BT v1 pieces in file order (open() follows symlinks into
         blobs/). Verify each file's HF checksum (LFS sha256 / git blob
         sha1) — skipped in offline mode.
       - Build .torrent: info.name = commit_sha, 4 HF webseeds, TRACKERS.
         Write to ./torrents/<stem>.torrent.
  6. Upload .torrent bytes to every Blossom server (BUD-02 kind 24242 auth
     event, PUT /upload, fail-soft). Collect URLs.
  7. Mine PoW (nonce tag) for --pow-seconds, then sign kind 30099
     parameterized-replaceable event (d = infohash) with magnet, name,
     size, pieces, piece_length, x (torrent sha256), torrent_size,
     torrent_created, m, url(s), webseed(s), tracker(s), source, plus
     enriched metadata + hf_match + cache_only tags.
  8. Fan out to all relays (strict per-relay OK, 10s connect / 15s send).
     Per-send log to ./log/cache_nostr_send_log_<ts>.jsonl.
  9. On success (>=1 relay OK): write ./events/<infohash>.json. Append a
     summary line to ./events/run_<ts>.jsonl either way.
 10. Print final summary table.

Deps: huggingface_hub, torf (_flatbencode), nostr_sdk, stdlib. No tqdm.
"""

import argparse
import asyncio
import base64
import fnmatch
import hashlib
import json
import os
import re
import sys
import time
import urllib.request
import urllib.error
from collections import namedtuple
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

try:
    import nostr_sdk
    _NOSTR_SDK_AVAILABLE = True
except ImportError:
    nostr_sdk = None
    _NOSTR_SDK_AVAILABLE = False

try:
    from huggingface_hub import HfApi
    from huggingface_hub.hf_api import RepoFile
    from huggingface_hub.constants import HF_HUB_CACHE as _HF_HUB_CACHE_CONST
    _HF_AVAILABLE = True
except ImportError:
    HfApi = None
    RepoFile = None
    _HF_HUB_CACHE_CONST = None
    _HF_AVAILABLE = False

try:
    from torf import _flatbencode as _bencode
    _TORF_AVAILABLE = True
except ImportError:
    _bencode = None
    _TORF_AVAILABLE = False


# ---------------------------------------------------------------------------
# ANSI
# ---------------------------------------------------------------------------

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

def cprint(color, msg, end="\n", file=None):
    print(f"{color}{msg}{C.R}", file=file or sys.stderr, end=end)

def cinput(color, prompt):
    return input(f"{color}{prompt}{C.R}")


# ---------------------------------------------------------------------------
# CONFIG (mirrors url-to-torr-combo.py + post-torr-nostr-j.py)
# ---------------------------------------------------------------------------

TRACKERS = [
    "udp://tracker.opentrackr.org:1337/announce",
    "udp://open.stealth.si:80/announce",
    "udp://explodie.org:6969/announce",
    "udp://tracker.torrent.eu.org:451/announce",
    "udp://exodus.desync.com:6969/announce",
]

GB = 1024 ** 3
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(SCRIPT_DIR, "log")

BLOSSOM_SERVERS = [
    "https://nostr.download",
    "https://blossom.primal.net",
    "https://cdn.hzrd149.com",
]

NOSTR_RELAYS = [
    "wss://nos.lol/",
    "wss://nostr-01.yakihonne.com/",
    "wss://nostr.mom/",
    "wss://relay.damus.io/",
    "wss://relay.primal.net/",
    "wss://relay.snort.social",
    "wss://relay.mostr.pub",
    "wss://no.str.cr",
    "wss://offchain.pub",
]

TORRENT_KIND = 30099
BLOSSOM_AUTH_KIND = 24242
AUTH_EXPIRY_SECONDS = 300

NOSTR_CONNECT_TIMEOUT = 10
NOSTR_SEND_TIMEOUT = 15

POW_DEFAULT_SECONDS = 10
POW_PROGRESS_EVERY_HASHES = 50000


# ---------------------------------------------------------------------------
# PROOF OF WORK (NIP-13 nonce mining; mirrors waifu-magnet-16.html worker)
# ---------------------------------------------------------------------------

def _leading_zero_bits(hex_id):
    """Count leading-zero bits in a 64-char hex SHA-256 digest."""
    bits = 0
    for ch in hex_id:
        n = int(ch, 16)
        if n == 0:
            bits += 4
        else:
            bits += 4 - n.bit_length()
            break
    return bits


def mine_pow(pubkey_hex, created_at, kind, tags_vec, content, duration_s):
    """Mine a NIP-13 `nonce` tag for `duration_s` seconds.

    Varies the last entry of `tags_vec` (which must be ["nonce", "..."]),
    recomputes the canonical Nostr event id (SHA-256 over the JSON array
    [0, pubkey, created_at, kind, tags, content] per NIP-01), and keeps
    the nonce yielding the most leading-zero bits.

    Returns (best_nonce, best_pow_bits, best_id_hex, total_hashes).
    Mirrors the runPowWorker loop in waifu-magnet-16.html.
    """
    if duration_s <= 0:
        return 0, 0, "", 0
    import random
    nonce = random.randint(0, 0xFFFFFFFF)
    best_nonce = nonce
    best_pow = -1
    best_id = ""
    hashes = 0
    t0 = time.monotonic()
    deadline = t0 + duration_s
    last_progress = t0
    # Pre-bind everything that doesn't change inside the loop.
    base_arr = [0, pubkey_hex, created_at, kind, tags_vec, content]
    sha256 = hashlib.sha256
    while True:
        now = time.monotonic()
        if now >= deadline:
            break
        for _ in range(10000):
            tags_vec[-1] = ["nonce", str(nonce)]
            s = json.dumps(base_arr, separators=(",", ":"), ensure_ascii=False)
            digest = sha256(s.encode("utf-8")).hexdigest()
            hashes += 1
            p = _leading_zero_bits(digest)
            if p > best_pow:
                best_pow = p
                best_nonce = nonce
                best_id = digest
            nonce = (nonce + 1) & 0xFFFFFFFF
        if hashes % POW_PROGRESS_EVERY_HASHES < 10000:
            elapsed = now - t0
            if now - last_progress >= 1.0:
                rate = hashes / max(elapsed, 0.001)
                secs_left = max(0, duration_s - elapsed)
                cprint(C.DIM, f"    [pow] best: {best_pow} leading-zero bits "
                       f"\u00b7 {rate:.0f} h/s \u00b7 {secs_left:.0f}s left")
                last_progress = now
    return best_nonce, max(best_pow, 0), best_id, hashes



# ---------------------------------------------------------------------------
# CLASSIFICATION (copied from url-to-torr-combo.py)
# ---------------------------------------------------------------------------

QUANT_TAGS = {
    "gguf": "gguf", "mlx": "mlx", "awq": "awq", "gptq": "gptq",
    "fp8": "fp8", "bitsandbytes": "bnb", "onnx": "onnx",
}
QUANT_SUFFIXES = [
    (r"-?GGUF$", "gguf"),
    (r"-?MLX(?:-\d+bit)?$", "mlx"),
    (r"-?AWQ(?:-(?:\d+bit|INT\d+))?$", "awq"),
    (r"-?GPTQ(?:-(?:\d+bit|INT\d+))?$", "gptq"),
    (r"-?FP8$", "fp8"),
    (r"-?NVFP4$", "nvfp4"),
    (r"-?MXFP4$", "mxfp4"),
    (r"-?BNB$", "bnb"),
    (r"-?ONNX$", "onnx"),
]
NAME_TOKEN_TYPES = {
    "NVFP4": "nvfp4", "MXFP4": "mxfp4", "FP8": "fp8",
}
GGUF_TOKEN_RE = re.compile(
    r"(UD-)?(BF16|F16|F32|Q\d_K_[A-Z]+|Q\d_K|IQ\d_[A-Z]+|IQ\d|Q\d_\d|Q\d)"
)
QUANT_NAME_STRIP = re.compile(
    r"(?:-?(?:GGUF|MLX(?:-\d+bit)?|AWQ(?:-(?:\d+bit|INT\d+))?|"
    r"GPTQ(?:-(?:\d+bit|INT\d+))?|"
    r"FP8|NVFP4|MXFP4|BNB|ONNX))$"
)
BPW_TABLE = {
    "BF16": 16.0, "F16": 16.0, "F32": 32.0,
    "Q8_0": 8.5, "Q8_K": 8.5, "Q6_K": 6.6, "Q5_K_M": 5.5, "Q5_K_S": 5.2,
    "Q5_0": 5.5, "Q5_1": 5.5,
    "Q4_K_M": 4.9, "Q4_K_S": 4.7, "Q4_0": 4.6, "Q4_1": 4.6,
    "Q3_K_M": 3.9, "Q3_K_S": 3.5, "Q3_K_L": 4.0,
    "Q2_K": 2.6, "Q2_K_S": 2.4,
    "IQ4_NL": 4.5, "IQ4_XS": 4.25, "IQ3_S": 3.4, "IQ3_M": 3.5,
    "IQ3_XXS": 3.1, "IQ2_M": 2.2, "IQ2_S": 2.0, "IQ2_XS": 1.8,
    "IQ2_XXS": 1.7,
    "IQ1_M": 1.7, "IQ1_S": 1.5,
    "NVFP4": 4.0, "MXFP4": 4.0, "FP8": 8.0,
    "MLX-4bit": 4.5, "MLX-8bit": 8.0, "AWQ-4bit": 4.5, "GPTQ-4bit": 4.5,
}


def classify_repo(repo_name, tags):
    for t in tags:
        tl = t.lower()
        if tl in QUANT_TAGS:
            return "quant", QUANT_TAGS[tl]
    for pat, qt in QUANT_SUFFIXES:
        if re.search(pat, repo_name, re.IGNORECASE):
            return "quant", qt
    for tok, qt in NAME_TOKEN_TYPES.items():
        if tok in repo_name:
            return "quant", qt
    if any(t.startswith("base_model:") for t in tags):
        return "fine tune", None
    return "base", None


def extract_base_model_ref(tags):
    for prefix in ("base_model:finetune:", "base_model:quantized:", "base_model:"):
        for t in tags:
            if t.startswith(prefix):
                ref = t[len(prefix):]
                if "/" in ref:
                    org, name = ref.split("/", 1)
                    return f"{org}/{name}", org, name
    return None


def extract_gguf_token(name):
    matches = list(GGUF_TOKEN_RE.finditer(name))
    if not matches:
        return None
    m = matches[-1]
    ud, tok = m.group(1), m.group(2)
    return ("UD-" + tok) if ud else tok


def bpw_for_token(token):
    if not token:
        return None
    key = token[3:] if token.startswith("UD-") else token
    return BPW_TABLE.get(key)


def detect_non_gguf_detail(repo_name, quant_type):
    for tok in ("NVFP4", "MXFP4", "FP8"):
        if tok in repo_name:
            return tok
    if quant_type == "mlx":
        m = re.search(r"MLX-(\d+bit)", repo_name, re.IGNORECASE)
        return m.group(1) if m else "MLX"
    if quant_type == "awq":
        m = re.search(r"AWQ-((?:\d+bit|INT\d+))", repo_name, re.IGNORECASE)
        return m.group(1) if m else "AWQ"
    if quant_type == "gptq":
        m = re.search(r"GPTQ-((?:\d+bit|INT\d+))", repo_name, re.IGNORECASE)
        return m.group(1) if m else "GPTQ"
    if quant_type == "bnb":
        return "BNB"
    if quant_type == "onnx":
        return "ONNX"
    return None


def make_display_name(file_class, lab, model_name, quant_type,
                     quant_dev, quant_detail):
    if file_class == "quant":
        if quant_detail and quant_detail != "many":
            term = quant_detail
        else:
            term = quant_type
        if quant_detail == "many":
            term = None
        if term and quant_dev:
            return f"{model_name} \u00b7 {term} \u00b7 {quant_dev}"
        if quant_dev:
            return f"{model_name} \u00b7 {quant_dev}"
        if term:
            return f"{model_name} \u00b7 {term}"
        return model_name
    return f"{lab} \u00b7 {model_name}"


def classify(org, repo, tags, model_info_obj):
    repo_name = repo
    file_class, quant_type = classify_repo(repo_name, tags)
    base_ref = extract_base_model_ref(tags)
    base_model_field = None
    if base_ref:
        base_model_field = {
            "repo": base_ref[0], "lab": base_ref[1], "name": base_ref[2],
        }
    num_parameters = getattr(model_info_obj, "numParameters", None)

    if file_class == "quant":
        if base_ref:
            lab, model_name = base_ref[1], base_ref[2]
        else:
            lab = org
            model_name = QUANT_NAME_STRIP.sub("", repo_name) or repo_name
        quant_dev = org
    else:
        lab = org
        model_name = repo_name
        quant_dev = None

    if file_class == "quant":
        model_kind = "base"
        if base_ref:
            try:
                base_info = HfApi().repo_info(repo_id=base_ref[0])
                bt_tags = getattr(base_info, "tags", []) or []
                if any(t.startswith("base_model:") for t in bt_tags):
                    model_kind = "fine tune"
            except Exception:
                pass
    else:
        model_kind = file_class

    quant_detail = None
    quant_bpw = None
    if quant_type == "gguf":
        pass
    elif quant_type:
        quant_detail = detect_non_gguf_detail(repo_name, quant_type)

    return {
        "file_class": file_class,
        "model_kind": model_kind,
        "quant_type": quant_type,
        "quant_dev": quant_dev,
        "quant_detail": quant_detail,
        "quant_bpw": quant_bpw,
        "lab": lab,
        "model_name": model_name,
        "base_model": base_model_field,
        "num_parameters": num_parameters,
    }


# ---------------------------------------------------------------------------
# TORRENT BUILD HELPERS (ported from url-to-torr-combo.py)
# ---------------------------------------------------------------------------

CHUNK = 256 * 1024


def _human_size(n):
    for u in ("B", "KiB", "MiB", "GiB", "TiB"):
        if n < 1024:
            return f"{n:.1f} {u}" if u != "B" else f"{n:.0f} B"
        n /= 1024
    return f"{n:.1f} PiB"


def pick_piece_length(total):
    for pl in (16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384):
        pl *= 1024
        if total / pl <= 2048:
            return pl
    return 16 * 1024 * 1024


def webseeds_for(org, repo):
    base = f"{org}/{repo}/resolve/"
    return [
        f"https://1@huggingface.co/{base}",
        f"https://2@huggingface.co/{base}",
        f"https://huggingface.co/{base}",
        f"https://hf-mirror.com/{base}",
    ]


def safe_slug(s):
    return re.sub(r"[^A-Za-z0-9._-]", "-", s).strip("-") or "model"


def _verify_hash(entry, path, piece_length, buf, pieces, hash_counter):
    if entry.lfs is not None:
        verify = hashlib.sha256()
        expected = entry.lfs.sha256
    else:
        verify = hashlib.sha1()
        verify.update(f"blob {entry.size}\0".encode())
        expected = entry.blob_id
    with open(path, "rb") as f:
        while True:
            chunk = f.read(CHUNK)
            if not chunk:
                break
            buf += chunk
            verify.update(chunk)
            hash_counter[0] += len(chunk)
            while len(buf) >= piece_length:
                pieces += hashlib.sha1(bytes(buf[:piece_length])).digest()
                del buf[:piece_length]
    return verify.hexdigest() == expected


def _bt_hash_only(path, piece_length, buf, pieces, hash_counter):
    """BT-piece hash a file without HF checksum verification (offline mode)."""
    with open(path, "rb") as f:
        while True:
            chunk = f.read(CHUNK)
            if not chunk:
                break
            buf += chunk
            hash_counter[0] += len(chunk)
            while len(buf) >= piece_length:
                pieces += hashlib.sha1(bytes(buf[:piece_length])).digest()
                del buf[:piece_length]


# On-disk file entry (offline mode — no HF RepoFile available).
DiskFile = namedtuple("DiskFile", ["path", "size", "lfs", "blob_id"])
# DiskFile.lfs / blob_id are None in offline mode so _verify_hash is never
# called on these; the caller dispatches via _bt_hash_only instead.


def compute_pieces_from_disk(matched, snap_dir, piece_length, hash_counter,
                             verify=True):
    """Hash BT v1 pieces in file order. When verify=True each file is also
    verified against its HF checksum (LFS sha256 / git blob sha1); on mismatch
    the file is re-read for the next pass would be inconsistent, so we raise.
    When verify=False (offline mode) we only BT-piece-hash, no checksum check.
    """
    pieces = bytearray()
    buf = bytearray()
    n = len(matched)
    for i, entry in enumerate(matched):
        p = snap_dir / entry.path
        if not p.exists():
            raise RuntimeError(f"missing local file: {p}")
        got = p.stat().st_size
        if got != entry.size:
            raise RuntimeError(
                f"{entry.path}: got {got} bytes, expected {entry.size}")
        if verify:
            ok = _verify_hash(entry, p, piece_length, buf, pieces, hash_counter)
            if not ok:
                raise RuntimeError(f"checksum mismatch: {entry.path}")
        else:
            _bt_hash_only(p, piece_length, buf, pieces, hash_counter)
        cprint(C.GRAY, f"  [{i+1}/{n}] {'verified' if verify else 'hashed'}: {entry.path}")
    if buf:
        pieces += hashlib.sha1(bytes(buf)).digest()
    return bytes(pieces)


# ---------------------------------------------------------------------------
# CACHE ENUMERATION
# ---------------------------------------------------------------------------

def detect_cache_dir(override):
    if override:
        p = Path(override).expanduser().resolve()
        if not p.is_dir():
            raise SystemExit(f"--cache-dir not a directory: {p}")
        return p
    if _HF_HUB_CACHE_CONST is not None:
        return Path(_HF_HUB_CACHE_CONST).expanduser()
    # Last-resort fallback (matches huggingface_hub's own default).
    return Path.home() / ".cache" / "huggingface" / "hub"


def parse_model_dir(model_dir):
    """Return (org, repo, commit_sha, snap_dir, on_disk_files).

    on_disk_files is a list of (relpath, size) walking snapshots/<sha>/
    following symlinks into blobs/. Returns None for commit_sha if no
    snapshot could be resolved.
    """
    name = model_dir.name
    repo_part = name[len("models--"):]
    if "--" not in repo_part:
        return None
    org_repo = repo_part.replace("--", "/", 1)
    if "/" not in org_repo:
        return None
    org, repo = org_repo.split("/", 1)

    sha = None
    refs = model_dir / "refs"
    if (refs / "main").exists():
        try:
            sha = (refs / "main").read_text().strip()
        except OSError:
            sha = None
    if not sha and refs.is_dir():
        for r in refs.iterdir():
            try:
                v = r.read_text().strip()
                if v:
                    sha = v
                    break
            except OSError:
                continue
    snaps = model_dir / "snapshots"
    if not sha and snaps.is_dir():
        subdirs = [s for s in snaps.iterdir() if s.is_dir()]
        if subdirs:
            sha = subdirs[0].name
    if not sha:
        return (org, repo, None, None, [])

    snap_dir = snaps / sha
    if not snap_dir.is_dir():
        return (org, repo, sha, None, [])

    on_disk = []
    for root, dirs, files in os.walk(snap_dir, followlinks=True):
        for f in files:
            p = Path(root) / f
            rel = str(p.relative_to(snap_dir))
            try:
                sz = p.stat().st_size
            except OSError:
                continue
            on_disk.append((rel, sz))
    on_disk.sort()
    return (org, repo, sha, snap_dir, on_disk)


# ---------------------------------------------------------------------------
# NSEC PROMPT
# ---------------------------------------------------------------------------

def get_nsec():
    """Return a hex nsec string. Honors NSEC env, else prompts. Allows [r]
    for a randomly generated mock nsec (one per run)."""
    env_nsec = os.environ.get("NSEC")
    if env_nsec:
        try:
            keys = nostr_sdk.Keys.parse(env_nsec)
            npub = keys.public_key().to_hex()
            cprint(C.DIM, f"  using NSEC from env (npub={npub[:16]}...)")
            return env_nsec
        except Exception as e:
            cprint(C.RED, f"  NSEC env var invalid: {e}")
    while True:
        ans = cinput(C.YELLOW, "  Paste NSEC (or [r] for random mock): ").strip()
        if ans in ("", "r", "R"):
            keys = nostr_sdk.Keys.generate()
            sec = keys.secret_key().to_hex()
            nsec = keys.secret_key().to_bech32() if hasattr(keys.secret_key(), "to_bech32") else sec
            npub = keys.public_key().to_hex()
            cprint(C.MAGENTA, f"  generated random mock nsec (npub={npub[:16]}...)")
            return nsec if isinstance(nsec, str) else sec
        try:
            nostr_sdk.Keys.parse(ans)
            return ans
        except Exception as e:
            cprint(C.RED, f"  could not parse nsec: {e}")


# ---------------------------------------------------------------------------
# BLOSSOM UPLOAD (copied from post-torr-nostr-j.py)
# ---------------------------------------------------------------------------

async def make_blossom_auth(nsec, sha256_hex, size):
    keys = nostr_sdk.Keys.parse(nsec)
    signer = nostr_sdk.NostrSigner.keys(keys)
    builder = nostr_sdk.EventBuilder(nostr_sdk.Kind(BLOSSOM_AUTH_KIND), "")
    expiration = str(int(time.time()) + AUTH_EXPIRY_SECONDS)
    tags = [
        nostr_sdk.Tag.parse(["t", "upload"]),
        nostr_sdk.Tag.parse(["expiration", expiration]),
        nostr_sdk.Tag.parse(["x", sha256_hex]),
        nostr_sdk.Tag.parse(["size", str(size)]),
    ]
    builder = builder.tags(tags)
    return await builder.sign(signer)


def upload_to_blossom(server, torrent_bytes, auth_event):
    auth_b64 = base64.b64encode(auth_event.as_json().encode()).decode()
    url = server.rstrip("/") + "/upload"
    req = urllib.request.Request(
        url, data=torrent_bytes, method="PUT",
        headers={
            "Authorization": f"Nostr {auth_b64}",
            "Content-Type": "application/x-bittorrent",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            body = r.read().decode("utf-8", errors="replace")
            try:
                resp = json.loads(body)
                blob_url = resp.get("url")
                if blob_url:
                    return True, blob_url, None
                return False, None, f"no url in response: {body[:200]}"
            except json.JSONDecodeError:
                return False, None, f"non-JSON response: {body[:200]}"
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8", errors="replace")[:200]
        except Exception:
            pass
        return False, None, f"HTTP {e.code}: {body}"
    except Exception as e:
        return False, None, str(e)


async def phase_blossom(nsec, torrent_path):
    with open(torrent_path, "rb") as f:
        torrent_bytes = f.read()
    sha256_hex = hashlib.sha256(torrent_bytes).hexdigest()
    size = len(torrent_bytes)
    cprint(C.DIM, f"  [blossom] uploading {size} bytes to "
           f"{len(BLOSSOM_SERVERS)} server(s)...")
    auth_event = await make_blossom_auth(nsec, sha256_hex, size)
    results = []
    for server in BLOSSOM_SERVERS:
        label = server.replace("https://", "").replace("http://", "").rstrip("/")
        ok, url, err = upload_to_blossom(server, torrent_bytes, auth_event)
        results.append((server, ok, url, err))
        if ok:
            cprint(C.GREEN, f"    [OK]   {label:24s} -> {url}")
        else:
            cprint(C.RED, f"    [FAIL] {label:24s} -> {err}")
    succeeded = sum(1 for _, ok, _, _ in results if ok)
    cprint(C.DIM, f"  [blossom] {succeeded}/{len(BLOSSOM_SERVERS)} succeeded")
    return results


# ---------------------------------------------------------------------------
# SIGN KIND 30099 (adapted from post-torr-nostr-j.py + enriched)
# ---------------------------------------------------------------------------

async def sign_listing_event(nsec, job, blossom_urls, torrent_sha256,
                              torrent_size, hf_match, offline_mode,
                              pow_seconds=POW_DEFAULT_SECONDS):
    keys = nostr_sdk.Keys.parse(nsec)
    signer = nostr_sdk.NostrSigner.keys(keys)
    pubkey_hex = keys.public_key().to_hex()
    content = ""

    # Build tags as plain [name, value] pairs first so we can mine the
    # NIP-13 nonce over the canonical event-id serialization before
    # handing them to nostr_sdk for signing.
    tags_vec = [
        ["d", job["infohash"]],
        ["magnet", job["magnet"]],
        ["name", job.get("torrent_stem") or job["name"]],
        ["size", str(job["total_size"])],
        ["pieces", str(job["piece_count"])],
        ["piece_length", str(job["piece_length"])],
        ["x", torrent_sha256],
        ["torrent_size", str(torrent_size)],
        ["torrent_created", datetime.fromtimestamp(
            os.path.getmtime(job["torrent_path"]),
            tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")],
        ["m", "application/x-bittorrent"],
        ["hf_match", hf_match],
        ["cache_only", "1"],
    ]
    for u in blossom_urls:
        tags_vec.append(["url", u])
    for ws in job["webseeds"]:
        tags_vec.append(["webseed", ws])
    seen_trackers = set()
    for tr in job["trackers"]:
        if tr not in seen_trackers:
            seen_trackers.add(tr)
            tags_vec.append(["tracker", tr])
    if job.get("source"):
        tags_vec.append(["source", job["source"]])

    # enriched metadata (skipped in offline mode where metadata is unreliable)
    if not offline_mode:
        for key, tag_name in (
            ("display_name", "display_name"),
            ("file_class", "file_class"),
            ("model_kind", "model_kind"),
            ("quant_type", "quant_type"),
            ("quant_dev", "quant_dev"),
            ("quant_detail", "quant_detail"),
            ("quant_bpw", "quant_bpw"),
            ("lab", "lab"),
            ("model_name", "model_name"),
            ("repo_id", "repo_id"),
            ("created_at", "created_at"),
            ("version", "version"),
            ("commit_sha", "commit_sha"),
        ):
            val = job.get(key)
            if val is not None and not (isinstance(val, str) and val == ""):
                tags_vec.append([tag_name, str(val)])
        bm = job.get("base_model")
        if isinstance(bm, dict) and bm.get("repo"):
            tags_vec.append(["base_model", bm["repo"]])
        elif isinstance(bm, str) and bm:
            tags_vec.append(["base_model", bm])

    # Pin created_at BEFORE mining so the mined event id matches the id
    # nostr_sdk will sign (the id is SHA-256 over [0, pubkey, created_at,
    # kind, tags, content]; if created_at drifts the signed id won't match
    # the mined id and the PoW is wasted).
    created_at = int(time.time())

    # --- Proof of work (NIP-13 nonce mining) ---
    if pow_seconds and pow_seconds > 0:
        tags_vec.append(["nonce", "0"])
        cprint(C.DIM, f"  [pow] mining nonce tag for {pow_seconds}s "
               f"(mirrors waifu-magnet-16.html runPowWorker)...")
        best_nonce, best_pow, best_id, n_hashes = mine_pow(
            pubkey_hex, created_at, TORRENT_KIND, tags_vec, content,
            pow_seconds)
        tags_vec[-1] = ["nonce", str(best_nonce)]
        cprint(C.DIM, f"  [pow] done: {n_hashes} hashes in {pow_seconds}s "
               f"-> {best_pow} leading-zero bits")
        if best_pow < 1:
            cprint(C.YELLOW, f"  [pow] no proof-of-work found in "
                   f"{pow_seconds}s; signing anyway with nonce=0")

    # Convert to nostr_sdk Tags and sign with the pinned created_at.
    tags = [nostr_sdk.Tag.parse(t) for t in tags_vec]
    builder = (nostr_sdk.EventBuilder(nostr_sdk.Kind(TORRENT_KIND), content)
               .tags(tags)
               .custom_created_at(nostr_sdk.Timestamp.from_secs(created_at)))
    try:
        return await builder.sign(signer)
    except Exception as e:
        cprint(C.RED, f"  [nostr] signing failed: {e}")
        return None


# ---------------------------------------------------------------------------
# RELAY FAN-OUT (copied from post-torr-nostr-j.py)
# ---------------------------------------------------------------------------

async def connect_relay(client, relay_url, timeout):
    rc = client.add_relay(nostr_sdk.RelayUrl.parse(relay_url))
    if asyncio.iscoroutine(rc):
        await asyncio.wait_for(rc, timeout=5)
    rc = client.connect()
    if asyncio.iscoroutine(rc):
        await asyncio.wait_for(rc, timeout=5)
    relays = client.relays()
    if asyncio.iscoroutine(relays):
        relays = await asyncio.wait_for(relays, timeout=5)
    target = nostr_sdk.RelayUrl.parse(relay_url)
    relay = relays.get(target) or next(iter(relays.values()), None)
    if relay is None:
        raise RuntimeError("relay not found in client.relays() after add")
    deadline = time.monotonic() + timeout
    while True:
        try:
            if relay.is_connected():
                return
        except Exception:
            pass
        if time.monotonic() >= deadline:
            raise TimeoutError("relay did not reach Connected in time")
        await asyncio.sleep(0.1)


def _record_send(log_path, event_id, relay_url, ok, latency_ms, error):
    rec = {
        "ts": int(time.time()),
        "event_id": event_id,
        "relay": relay_url,
        "ok": ok,
        "latency_ms": latency_ms,
        "error": error,
    }
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")


async def send_to_relay(relay_url, event_obj, log_path):
    event_id = event_obj.id().to_hex()
    client = nostr_sdk.Client()
    label = relay_url.replace("wss://", "").replace("ws://", "").rstrip("/")
    try:
        await asyncio.wait_for(
            connect_relay(client, relay_url, NOSTR_CONNECT_TIMEOUT),
            timeout=NOSTR_CONNECT_TIMEOUT + 1,
        )
    except Exception as e:
        err = f"connect failed: {e}"
        cprint(C.RED, f"    [FAIL] {label:30s} ({err})")
        _record_send(log_path, event_id, relay_url, False, 0, err)
        try:
            await client.disconnect()
        except Exception:
            pass
        return False, 0, err

    t0 = time.monotonic()
    try:
        out = client.send_event(event_obj)
        if asyncio.iscoroutine(out):
            out = await asyncio.wait_for(out, timeout=NOSTR_SEND_TIMEOUT)
        latency_ms = int((time.monotonic() - t0) * 1000)
        target = str(nostr_sdk.RelayUrl.parse(relay_url))
        success_urls = {str(u) for u in out.success}
        failed_urls = {str(u) for u in out.failed.keys()}
        if target in success_urls:
            cprint(C.GREEN, f"    [OK]   {label:30s} ({latency_ms}ms)")
            _record_send(log_path, event_id, relay_url, True, latency_ms, "")
            return True, latency_ms, ""
        elif target in failed_urls:
            reason = out.failed[next(u for u in out.failed if str(u) == target)]
            err = f"rejected: {reason}"
            cprint(C.RED, f"    [FAIL] {label:30s} ({err})")
            _record_send(log_path, event_id, relay_url, False, latency_ms, err)
            return False, latency_ms, err
        else:
            err = "no OK from relay (not in success or failed)"
            cprint(C.RED, f"    [FAIL] {label:30s} ({err})")
            _record_send(log_path, event_id, relay_url, False, latency_ms, err)
            return False, latency_ms, err
    except asyncio.TimeoutError:
        latency_ms = int((time.monotonic() - t0) * 1000)
        err = "timeout (send_event did not return)"
        cprint(C.RED, f"    [FAIL] {label:30s} ({err})")
        _record_send(log_path, event_id, relay_url, False, latency_ms, err)
        return False, latency_ms, err
    except Exception as e:
        latency_ms = int((time.monotonic() - t0) * 1000)
        err = str(e)
        cprint(C.RED, f"    [FAIL] {label:30s} ({err})")
        _record_send(log_path, event_id, relay_url, False, latency_ms, err)
        return False, latency_ms, err
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass


async def phase_relay(event_obj, log_path):
    cprint(C.DIM, f"  [nostr] fan-out to {len(NOSTR_RELAYS)} relay(s)...")
    tasks = [send_to_relay(url, event_obj, log_path) for url in NOSTR_RELAYS]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    ok = sum(1 for r in results if isinstance(r, tuple) and r[0])
    fail = len(NOSTR_RELAYS) - ok
    cprint(C.DIM, f"  [nostr] {ok}/{len(NOSTR_RELAYS)} accepted")
    return ok, fail


# ---------------------------------------------------------------------------
# MODEL ROW (selection table)
# ---------------------------------------------------------------------------

class ModelRow:
    def __init__(self, idx, org, repo, sha, snap_dir, on_disk_files,
                 hf_tree_files=None, hf_status="OK"):
        self.idx = idx
        self.org = org
        self.repo = repo
        self.sha = sha
        self.snap_dir = snap_dir
        self.on_disk_files = on_disk_files  # list of (relpath, size)
        self.hf_tree_files = hf_tree_files  # set of relpaths from HF, or None
        # status: OK | PARTIAL | OFFLINE | NO_SHA
        self.hf_status = hf_status

    @property
    def repo_id(self):
        return f"{self.org}/{self.repo}"

    @property
    def on_disk_paths(self):
        return {p for p, _ in self.on_disk_files}

    @property
    def on_disk_count(self):
        return len(self.on_disk_files)

    @property
    def total_size(self):
        return sum(s for _, s in self.on_disk_files)

    @property
    def hf_count(self):
        return len(self.hf_tree_files) if self.hf_tree_files is not None else None

    @property
    def missing_paths(self):
        if self.hf_tree_files is None:
            return set()
        return self.hf_tree_files - self.on_disk_paths

    @property
    def is_complete(self):
        if self.hf_tree_files is None:
            return None  # unknown (offline)
        return self.on_disk_paths == self.hf_tree_files

    def is_selectable(self, allow_partial=False):
        """Whether this row should be auto-selected by 'all'."""
        if self.hf_status == "NO_SHA":
            return False
        if self.hf_status == "OFFLINE":
            return True  # always offer offline-mode conversion
        if self.is_complete:
            return True
        return allow_partial  # partial: only selectable with --allow-partial


# ---------------------------------------------------------------------------
# SELECTION PROMPT
# ---------------------------------------------------------------------------

def prompt_selection(rows, allow_partial, yes):
    """Return list of selected ModelRow. --yes selects all selectable rows."""
    if yes:
        sel = [r for r in rows if r.is_selectable(allow_partial)]
        cprint(C.DIM, f"  --yes: auto-selected {len(sel)} of {len(rows)} row(s)")
        return sel

    cprint(C.BOLD, "\n  Generate torrents based on all of these? [y] yes  [N] no "
           "(pick manually)  [q] quit")
    ans = cinput(C.YELLOW, "  choice [y/N/q]: ").strip().lower()
    if ans in ("q", "quit"):
        return []
    if ans in ("y", "yes", ""):
        sel = [r for r in rows if r.is_selectable(allow_partial)]
        if not sel:
            cprint(C.RED, "  no selectable rows.")
        return sel

    # manual: ask for numbers/ranges
    while True:
        s = cinput(C.YELLOW, "  enter numbers/ranges (e.g. 1,3,5-8): ").strip()
        if not s:
            return []
        try:
            picks = set()
            for tok in s.split(","):
                tok = tok.strip()
                if not tok:
                    continue
                if "-" in tok:
                    a, b = tok.split("-", 1)
                    for i in range(int(a), int(b) + 1):
                        picks.add(i)
                else:
                    picks.add(int(tok))
        except ValueError:
            cprint(C.RED, "  invalid input, try again")
            continue
        sel = [r for r in rows if r.idx in picks]
        for r in rows:
            if r.idx in picks and not r.is_selectable(allow_partial):
                cprint(C.YELLOW, f"  row {r.idx} ({r.repo_id}) not selectable "
                       f"(status={r.hf_status}); skipping")
        sel = [r for r in sel if r.is_selectable(allow_partial)]
        return sel


# ---------------------------------------------------------------------------
# ONE MODEL: BUILD TORRENT
# ---------------------------------------------------------------------------

def build_torrent_for_model(row, args, out_dir):
    """Build a .torrent for one cached model. Returns a manifest dict (with
    torrent_path, infohash, magnet, etc.) or None on failure."""
    repo_id = row.repo_id
    cprint(C.BOLD + C.CYAN, f"\n  {repo_id}  (commit {row.sha[:7]})")
    cprint(C.DIM, "  " + "-" * 60)
    cprint(C.DIM, f"  on disk: {row.on_disk_count} file(s), "
           f"{_human_size(row.total_size)}")

    # --- try HF API for canonical file list + metadata ---
    offline_mode = bool(args.offline)
    info = None
    tree = None
    if not offline_mode and _HF_AVAILABLE:
        api = HfApi()
        try:
            info = api.repo_info(repo_id=repo_id, revision=row.sha)
            tree = list(api.list_repo_tree(
                repo_id=repo_id, revision=row.sha, recursive=True))
        except Exception as e:
            cprint(C.YELLOW, f"  [offline] HF API failed for {repo_id}@"
                   f"{row.sha[:7]}: {type(e).__name__} {str(e)[:120]}")
            cprint(C.YELLOW, f"  [offline] falling back to disk-only mode; "
                   "checksum verify + enriched metadata will be skipped.")
            offline_mode = True
            info = None
            tree = None
    elif offline_mode:
        cprint(C.YELLOW, f"  [offline] --offline set; skipping HF API + "
               "checksum verify.")

    # --- build the matched file list ---
    if offline_mode or tree is None:
        # No HF RepoFile list — synthesize DiskFile entries from disk walk.
        matched = [DiskFile(path=p, size=s, lfs=None, blob_id=None)
                   for p, s in row.on_disk_files]
        hf_match = "unknown"
    else:
        all_hf = [e for e in tree if isinstance(e, RepoFile)]
        hf_paths = {e.path for e in all_hf}
        if args.allow_partial:
            # only the HF entries that are actually on disk
            on_disk_set = row.on_disk_paths
            matched = [e for e in all_hf if e.path in on_disk_set]
            missing = hf_paths - on_disk_set
            if missing:
                cprint(C.YELLOW, f"  [partial] {len(missing)} of {len(hf_paths)} "
                       f"HF files not on disk; torrent will contain "
                       f"{len(matched)} of {len(hf_paths)}")
                for m in sorted(missing)[:8]:
                    cprint(C.GRAY, f"    missing: {m}")
                if len(missing) > 8:
                    cprint(C.GRAY, f"    ... ({len(missing)-8} more)")
            hf_match = "partial"
        else:
            # require-complete (default): caller should have filtered this
            # row out, but defend anyway.
            on_disk_set = row.on_disk_paths
            if hf_paths != on_disk_set:
                cprint(C.RED, f"  [skip] partial cache (not selected by "
                       "default); pass --allow-partial to include.")
                return None
            matched = list(all_hf)
            hf_match = "yes"

    if not matched:
        cprint(C.RED, "  [fail] no files to torrent.")
        return None

    matched.sort(key=lambda e: (getattr(e, "lfs", None) is not None,
                                e.path))
    total_size = sum((f.size or 0) for f in matched)
    pl = args.piece_length or pick_piece_length(total_size)
    cprint(C.DIM, f"  {len(matched)} file(s), {_human_size(total_size)}, "
           f"piece length {pl // 1024} KiB")

    # --- classify (skipped in offline mode) ---
    meta = {
        "file_class": "base", "model_kind": "base",
        "quant_type": None, "quant_dev": None,
        "quant_detail": None, "quant_bpw": None,
        "lab": row.org, "model_name": row.repo,
        "base_model": None, "num_parameters": None,
    }
    if not offline_mode and info is not None:
        tags = getattr(info, "tags", []) or []
        meta = classify(row.org, row.repo, tags, info)

        # GGUF quant_detail from filenames
        if meta["quant_type"] == "gguf" and not meta["quant_detail"]:
            gguf_files = [f for f in matched
                          if isinstance(f, RepoFile) and f.path.endswith(".gguf")]
            if gguf_files:
                tokens = set()
                for f in gguf_files:
                    tok = extract_gguf_token(f.path.rsplit("/", 1)[-1])
                    if tok:
                        tokens.add(tok)
                if len(tokens) == 1:
                    meta["quant_detail"] = tokens.pop()
                elif tokens:
                    meta["quant_detail"] = "many"
            if meta["quant_detail"] and meta["quant_detail"] != "many":
                meta["quant_bpw"] = bpw_for_token(meta["quant_detail"])

        # bpw from total weights vs numParameters
        if meta["quant_bpw"] is None and meta["num_parameters"]:
            total_w = sum(f.size for f in matched
                          if isinstance(f, RepoFile)
                          and f.path.endswith((".safetensors", ".gguf", ".onnx")))
            if total_w > 0:
                bpw = round(total_w * 8 / meta["num_parameters"], 2)
                if 1.0 <= bpw <= 32.0:
                    meta["quant_bpw"] = bpw

        if meta["file_class"] in ("base", "fine tune") and meta["quant_bpw"] is None:
            st_files = [f for f in matched
                        if isinstance(f, RepoFile)
                        and f.path.endswith(".safetensors")]
            if st_files and meta["num_parameters"]:
                total_w = sum(f.size for f in st_files)
                bpw = round(total_w * 8 / meta["num_parameters"], 2)
                if 1.0 <= bpw <= 32.0:
                    meta["quant_bpw"] = bpw
                    meta["quant_detail"] = "BF16" if bpw >= 15.5 else "safetensors"

    display_name = make_display_name(
        meta["file_class"], meta["lab"], meta["model_name"],
        meta["quant_type"], meta["quant_dev"], meta["quant_detail"])

    # --- classification summary ---
    cprint(C.BLUE, "\n  classification:")
    cprint(C.DIM, f"    class    : {meta['file_class']}")
    cprint(C.DIM, f"    kind     : {meta['model_kind']}")
    if meta["quant_type"]:
        cprint(C.DIM, f"    quant    : {meta['quant_type']}"
               + (f" ({meta['quant_detail']})" if meta["quant_detail"] else ""))
    if meta["quant_bpw"]:
        cprint(C.DIM, f"    bpw      : {meta['quant_bpw']}")
    cprint(C.DIM, f"    lab      : {meta['lab']}")
    cprint(C.DIM, f"    model    : {meta['model_name']}")
    cprint(C.DIM, f"    display  : {display_name}")

    # --- hash + verify ---
    hash_counter = [0]
    t0 = time.monotonic()
    try:
        pieces = compute_pieces_from_disk(
            matched, row.snap_dir, pl, hash_counter,
            verify=(not offline_mode))
    except Exception as exc:
        cprint(C.RED, f"  [fail] {exc}")
        return None
    dt = time.monotonic() - t0
    piece_count = len(pieces) // 20
    cprint(C.GREEN, f"  hashed {_human_size(row.total_size)} in {dt:.1f}s, "
           f"{piece_count} pieces")

    # --- build .torrent ---
    info_dict = {
        b"name": row.sha.encode(),
        b"piece length": pl,
        b"pieces": pieces,
        b"files": [
            {b"length": f.size,
             b"path": [s.encode() for s in f.path.split("/")]}
            for f in matched
        ],
    }
    webseeds = webseeds_for(row.org, row.repo)
    metainfo = {
        b"info": info_dict,
        b"announce": TRACKERS[0].encode(),
        b"announce-list": [[t.encode()] for t in TRACKERS],
        b"url-list": [w.encode() for w in webseeds],
        b"creation date": int(time.time()),
        b"comment": f"HF: {repo_id}@{row.sha[:7]} (cache)".encode(),
        b"created by": b"cache-to-torr-nostr.py",
    }
    infohash = hashlib.sha1(_bencode.encode(info_dict)).hexdigest()
    stem = f"{safe_slug(display_name)}-{row.sha[:7]}"
    if meta.get("quant_detail") and meta["quant_detail"] not in display_name \
            and meta["quant_detail"] != "many":
        stem += f".{safe_slug(meta['quant_detail'])}"

    out_dir.mkdir(parents=True, exist_ok=True)
    torrent_path = out_dir / f"{stem}.torrent"
    torrent_path.write_bytes(_bencode.encode(metainfo))

    cprint(C.GREEN, f"  torrent: {torrent_path.name}")
    cprint(C.DIM, f"  hash: {infohash}")
    cprint(C.DIM, f"  pieces: {piece_count}  webseeds: {len(webseeds)}")

    # --- magnet ---
    magnet = "&".join(
        [f"magnet:?xt=urn:btih:{infohash}", f"dn={quote(display_name)}",
         f"xl={total_size}"]
        + [f"tr={quote(t)}" for t in TRACKERS]
        + [f"ws={quote(w)}" for w in webseeds]
    )

    # --- manifest (in-memory; also persisted to ./events/<infohash>.json
    #     after the relay fan-out) ---
    ts = int(time.time())
    manifest = {
        "ts": ts,
        "torrent_path": str(torrent_path),
        "torrent_stem": stem,
        "infohash": infohash,
        "magnet": magnet,
        "name": row.sha,
        "total_size": total_size,
        "piece_length": pl,
        "piece_count": piece_count,
        "webseeds": webseeds,
        "trackers": list(TRACKERS),
        "source": f"huggingface.co/{repo_id}",
        "display_name": display_name,
        "repo_id": repo_id,
        "lab": meta.get("lab"),
        "model_name": meta.get("model_name"),
        "version": "main",
        "created_at": (str(getattr(info, "created_at", "") or "")
                       if info is not None else None) or None,
        "file_class": meta.get("file_class"),
        "model_kind": meta.get("model_kind"),
        "quant_type": meta.get("quant_type"),
        "quant_dev": meta.get("quant_dev"),
        "quant_detail": meta.get("quant_detail"),
        "quant_bpw": meta.get("quant_bpw"),
        "base_model": meta.get("base_model"),
        "commit_sha": row.sha,
        "hf_match": hf_match,
        "offline_mode": offline_mode,
        "files": [
            {"path": f.path, "size": f.size, "index": i}
            for i, f in enumerate(matched)
        ],
    }
    manifest = {k: v for k, v in manifest.items() if v is not None}
    return manifest


# ---------------------------------------------------------------------------
# ONE MODEL: BLOSSOM + SIGN + RELAY + PERSIST
# ---------------------------------------------------------------------------

async def process_one_model(nsec, row, args, out_dir, events_dir, log_path,
                            run_log_path, run_ts):
    cprint(C.BOLD, f"\n{'=' * 70}")
    cprint(C.BOLD, f"  [{row.idx}] {row.repo_id}@{row.sha[:7]}")
    cprint(C.BOLD, f"{'=' * 70}")

    result = {
        "ts": int(time.time()),
        "run_ts": run_ts,
        "idx": row.idx,
        "repo_id": row.repo_id,
        "commit_sha": row.sha,
        "built": False,
        "offline_mode": False,
        "torrent_path": None,
        "infohash": None,
        "torrent_size": None,
        "blossom_ok": 0,
        "blossom_total": len(BLOSSOM_SERVERS),
        "blossom_urls": [],
        "event_id": None,
        "relays_ok": 0,
        "relays_total": len(NOSTR_RELAYS),
        "success": False,
        "error": None,
    }

    if args.dry_run:
        cprint(C.YELLOW, "  --dry-run: skipping torrent build + publish.")
        # Still try classification for visibility.
        # (build_torrent_for_model prints classification before writing.)
        m = build_torrent_for_model(row, args, out_dir)
        if m is not None:
            result["built"] = True
            result["offline_mode"] = m.get("offline_mode", False)
            result["torrent_path"] = m.get("torrent_path")
            result["infohash"] = m.get("infohash")
            # NOTE: dry-run actually writes the .torrent (build_torrent writes
            # the file unconditionally). Acceptable since --dry-run is for
            # local inspection; remove the file to keep clean.
            try:
                os.remove(m["torrent_path"])
                cprint(C.DIM, f"  (dry-run removed {m['torrent_path']})")
            except OSError:
                pass
        # run log line
        with open(run_log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(result, ensure_ascii=False) + "\n")
        return result

    manifest = build_torrent_for_model(row, args, out_dir)
    if manifest is None:
        result["error"] = "torrent build failed"
        with open(run_log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(result, ensure_ascii=False) + "\n")
        cprint(C.RED, f"  [fail] could not build torrent for {row.repo_id}")
        return result

    result["built"] = True
    result["offline_mode"] = manifest.get("offline_mode", False)
    result["torrent_path"] = manifest["torrent_path"]
    result["infohash"] = manifest["infohash"]
    result["torrent_size"] = os.path.getsize(manifest["torrent_path"])

    # --- Blossom upload ---
    blossom_results = await phase_blossom(nsec, manifest["torrent_path"])
    blossom_urls = [u for _, ok, u, _ in blossom_results if ok and u]
    result["blossom_ok"] = sum(1 for _, ok, _, _ in blossom_results if ok)
    result["blossom_urls"] = blossom_urls

    # --- Sign kind 30099 event ---
    with open(manifest["torrent_path"], "rb") as f:
        torrent_sha256 = hashlib.sha256(f.read()).hexdigest()
    torrent_size = result["torrent_size"]
    hf_match = manifest.get("hf_match", "unknown")
    offline_mode = manifest.get("offline_mode", False)
    cprint(C.DIM, "  [nostr] signing kind 30099 listing event...")
    event_obj = await sign_listing_event(
        nsec, manifest, blossom_urls, torrent_sha256, torrent_size,
        hf_match, offline_mode, pow_seconds=args.pow_seconds)
    if event_obj is None:
        result["error"] = "signing failed"
        with open(run_log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(result, ensure_ascii=False) + "\n")
        cprint(C.RED, "  [fail] could not sign event")
        return result
    event_id = event_obj.id().to_hex()
    result["event_id"] = event_id
    cprint(C.DIM, f"  [nostr] event id: {event_id}")

    # --- Relay fan-out ---
    ok_count, fail_count = await phase_relay(event_obj, log_path)
    result["relays_ok"] = ok_count
    result["relays_total"] = len(NOSTR_RELAYS)
    success = ok_count >= 1
    result["success"] = success
    if not success:
        result["error"] = "no relay accepted the event"

    # --- Persist event JSON on success ---
    if success:
        events_dir.mkdir(parents=True, exist_ok=True)
        ev_path = events_dir / f"{manifest['infohash']}.json"
        try:
            ev_json = json.loads(event_obj.as_json())
        except Exception:
            ev_json = {"raw": event_obj.as_json()}
        payload = {
            "event": ev_json,
            "meta": {
                "repo_id": manifest["repo_id"],
                "commit_sha": manifest["commit_sha"],
                "torrent_path": manifest["torrent_path"],
                "infohash": manifest["infohash"],
                "magnet": manifest["magnet"],
                "display_name": manifest.get("display_name"),
                "blossom_urls": blossom_urls,
                "relays_ok": ok_count,
                "relays_total": len(NOSTR_RELAYS),
                "event_id": event_id,
                "hf_match": hf_match,
                "offline_mode": offline_mode,
                "ts": int(time.time()),
            },
        }
        with open(ev_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)
            f.write("\n")
        cprint(C.GREEN, f"  event saved: {ev_path}")

    # --- Run log line ---
    with open(run_log_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(result, ensure_ascii=False) + "\n")

    tag = "done" if success else "fail"
    cprint(C.GREEN if success else C.RED,
           f"  [{tag}] blossom {result['blossom_ok']}/{result['blossom_total']}, "
           f"relays {ok_count}/{len(NOSTR_RELAYS)}, "
           f"event {event_id[:16]}...")
    return result


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser(
        description="Scan HF cache, build .torrent per model, upload to "
                    "Blossom, sign kind 30099 Nostr event, fan out to relays.")
    p.add_argument("--cache-dir", default=None,
                   help="override HF cache folder (default: "
                        "huggingface_hub.constants.HF_HUB_CACHE)")
    p.add_argument("--out", default="./torrents",
                   help="torrent output dir (default: ./torrents)")
    p.add_argument("--events-dir", default="./events",
                   help="event JSON output dir (default: ./events)")
    p.add_argument("--yes", action="store_true",
                   help="skip model-selection prompt (process all selectable)")
    p.add_argument("--allow-partial", action="store_true",
                   help="torrent partial caches (default: skip if not 100%%)")
    p.add_argument("--offline", action="store_true",
                   help="force offline mode (no HF API + no checksum verify)")
    p.add_argument("--dry-run", action="store_true",
                   help="enumerate + classify only, write nothing")
    p.add_argument("--limit", type=int, default=0,
                   help="stop after N models (0 = no limit)")
    p.add_argument("--piece-length", type=int, default=None,
                   help="piece length in bytes (default: auto)")
    p.add_argument("--pow-seconds", type=int, default=POW_DEFAULT_SECONDS,
                   help="seconds to mine a NIP-13 nonce tag (proof-of-work) "
                        "before signing each kind 30099 event; 0 disables "
                        f"(default: {POW_DEFAULT_SECONDS})")
    args = p.parse_args()

    # --- startup guards ---
    if not _NOSTR_SDK_AVAILABLE:
        cprint(C.RED, "ERROR: nostr-sdk not installed; pip install nostr-sdk")
        return 2
    if not _TORF_AVAILABLE:
        cprint(C.RED, "ERROR: torf not installed; pip install torf")
        return 2
    if not _HF_AVAILABLE:
        cprint(C.RED, "ERROR: huggingface_hub not installed; "
               "pip install huggingface_hub")
        return 2

    os.makedirs(LOG_DIR, exist_ok=True)

    # --- detect cache dir ---
    try:
        cache = detect_cache_dir(args.cache_dir)
    except SystemExit as e:
        cprint(C.RED, str(e))
        return 2
    cprint(C.BOLD + C.CYAN, "\n  HF cache folder")
    cprint(C.DIM, f"    {cache}")
    if not cache.is_dir():
        cprint(C.RED, f"  cache folder does not exist: {cache}")
        return 2

    # --- enumerate models ---
    model_dirs = sorted(p for p in cache.iterdir()
                        if p.is_dir() and p.name.startswith("models--"))
    if not model_dirs:
        cprint(C.YELLOW, "  no models--* folders found in cache.")
        return 0
    cprint(C.DIM, f"  found {len(model_dirs)} model folder(s)")

    rows = []
    for i, md in enumerate(model_dirs, 1):
        parsed = parse_model_dir(md)
        if parsed is None:
            cprint(C.YELLOW, f"  [{i}] {md.name}: could not parse; skipping")
            continue
        org, repo, sha, snap_dir, on_disk = parsed
        if sha is None:
            cprint(C.YELLOW, f"  [{i}] {org}/{repo}: no commit SHA found")
            rows.append(ModelRow(i, org, repo, None, None, [],
                                 hf_status="NO_SHA"))
            continue
        if snap_dir is None:
            cprint(C.YELLOW, f"  [{i}] {org}/{repo}@{sha[:7]}: "
                   "snapshot dir missing")
            rows.append(ModelRow(i, org, repo, sha, None, on_disk,
                                 hf_status="NO_SHA"))
            continue

        # probe HF for canonical file list (cheap; we'll re-fetch per model
        # during the build phase anyway — this is just for the table)
        hf_paths = None
        hf_status = "OK"
        if not args.offline:
            try:
                api = HfApi()
                tree = list(api.list_repo_tree(
                    repo_id=f"{org}/{repo}", revision=sha, recursive=True))
                hf_paths = {e.path for e in tree
                            if isinstance(e, RepoFile)}
            except Exception:
                hf_status = "OFFLINE"
        row = ModelRow(i, org, repo, sha, snap_dir, on_disk,
                       hf_tree_files=hf_paths, hf_status=hf_status)
        # adjust status to PARTIAL if HF says incomplete
        if hf_paths is not None and on_disk and \
                {p for p, _ in on_disk} != hf_paths:
            row.hf_status = "PARTIAL"
        elif hf_paths is not None and not on_disk:
            row.hf_status = "PARTIAL"
        rows.append(row)

    # --- print table ---
    cprint(C.BOLD, "\n  cached models:")
    cprint(C.DIM, "  " + "-" * 78)
    cprint(C.DIM, f"  {'idx':>3}  {'repo_id':40s}  {'commit':10s}  "
           f"{'disk':>5}  {'hf':>5}  {'size':>9}  status")
    cprint(C.DIM, "  " + "-" * 78)
    for r in rows:
        repo_id = r.repo_id[:40]
        sha_s = (r.sha[:7] if r.sha else "—")
        disk_n = r.on_disk_count
        hf_n = r.hf_count if r.hf_count is not None else "—"
        size_s = _human_size(r.total_size) if r.total_size else "—"
        status = r.hf_status
        if status == "PARTIAL":
            color = C.YELLOW
        elif status == "OK":
            color = C.GREEN
        elif status == "OFFLINE":
            color = C.MAGENTA
        else:
            color = C.RED
        cprint(color, f"  {r.idx:>3}  {repo_id:40s}  {sha_s:10s}  "
               f"{disk_n!s:>5}  {hf_n!s:>5}  {size_s:>9}  {status}")
    cprint(C.DIM, "  " + "-" * 78)
    cprint(C.DIM, f"  selectable (allow_partial={args.allow_partial}): "
           f"{sum(1 for r in rows if r.is_selectable(args.allow_partial))}")

    # --- selection ---
    selected = prompt_selection(rows, args.allow_partial, args.yes)
    if args.limit and len(selected) > args.limit:
        cprint(C.DIM, f"  --limit {args.limit}: truncating selection")
        selected = selected[:args.limit]
    if not selected:
        cprint(C.YELLOW, "  nothing to do.")
        return 0
    cprint(C.GREEN, f"\n  selected {len(selected)} model(s) for torrent generation")

    # --- nsec (skipped in dry-run; we never sign or post) ---
    if args.dry_run:
        cprint(C.DIM, "  --dry-run: skipping NSEC prompt.")
        nsec = None
    else:
        cprint(C.BOLD, "\n  Nostr identity")
        nsec = get_nsec()

    # --- run ---
    run_ts = int(time.time())
    run_log_path = os.path.join(args.events_dir, f"run_{run_ts}.jsonl")
    os.makedirs(args.events_dir, exist_ok=True)
    log_path = os.path.join(LOG_DIR, f"cache_nostr_send_log_{run_ts}.jsonl")
    # Touch run log
    with open(run_log_path, "a", encoding="utf-8") as f:
        f.write(json.dumps({"run_start": run_ts,
                            "n_models": len(selected),
                            "allow_partial": args.allow_partial,
                            "offline": args.offline}) + "\n")

    results = []
    try:
        for r in selected:
            res = asyncio.run(process_one_model(
                nsec, r, args, Path(args.out), Path(args.events_dir),
                log_path, run_log_path, run_ts))
            results.append(res)
    except KeyboardInterrupt:
        cprint(C.YELLOW, "\n  interrupted — partial run saved.")
        return 130

    # --- final summary ---
    built = sum(1 for r in results if r and r.get("built"))
    sent = sum(1 for r in results if r and r.get("success"))
    failed = sum(1 for r in results if r and not r.get("success"))

    cprint(C.BOLD, "\n  " + "=" * 70)
    cprint(C.BOLD, "  SUMMARY")
    cprint(C.BOLD, "  " + "=" * 70)
    cprint(C.DIM, f"  {'idx':>3}  {'repo_id':40s}  {'commit':10s}  "
           f"{'torrent':8s}  {'blossom':10s}  {'event':18s}  relays")
    cprint(C.DIM, "  " + "-" * 78)
    for r in results:
        if r is None:
            continue
        repo_id = r["repo_id"][:40]
        sha_s = r["commit_sha"][:7] if r["commit_sha"] else "—"
        tor = "yes" if r.get("built") else "FAIL"
        blo = f"{r['blossom_ok']}/{r['blossom_total']}"
        ev = (r["event_id"][:16] + "…") if r.get("event_id") else "—"
        rel = f"{r['relays_ok']}/{r['relays_total']}"
        if r.get("success"):
            color = C.GREEN
        elif r.get("built"):
            color = C.YELLOW
        else:
            color = C.RED
        cprint(color, f"  {r['idx']:>3}  {repo_id:40s}  {sha_s:10s}  "
               f"{tor:8s}  {blo:10s}  {ev:18s}  {rel}")
    cprint(C.DIM, "  " + "-" * 78)
    cprint(C.BOLD, f"  built={built}  sent={sent}  failed={failed}")
    cprint(C.DIM, f"  torrents:  {args.out}/")
    cprint(C.DIM, f"  events:    {args.events_dir}/")
    cprint(C.DIM, f"  send log:  {log_path}")
    cprint(C.DIM, f"  run log:   {run_log_path}")

    # --- seeding instructions ---
    seedable = [r for r in results if r and r.get("built")]
    if seedable:
        cprint(C.BOLD + C.CYAN, "\n  " + "=" * 70)
        cprint(C.BOLD + C.CYAN, "  HOW TO SEED THESE TORRENTS")
        cprint(C.BOLD + C.CYAN, "  " + "=" * 70)
        cprint(C.DIM, "  The .torrent files have webseeds pointing at HF, so any")
        cprint(C.DIM, "  client can download the data from HF and then seed it.")
        cprint(C.DIM, "  Two paths:\n")
        cprint(C.BOLD, "  A) Let the client fetch from HF (no extra setup):")
        cprint(C.DIM, "     Open each .torrent in your client (qBittorrent,")
        cprint(C.DIM, "     Transmission, aria2, etc.). It pulls from the 4 HF")
        cprint(C.DIM, "     webseeds + 5 UDP trackers, then seeds automatically.\n")
        cprint(C.BOLD, "  B) Reuse your HF cache (skip the re-download):")
        cprint(C.DIM, "     For each model, copy real files out of the symlinked")
        cprint(C.DIM, "     cache (the cache snapshot uses symlinks into blobs/,")
        cprint(C.DIM, "     which a torrent client can't seed through):")
        for r in seedable:
            sha = r["commit_sha"]
            repo = r["repo_id"]
            cprint(C.GREEN, f"       hf download {repo} --revision {sha} "
                   f"--local-dir ./seed/{sha}")
        cprint(C.DIM, "     Then in your client add the .torrent with the")
        cprint(C.DIM, "     download/save location set to ./seed/ (the client")
        cprint(C.DIM, "     creates the <sha> subfolder itself from the torrent")
        cprint(C.DIM, "     name), force-recheck, and start.")
        cprint(C.DIM, "     Note: this ~2x's disk use until you reclaim the")
        cprint(C.DIM, "     cache copy (`huggingface-cli delete-cache` or")
        cprint(C.DIM, "     `rm -rf ~/.cache/huggingface/hub/models--<org>--<repo>`).")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""maker-v6.py — streaming in-memory multi-branch HuggingFace torrent builder
==============================================================================

WHAT THIS IS
------------
llama.garden (https://llama.garden) is a torrent index of language models.
Models are mirrored from HuggingFace into BitTorrent swarms so they can be
fetched peer-to-peer, cached by seeders, and survive link rot. This script
turns one HuggingFace repo into one .torrent file that anyone can publish on
the Nostr relay network (kind 30099 listing events, see
`post-torr-nostr-j.py`) and that the `waifu-magnet` viewer renders as cards.

maker-v6 is the STREAMING builder. Unlike the disk-based `maker-v3.py` /
`maker-v5.py`, v6 touches NO disk for model data: every file is streamed
over HTTP straight into the companion Go binary `hf-piece-hasher/main.go`,
which feeds the bytes into a rolling SHA1 piece buffer, appends the
virtual BEP-5 padding zeros, and writes the raw 20-byte piece digests to
stdout in file order. Python reads those digests, builds the info dict,
bencodes it, and writes only the .torrent + manifest to ./queue/.

Use v6 when:
  - You want a .torrent without downloading 100+ GiB to disk.
  - You are on a beefy box (fast CPU + big pipe) and want sub-10-minute
    builds for terabyte-class repos (verified: 1.4 TiB in ~5 min on a
    200 Gbps ARM64 node, 434 GiB in 18 min on a 1 Gbps Xeon).
  - You already seed from a separate machine / HuggingFace webseed, so a
    local copy is unnecessary.

Use maker-v3.py instead when you want the downloaded files on disk for
seeding from the same host, or maker-v5.py when you want to shard a huge
build across N machines (v5 keeps the same infohash across shards; v6 does
not shard).

PREREQUISITES
-------------
  - Python 3.10+ with: huggingface_hub, requests, torf, gguf (optional).
  - The Go binary. Build it once:
        cd hf-piece-hasher && go build .
    (Go 1.22+; go.mod says 1.23 but `sed -i 's/go 1.23/go 1.22/'` works.)
  - A HuggingFace read token (REQUIRED for any gated repo, strongly
    recommended even for public repos to avoid anonymous rate limits):
        export HF_TOKEN=hf_xxx
  - No local Transmission, no MUSCLE_* env, no temp dir. v6 is hash-only.

QUICK START
-----------
  # 1. Build a repo into a .torrent + manifest (hash only, no disk I/O —
  #    this is the default):
  export HF_TOKEN=hf_xxx
  python maker-v6.py turboderp/Qwen3.6-27B-exl3 --num-workers 16

  # 2. Same, but also write the model files to disk as they stream (so you
  #    can seed from this host afterwards). Writing is opt-in:
  python maker-v6.py org/repo --num-workers 16 --write-dir ./seeds/<repo>

  # 3. Publish the finished torrent to llama.garden's Nostr index:
  python post-torr-nostr-j.py <repo>            # reads ./queue/ manifest

  # 4. Remote build over SSH (recommended for big repos — see
  #    docs/maker-v6-remote-build.md for the full guide):
  ssh remote "cd ~/ob && HF_TOKEN=hf_xxx python3 maker-v6.py org/repo \
    --num-workers 16 --chunk-size 4194304"

FLAGS
-----
  --num-workers N    parallel HTTP streams in the Go hasher (default 8)
  --chunk-size B     read chunk size (default 1 MiB)
  --max-retries N    per-file resume retries (default 10)
  --hasher-bin PATH  path to the Go binary (default ./hf-piece-hasher)
  --no-write          (removed — no-write is now the default)
  --write-dir DIR     write the streamed files to DIR (opt-in; default OFF)
  --mask GLOB         repeatable basename glob to include (default all)
  --redirector-host   redirector host for the CNAME/N@ webseeds
                      (default api.llama.garden)

Removed vs v5 (all disk/seed concerns): --yes, --delete, --keep-dir,
--shard, --num-downloaders, --hash-workers, --no-verify, --min-free-gb,
--no-write (no-write is now the default; use --write-dir to opt in).

TORRENT LAYOUT (same as v3/v4/v5)
---------------------------------
  info.name = <repo>              (e.g. "Qwen3.6-27B-exl3", no slash, no org)
  file path  = ["resolve", <commit_sha>, ...original_file_segments]

libtorrent appends <name>/<path> to trailing-slash webseeds:
  HF direct:   https://huggingface.co/<org>/  + <repo>/resolve/<commit>/<file>
               = https://huggingface.co/<org>/<repo>/resolve/<commit>/<file>  OK
  CNAME redir: http://z<i>.api.llama.garden/<org>/<repo>/<ih>/  + <repo>/resolve/<commit>/<file>
               = http://z<i>.api.llama.garden/<org>/<repo>/<ih>/<repo>/resolve/<commit>/<file>  OK

7 webseeds (1 HF + 2 CNAME + 4 N@). Same as v5.

PIPELINE
--------
  1. Parse repo id (org/repo or full HF URL).
  2. Enumerate ALL branches via HfApi().list_repo_refs().
  3. For each branch: resolve commit SHA, list files, collect into one
     matched list. Dedup by (commit_sha, path).
  4. Classify: GENERAL (no hard-coded EXL3). Detection order:
       a. GGUF present  -> quant_type="gguf", quant_detail from filename
          tokens (mixed -> "many"), cross-checked against GGUF header
          metadata (peek_gguf_header_http, a 2 MiB Range fetch).
       b. config.json across selected branches -> quant_type / quant_detail
          from quantization_config.quant_method.
       c. Repo-name / HF-tag heuristics (classify_repo) -> fallback.
       d. None of the above -> quant_type=None, file_class from
          classify_repo (base / fine tune).
  5. Stream+hash: spawn ./hf-piece-hasher with a JSON spec on stdin.
     The Go binary opens N parallel HTTP streams, feeds each chunk into a
     rolling piece-buffer, appends virtual BEP-5 pad bytes, and writes the
     raw 20-byte SHA1 digests to stdout in file order. No disk I/O anywhere.
  6. Write .torrent + manifest to ./queue/ for the Nostr publisher.

STANDALONE
----------
All helpers (colors, trackers, classification, padding, webseed URLs,
bencode) are defined inline here. Runtime imports: huggingface_hub,
requests, torf, gguf (optional).

SEE ALSO
--------
  - maker-v3.py  : disk-based builder (downloads files, hashes on disk).
  - maker-v5.py  : sharded disk builder (same infohash across N machines).
  - hf-piece-hasher/main.go : the Go streaming hasher this script drives.
  - post-torr-nostr-j.py : publish the .torrent + manifest to Nostr (kind 30099).
  - approve-submissions.py : review other people's kind 30099 submissions.
  - wip.md / map.md / docs/maker-v6-remote-build.md : full guides.
"""

import argparse
import fnmatch
import hashlib
import json
import os
import re
import struct
import subprocess
import sys
import time
from pathlib import Path
from urllib.parse import quote, urlparse

import requests

os.environ.setdefault("HF_HUB_DISABLE_PROGRESS_BARS", "1")

from huggingface_hub import HfApi, hf_hub_download
from huggingface_hub.hf_api import RepoFile
from torf import _flatbencode as _bencode

# gguf is optional — only needed when the repo contains .gguf files.
try:
    from gguf import GGUFReader
    from gguf.constants import LlamaFileType
    _GGUF_OK = True
except Exception:
    _GGUF_OK = False


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
    BLUE = "\033[34m"
    MAGENTA = "\033[35m"
    CYAN = "\033[36m"
    GRAY = "\033[90m"


def cprint(color, msg, end="\n"):
    print(f"{color}{msg}{C.R}", file=sys.stderr, end=end)


# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

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

REDIRECTOR_HOST = "api.llama.garden"
REDIRECTOR_CNAME_PREFIX = "z"  # z1..zN.api.llama.garden
REDIRECTOR_CNAME_COUNT = 2
REDIRECTOR_USERINFO_COUNT = 4

_PAD_SIZES_PATH = "pad-sizes.json"

# Default path to the Go hashing binary. Resolved relative to this script
# so it works from any cwd. Override with --hasher-bin.
_HASHER_BIN_DEFAULT = str(Path(__file__).resolve().parent / "hf-piece-hasher"
                          / "hf-piece-hasher")


# ---------------------------------------------------------------------------
# CLASSIFICATION (ported from maker-v5.py)
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
    (r"-?EXL3$", "exl3"),
    (r"-?EXL2$", "exl2"),
]
NAME_TOKEN_TYPES = {
    "NVFP4": "nvfp4", "MXFP4": "mxfp4", "FP8": "fp8",
    "EXL3": "exl3", "EXL2": "exl2",
}
GGUF_TOKEN_RE = re.compile(
    r"(UD-)?(BF16|F16|F32|Q\d_K_[A-Z]+|Q\d_K|IQ\d_[A-Z]+|IQ\d|Q\d_\d|Q\d)"
)
QUANT_NAME_STRIP = re.compile(
    r"-?(?:GGUF|MLX(?:-\d+bit)?|AWQ(?:-(?:\d+bit|INT\d+))?|"
    r"GPTQ(?:-(?:\d+bit|INT\d+))?|"
    r"FP8|NVFP4|MXFP4|BNB|ONNX|EXL3|EXL2)"
    r"(?:-[A-Za-z0-9_]+)*$"
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

QUANT_METHOD_MAP = {
    "awq": "awq", "gptq": "gptq", "bitsandbytes": "bnb", "bnb": "bnb",
    "fp8": "fp8", "exl2": "exl2", "exl3": "exl3", "gguf": "gguf",
    "mlx": "mlx", "onnx": "onnx", "nvfp4": "nvfp4", "mxfp4": "mxfp4",
}

_GGUF_FILETYPE_TOKENS = {}
if _GGUF_OK:
    for _t in LlamaFileType:
        _name = _t.name
        if _name.startswith("MOSTLY_"):
            _tok = _name[len("MOSTLY_"):]
            _GGUF_FILETYPE_TOKENS[_t.value] = _tok
_GGUF_FILETYPE_TOKENS.update({
    33: "Q4_0_4_4", 34: "Q4_0_4_8", 35: "Q4_0_8_8",
})


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
    if quant_type in ("exl2", "exl3"):
        return quant_type.upper()
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
        stripped = QUANT_NAME_STRIP.sub("", repo_name) or repo_name
        if base_ref:
            repo_params = set(re.findall(r'\d+B', stripped))
            base_params = set(re.findall(r'\d+B', base_ref[2]))
            if repo_params and base_params and repo_params != base_params:
                lab = org
                model_name = stripped
            else:
                lab, model_name = base_ref[1], base_ref[2]
        else:
            lab = org
            model_name = stripped
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
# GGUF header metadata reader (HTTP Range peek only — no local files in v6)
# ---------------------------------------------------------------------------

_GGUF_SCALAR_SIZES = {0: 1, 1: 1, 2: 2, 3: 2, 4: 4, 5: 4, 6: 4, 7: 1,
                      10: 8, 11: 8, 12: 8}


def _gguf_scalar_fields(buf):
    if not buf or buf[:4] != b"GGUF":
        return None
    off = 4
    try:
        _version, = struct.unpack_from("<I", buf, off); off += 4
        _tensor_count, = struct.unpack_from("<Q", buf, off); off += 8
        kv_count, = struct.unpack_from("<Q", buf, off); off += 8
    except struct.error:
        return None

    want = {"general.architecture", "general.name", "general.file_type",
            "general.quantization_name", "general.quantization_version"}
    out = {
        "architecture": None, "name": None,
        "file_type": None, "file_type_raw": None,
        "quant_name": None, "quant_version": None,
    }
    n = len(buf)

    def _read_str(o):
        (ln,) = struct.unpack_from("<Q", buf, o)
        o += 8
        if o + ln > n:
            raise struct.error
        return buf[o:o + ln].decode("utf-8", "replace"), o + ln

    for _ in range(kv_count):
        if not want:
            break
        try:
            (klen,) = struct.unpack_from("<Q", buf, off); off += 8
            if off + klen > n:
                break
            key = buf[off:off + klen].decode("utf-8", "replace")
            off += klen
            (vtype,) = struct.unpack_from("<I", buf, off); off += 4
        except struct.error:
            break

        if key in want and vtype != 9:
            try:
                if vtype == 8:
                    val, off = _read_str(off)
                elif vtype == 4:
                    val, = struct.unpack_from("<I", buf, off); off += 4
                elif vtype == 5:
                    val, = struct.unpack_from("<i", buf, off); off += 4
                elif vtype == 10:
                    val, = struct.unpack_from("<Q", buf, off); off += 8
                elif vtype == 11:
                    val, = struct.unpack_from("<q", buf, off); off += 8
                elif vtype == 6:
                    val, = struct.unpack_from("<f", buf, off); off += 4
                elif vtype == 12:
                    val, = struct.unpack_from("<d", buf, off); off += 8
                elif vtype == 0:
                    val, = struct.unpack_from("<B", buf, off); off += 1
                elif vtype == 1:
                    val, = struct.unpack_from("<b", buf, off); off += 1
                elif vtype == 7:
                    val, = struct.unpack_from("<B", buf, off); off += 1
                elif vtype == 2:
                    val, = struct.unpack_from("<H", buf, off); off += 2
                elif vtype == 3:
                    val, = struct.unpack_from("<h", buf, off); off += 2
                else:
                    val = None
                    off += _GGUF_SCALAR_SIZES.get(vtype, 0)
            except struct.error:
                break
            if key == "general.architecture":
                out["architecture"] = val
            elif key == "general.name":
                out["name"] = val
            elif key == "general.file_type":
                out["file_type_raw"] = val
                out["file_type"] = (_GGUF_FILETYPE_TOKENS.get(val)
                                    if (_GGUF_OK and val is not None) else None)
            elif key == "general.quantization_name":
                out["quant_name"] = val
            elif key == "general.quantization_version":
                out["quant_version"] = val
            want.discard(key)
        else:
            try:
                if vtype == 9:
                    (etype,) = struct.unpack_from("<I", buf, off); off += 4
                    (acount,) = struct.unpack_from("<Q", buf, off); off += 8
                    if etype == 8:
                        for _ in range(acount):
                            _s, off = _read_str(off)
                    else:
                        off += acount * _GGUF_SCALAR_SIZES.get(etype, 0)
                elif vtype == 8:
                    _s, off = _read_str(off)
                else:
                    off += _GGUF_SCALAR_SIZES.get(vtype, 0)
            except struct.error:
                break
        if off > n:
            break

    return out


def peek_gguf_header_http(url, timeout=20):
    if not _GGUF_OK:
        return None
    try:
        with requests.get(url, headers={"User-Agent": "maker-v6/1.0",
                          "Range": "bytes=0-2097151"},
                          timeout=timeout, stream=True) as r:
            r.raise_for_status()
            body = r.raw.read(2097152)
        if not body or body[:4] != b"GGUF":
            return None
        return _gguf_scalar_fields(body)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# config.json reader (per-branch fetch + aggregation)
# ---------------------------------------------------------------------------

def fetch_branch_config(api, repo_id, commit_sha, hf_token=None):
    try:
        p = hf_hub_download(
            repo_id=repo_id, filename="config.json", revision=commit_sha,
            token=hf_token)
        with open(p, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    except Exception:
        return None
    if not isinstance(cfg, dict):
        return None
    qc = cfg.get("quantization_config")
    if not isinstance(qc, dict):
        qc = None
    out = {
        "quant_method": (qc or {}).get("quant_method"),
        "bits": (qc or {}).get("bits"),
        "model_type": cfg.get("model_type"),
        "architectures": cfg.get("architectures"),
    }
    if not any(v is not None for v in out.values()):
        return None
    return out


def aggregate_branch_configs(branch_configs):
    with_method = [c for c in branch_configs
                   if c and c.get("quant_method")]
    if not with_method:
        return None, None

    methods = set()
    bitses = set()
    for c in with_method:
        m = c["quant_method"]
        if isinstance(m, str):
            methods.add(m.lower())
        bits = c.get("bits")
        if bits is not None:
            bitses.add(float(bits))

    if not methods:
        return None, None

    if len(methods) == 1:
        raw = next(iter(methods))
        qtype = QUANT_METHOD_MAP.get(raw, raw)
        if len(bitses) <= 1:
            if not bitses:
                detail = detect_non_gguf_detail("", qtype)
            else:
                b = next(iter(bitses))
                if qtype in ("exl2", "exl3"):
                    detail = f"{b:g}bpw" if len(with_method) == 1 else "many"
                elif qtype in ("awq", "gptq", "bnb"):
                    detail = f"{int(b) if b == int(b) else b:g}bit"
                else:
                    detail = f"{b:g}"
            return qtype, detail
        return qtype, "many"

    return "mixed", "many"


# ---------------------------------------------------------------------------
# HF URL parsing
# ---------------------------------------------------------------------------

def parse_hf_url(url):
    p = urlparse(url)
    if p.netloc not in ("huggingface.co", "www.huggingface.co", "hf-mirror.com"):
        raise ValueError(f"not a huggingface.co URL: {url}")
    parts = [s for s in p.path.split("/") if s]
    if len(parts) < 2:
        raise ValueError(f"URL must include org/repo: {url}")
    org, repo = parts[0], parts[1]
    revision = "main"
    subfolder = None
    file_mask = None
    if len(parts) >= 4 and parts[2] == "tree":
        revision = parts[3]
        if len(parts) > 4:
            subfolder = "/".join(parts[4:])
    elif len(parts) >= 4 and parts[2] == "blob":
        revision = parts[3]
        file_path = "/".join(parts[4:])
        if not file_path:
            raise ValueError(f"blob URL has no file path: {url}")
        if "/" in file_path:
            subfolder, basename = file_path.rsplit("/", 1)
        else:
            subfolder, basename = None, file_path
        file_mask = basename
    return org, repo, revision, subfolder, file_mask


# ---------------------------------------------------------------------------
# Misc helpers
# ---------------------------------------------------------------------------

def pick_piece_length(total):
    for pl in (16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384):
        pl *= 1024
        if total / pl <= 2048:
            return pl
    return 16 * 1024 * 1024


def safe_slug(s):
    return re.sub(r"[^A-Za-z0-9._-]", "-", s).strip("-") or "model"


def _human_size(n):
    for u in ("B", "KiB", "MiB", "GiB", "TiB"):
        if n < 1024:
            return f"{n:.1f} {u}" if u != "B" else f"{n:.0f} B"
        n /= 1024
    return f"{n:.1f} PiB"


# ---------------------------------------------------------------------------
# BEP-5 padding
# ---------------------------------------------------------------------------

def _bep5_padding_sizes(matched, piece_length):
    """BEP-5 padding: list of (filename, size) for each .pad file needed.
    Returns exactly len(matched) entries — one per file, positionally
    indexed by the file's position in matched. A pad whose size is 0
    means that file already ends on a piece boundary (no pad file emitted)."""
    pads = []
    cumulative = 0
    for i, f in enumerate(matched):
        cumulative += (f.size or 0)
        rem = cumulative % piece_length
        pad = (piece_length - rem) % piece_length
        pads.append((f".pad.{i}", pad))
        cumulative += pad
    return pads


def _record_pad_sizes(infohash, pads):
    real_pads = [(name, size) for name, size in pads if size > 0]
    if not real_pads:
        return
    p = Path(_PAD_SIZES_PATH)
    data = {}
    try:
        if p.exists():
            with open(p, "r", encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, dict):
                data = {}
    except (OSError, ValueError):
        data = {}
    data[infohash.lower()] = {name: int(size) for name, size in real_pads}
    tmp = p.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    tmp.replace(p)


# ---------------------------------------------------------------------------
# MBFile — lightweight stand-in for RepoFile with branch/commit metadata
# ---------------------------------------------------------------------------

class MBFile:
    __slots__ = ("path", "size", "lfs_sha256", "blob_id", "commit_sha",
                 "_auto_pad")

    def __init__(self, path, size, lfs_sha256=None, blob_id=None,
                 commit_sha=None):
        self.path = path
        self.size = size
        self.lfs_sha256 = lfs_sha256
        self.blob_id = blob_id
        self.commit_sha = commit_sha
        self._auto_pad = False

    @property
    def lfs(self):
        if self.lfs_sha256:
            class _Lfs:
                sha256 = self.lfs_sha256
            return _Lfs()
        return None

    def torrent_path_segments(self):
        return ["resolve", self.commit_sha] + self.path.split("/")

    def __repr__(self):
        return (f"MBFile({self.path} "
                f"{self.size} LFS={self.lfs_sha256 is not None})")


# ---------------------------------------------------------------------------
# File listing (single commit)
# ---------------------------------------------------------------------------

def collect_files(api, repo_id, commit_sha, subfolder=None):
    """List all files at a given commit. If subfolder is set, only files
    whose path starts with that prefix are included."""
    tree = list(api.list_repo_tree(
        repo_id=repo_id, revision=commit_sha, recursive=True))
    prefix = subfolder.rstrip("/") + "/" if subfolder else None
    out = []
    for e in tree:
        if not hasattr(e, "size"):
            continue
        if prefix and not e.path.startswith(prefix):
            continue
        lfs_sha256 = None
        blob_id = None
        if e.lfs is not None:
            lfs_sha256 = e.lfs.sha256
        else:
            blob_id = getattr(e, "blob_id", None)
        out.append(MBFile(
            path=e.path, size=e.size or 0,
            lfs_sha256=lfs_sha256, blob_id=blob_id,
            commit_sha=commit_sha))
    return out


# ---------------------------------------------------------------------------
# Webseed URL construction
# ---------------------------------------------------------------------------

def webseeds_hf_multibranch(org):
    return [f"https://huggingface.co/{org}/"]


def webseeds_cname_multibranch(org, repo, infohash, redirector_host):
    return [f"http://{REDIRECTOR_CNAME_PREFIX}{i}.{redirector_host}/{org}/{repo}/{infohash}/"
            for i in range(1, REDIRECTOR_CNAME_COUNT + 1)]


def webseeds_userinfo_multibranch(org, repo, infohash, redirector_host):
    return [f"http://{i}@{redirector_host}/{org}/{repo}/{infohash}/"
            for i in range(1, REDIRECTOR_USERINFO_COUNT + 1)]


def all_webseeds_multibranch(org, repo, infohash, redirector_host):
    return (webseeds_hf_multibranch(org)
            + webseeds_cname_multibranch(org, repo, infohash, redirector_host)
            + webseeds_userinfo_multibranch(org, repo, infohash, redirector_host))


# ---------------------------------------------------------------------------
# Go hasher invocation
# ---------------------------------------------------------------------------

def run_go_hasher(matched, pads, piece_length, hasher_bin, num_workers,
                  chunk_size, max_retries, hf_token, origin, org, repo,
                  write_dir=None):
    """Spawn the Go hf-piece-hasher binary with a JSON spec on stdin.

    Reads progress JSON lines from stderr and the raw 20-byte piece
    digests from stdout. Returns (pieces_bytes, None) on success or
    (None, error_message) on failure.

    If write_dir is set, each file's out_path is set to
    <write_dir>/<repo>/resolve/<commit>/<file> so the Go binary writes
    files to disk as it streams (write-only, never read back).

    Progress display: a single status line redrawn in place, matching
    v5's prog_loop style:
      stream: 45.2% 2.3GB/5.1GB 632.0 MB/s  |  files: 18/24 done
    """
    spec_files = []
    for i, f in enumerate(matched):
        url = f"{origin}/{org}/{repo}/resolve/{f.commit_sha}/{f.path}"
        entry = {
            "index": i,
            "url": url,
            "size": f.size,
            "pad_size": pads[i][1],
        }
        if write_dir:
            entry["out_path"] = str(Path(write_dir) / repo / "resolve"
                                    / f.commit_sha / f.path)
        spec_files.append(entry)
    if write_dir and spec_files:
        cprint(C.DIM, f"  spec[0].out_path = {spec_files[0].get('out_path', '<MISSING>')}")
    elif not write_dir:
        cprint(C.YELLOW, "  [no-write] hasher will NOT write files to disk")
    spec = {
        "piece_length": piece_length,
        "files": spec_files,
        "num_workers": num_workers,
        "chunk_size": chunk_size,
        "max_retries": max_retries,
    }
    if hf_token:
        spec["hf_token"] = hf_token

    total_size = sum(f.size + pads[i][1] for i, f in enumerate(matched))
    total_files = len(matched)
    files_done = [False] * total_files

    proc = subprocess.Popen(
        [hasher_bin],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=0,
    )
    proc.stdin.write(json.dumps(spec).encode())
    proc.stdin.close()

    pieces = bytearray()
    err_lines = []
    files_completed = 0

    # Read stderr line by line for progress, read stdout in a separate
    # thread so it doesn't block on stderr. The Go binary writes pieces
    # to stdout only at the very end (after all files are hashed), so
    # stdout is quiescent during the streaming phase.
    import threading

    stdout_buf = bytearray()

    def _read_stdout():
        while True:
            chunk = proc.stdout.read(1 << 20)
            if not chunk:
                break
            stdout_buf.extend(chunk)

    stdout_thread = threading.Thread(target=_read_stdout, daemon=True)
    stdout_thread.start()

    for raw_line in proc.stderr:
        line = raw_line.decode("utf-8", "replace").strip()
        if not line:
            continue
        try:
            ev = json.loads(line)
        except json.JSONDecodeError:
            err_lines.append(line)
            continue
        etype = ev.get("type")
        if etype == "progress":
            cur = ev.get("bytes", 0)
            rate_str = ev.get("rate", "")
            pct = 100.0 * cur / total_size if total_size else 0
            done = sum(1 for d in files_done if d)
            cprint(C.GRAY, f"\r\x1b[K  stream: {pct:5.1f}% "
                   f"{_human_size(cur)}/{_human_size(total_size)} "
                   f"{rate_str}  |  files: {done}/{total_files} done",
                   end="")
        elif etype == "file_done":
            idx = ev.get("index", -1)
            if 0 <= idx < total_files:
                files_done[idx] = True
        elif etype == "file_err":
            idx = ev.get("index", -1)
            msg = ev.get("msg", "?")
            cprint(C.RED, f"\n  [file {idx}] {msg}")
            err_lines.append(f"file {idx}: {msg}")
        elif etype == "done":
            cur = ev.get("bytes", 0)
            elapsed = ev.get("elapsed", "?")
            rate_str = ev.get("rate", "?")
            cprint(C.GREEN, f"\r\x1b[K  streamed + hashed {_human_size(cur)} "
                   f"in {elapsed} ({rate_str})                    ")
        elif etype == "error":
            err_lines.append(ev.get("msg", "unknown error"))
            cprint(C.RED, f"\n  [hasher] {ev.get('msg', 'error')}")

    proc.wait()
    stdout_thread.join(timeout=5)

    if proc.returncode != 0:
        return None, (f"hasher exited {proc.returncode}: "
                      + "; ".join(err_lines[-3:]))

    pieces = bytes(stdout_buf)
    expected = sum(_piece_count_for_file(f.size, pads[i][1], piece_length)
                   for i, f in enumerate(matched)) * 20
    if len(pieces) != expected:
        return None, (f"piece count mismatch: got {len(pieces)} bytes "
                      f"({len(pieces)//20} pieces), expected {expected} bytes "
                      f"({expected//20} pieces)")
    return pieces, None


def _piece_count_for_file(file_size, pad_size, piece_length):
    """Number of BT v1 pieces a single file + its pad contributes.
    Matches the Go binary's flushPieces + final-fragment logic."""
    total = file_size + pad_size
    return (total + piece_length - 1) // piece_length


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser(
        description="Build a multi-branch .torrent for a HF repo (any "
                    "quant family). Streams from HF over HTTP into the Go "
                    "hf-piece-hasher binary — no disk, no Xet, no local "
                    "Transmission. Classification auto-detected.")
    p.add_argument("repo", help="HF repo id (org/repo) or full HF URL")
    p.add_argument("--out", default="./torrents",
                   help="output directory (default: ./torrents)")
    p.add_argument("--queue-dir", default="./queue",
                   help="manifest output dir (default: ./queue)")
    p.add_argument("--hasher-bin", default=_HASHER_BIN_DEFAULT,
                   help=f"path to the Go hf-piece-hasher binary "
                        f"(default: {_HASHER_BIN_DEFAULT})")
    p.add_argument("--num-workers", type=int, default=8,
                   help="parallel HTTP streams in the Go hasher "
                        "(default: 8)")
    p.add_argument("--chunk-size", type=int, default=1 << 20,
                   help="read chunk size in bytes (default: 1 MiB)")
    p.add_argument("--max-retries", type=int, default=10,
                   help="per-file resume retries (default: 10)")
    p.add_argument("--piece-length", type=int, default=None,
                   help="piece length in bytes (default: auto)")
    p.add_argument("--no-manifest", action="store_true",
                   help="skip writing manifest to ./queue/")
    p.add_argument("--mask", action="append", default=None, metavar="GLOB",
                   help="only include files whose basename matches this glob "
                        "(repeatable; e.g. '*.Q8_0.gguf')")
    p.add_argument("--redirector-host", default=REDIRECTOR_HOST,
                   help="redirector host for CNAME/N@ webseeds "
                        f"(default: {REDIRECTOR_HOST}; use 127.0.0.1:8083 "
                        "for local pump-api-v3 testing)")
    p.add_argument("--write-dir", default=None,
                   help="write downloaded files here after hashing "
                        "(default: OFF — no disk I/O; pass a path to enable)")
    args = p.parse_args()

    hasher_bin = args.hasher_bin
    if not Path(hasher_bin).exists():
        cprint(C.RED, f"[fail] hasher binary not found: {hasher_bin}")
        cprint(C.DIM, "  build it with:  cd hf-piece-hasher && go build .")
        return 2

    # --- parse repo ---
    url_rev = None
    url_subfolder = None
    try:
        if args.repo.startswith("http"):
            org, repo, url_rev, url_subfolder, _mask = parse_hf_url(args.repo)
        else:
            parts = [s for s in args.repo.split("/") if s]
            if len(parts) < 2:
                raise ValueError(f"expected org/repo, got: {args.repo}")
            org, repo = parts[0], parts[1]
    except ValueError as e:
        cprint(C.RED, f"[fail] {e}")
        return 2

    if url_subfolder:
        cprint(C.DIM, f"  URL subfolder filter: {url_subfolder}")

    cprint(C.BOLD + C.CYAN, f"\n  {org}/{repo}")
    cprint(C.DIM, f"  redirector: {args.redirector_host}")
    cprint(C.DIM, f"  hasher: {hasher_bin}  workers: {args.num_workers}  "
           f"chunk: {args.chunk_size // 1024} KiB")
    if args.no_manifest:
        cprint(C.YELLOW, "  [no-manifest] will NOT write to ./queue/ "
               "(orchestrator will not pick this up)")
    cprint(C.DIM, "  " + "-" * 50)

    # --- collect files at commit ---
    api = HfApi()
    repo_id = f"{org}/{repo}"
    revision = url_rev or "main"
    try:
        info = api.repo_info(repo_id=repo_id, revision=revision)
        main_commit = info.sha
        tags = getattr(info, "tags", []) or []
    except Exception as exc:
        cprint(C.RED, f"[fail] could not resolve repo: {exc}")
        return 2
    cprint(C.GREEN, f"  commit: {main_commit[:7]}")

    try:
        matched = collect_files(api, repo_id, main_commit,
                                subfolder=url_subfolder)
    except Exception as exc:
        cprint(C.RED, f"[fail] {exc}")
        return 2

    if not matched:
        cprint(C.RED, "[fail] no files found across any branch")
        return 1

    # --- apply --mask ---
    if args.mask:
        masks = args.mask
        before = len(matched)
        matched = [f for f in matched
                   if any(fnmatch.fnmatch(f.path.rsplit("/", 1)[-1], m)
                          for m in masks)]
        if not matched:
            cprint(C.RED, f"[fail] --mask {masks} matched 0 files "
                   f"(of {before} collected)")
            return 1
        cprint(C.DIM, f"  --mask: {len(matched)}/{before} files matched")

    cprint(C.YELLOW, f"\n  {len(matched)} file(s), "
           f"{_human_size(sum(f.size for f in matched))}")

    # --- classify (GENERAL decision tree, no hard-coded EXL3) ---
    meta = classify(org, repo, tags, info)
    base_model = meta.get("base_model")
    num_parameters = meta.get("num_parameters")

    has_gguf = any(f.path.lower().endswith(".gguf") for f in matched)

    if has_gguf:
        meta["file_class"] = "quant"
        meta["quant_type"] = "gguf"
        gguf_files = [f for f in matched if f.path.lower().endswith(".gguf")]
        tokens = set()
        for f in gguf_files:
            tok = extract_gguf_token(f.path.rsplit("/", 1)[-1])
            if tok:
                tokens.add(tok)
        header_tok = None
        if _GGUF_OK and gguf_files:
            peek_url = (f"https://huggingface.co/{org}/{repo}/resolve/"
                        f"{gguf_files[0].commit_sha}/{gguf_files[0].path}")
            gm = peek_gguf_header_http(peek_url)
            if gm:
                header_tok = gm.get("file_type") or gm.get("quant_name")
                cprint(C.DIM, f"  gguf header: arch={gm.get('architecture')} "
                       f"file_type={gm.get('file_type')} "
                       f"qname={gm.get('quant_name')}")
        if tokens:
            if len(tokens) == 1:
                meta["quant_detail"] = next(iter(tokens))
            else:
                meta["quant_detail"] = "many"
        elif header_tok:
            meta["quant_detail"] = header_tok
        else:
            meta["quant_detail"] = None
        meta["quant_bpw"] = (bpw_for_token(next(iter(tokens)))
                             if len(tokens) == 1 else None)
        meta["model_kind"] = "base"
        if not meta.get("quant_dev"):
            meta["quant_dev"] = org
        stripped = QUANT_NAME_STRIP.sub("", repo) or repo
        meta["model_name"] = stripped
        meta["lab"] = org
    else:
        hf_token = os.environ.get("HF_TOKEN") or None
        cprint(C.DIM, "  fetching config.json ...")
        bc = fetch_branch_config(api, repo_id, main_commit,
                                 hf_token=hf_token)
        if bc and bc.get("quant_method"):
            cprint(C.DIM, f"    {bc['quant_method']} bits={bc.get('bits')}")
        qtype, qdetail = aggregate_branch_configs([bc])
        if qtype:
            meta["file_class"] = "quant"
            meta["quant_type"] = qtype
            meta["quant_detail"] = qdetail
            bitses = {float(bc["bits"])} if bc and bc.get("bits") is not None else set()
            if len(bitses) == 1:
                meta["quant_bpw"] = next(iter(bitses))
            else:
                meta["quant_bpw"] = None
            meta["model_kind"] = "base"
            if not meta.get("quant_dev"):
                meta["quant_dev"] = org
            stripped = QUANT_NAME_STRIP.sub("", repo) or repo
            meta["model_name"] = stripped
            meta["lab"] = org
        else:
            if meta["quant_type"] in ("exl3", "exl2"):
                meta["quant_detail"] = meta["quant_type"].upper()
                meta["file_class"] = "quant"
                meta["model_kind"] = "base"
                stripped = QUANT_NAME_STRIP.sub("", repo) or repo
                meta["model_name"] = stripped
                meta["lab"] = org
                if not meta.get("quant_dev"):
                    meta["quant_dev"] = org

    meta["base_model"] = base_model
    meta["num_parameters"] = num_parameters

    display_name = make_display_name(
        meta["file_class"], meta["lab"], meta["model_name"],
        meta["quant_type"], meta["quant_dev"], meta["quant_detail"])

    cprint(C.BLUE, "\n  classification:")
    cprint(C.DIM, f"    class    : {meta['file_class']}")
    cprint(C.DIM, f"    kind     : {meta['model_kind']}")
    cprint(C.DIM, f"    quant    : {meta['quant_type']} ({meta['quant_detail']})")
    cprint(C.DIM, f"    lab      : {meta['lab']}")
    cprint(C.DIM, f"    model    : {meta['model_name']}")
    cprint(C.DIM, f"    display  : {display_name}")

    # --- sort matched (by commit, then path for deterministic ordering) ---
    matched.sort(key=lambda e: (e.commit_sha, e.path))
    total_size = sum((f.size or 0) for f in matched)
    pl = args.piece_length or pick_piece_length(total_size)
    pads = _bep5_padding_sizes(matched, pl)
    cprint(C.YELLOW, f"\n  {len(matched)} file(s), {_human_size(total_size)}, "
           f"piece length {pl // 1024} KiB")

    # --- stream + hash via Go binary ---
    hf_token = os.environ.get("HF_TOKEN") or None
    origin = "https://huggingface.co"

    write_dir = args.write_dir
    if write_dir:
        write_dir = str(Path(write_dir).resolve())
        cprint(C.DIM, f"  write: {write_dir}/{repo}/")

    try:
        pieces, err = run_go_hasher(
            matched, pads, pl, args.hasher_bin, args.num_workers,
            args.chunk_size, args.max_retries, hf_token, origin, org, repo,
            write_dir=write_dir)
    except KeyboardInterrupt:
        cprint(C.YELLOW, "\n  interrupted")
        return 130
    if err:
        cprint(C.RED, f"\n[fail] {err}")
        return 1

    cprint(C.GREEN, f"  {len(pieces) // 20} pieces, "
           f"{len(pieces)} bytes of piece hashes")

    # --- build .torrent ---
    files_list = []
    for i, f in enumerate(matched):
        segs = f.torrent_path_segments()
        files_list.append({b"length": f.size,
                           b"path": [s.encode() for s in segs]})
        pad_name, pad_size = pads[i]
        if pad_size > 0:
            files_list.append({b"length": pad_size,
                               b"path": [pad_name.encode()],
                               b"attr": b"p"})
    info_dict = {
        b"name": repo.encode(),
        b"piece length": pl,
        b"pieces": pieces,
        b"files": files_list,
    }
    infohash = hashlib.sha1(_bencode.encode(info_dict)).hexdigest()

    webseeds = all_webseeds_multibranch(org, repo, infohash, args.redirector_host)

    metainfo = {
        b"info": info_dict,
        b"announce": TRACKERS[0].encode(),
        b"announce-list": [[t.encode()] for t in TRACKERS],
        b"url-list": [w.encode() for w in webseeds],
        b"creation date": int(time.time()),
        b"comment": f"HF: {org}/{repo} commit={main_commit[:7]}".encode(),
        b"created by": b"maker-v6.py",
    }

    stem = f"{safe_slug(display_name)}-{main_commit[:7]}-{infohash[:7]}"
    if meta.get("quant_detail") and meta["quant_detail"] not in display_name:
        stem += f".{safe_slug(meta['quant_detail'])}"

    out_dir = Path(args.out).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    torrent_path = out_dir / f"{stem}.torrent"
    torrent_path.write_bytes(_bencode.encode(metainfo))

    if pads:
        _record_pad_sizes(infohash, pads)

    cprint(C.GREEN, f"\n  torrent: {torrent_path.name}")
    cprint(C.DIM, f"  hash: {infohash}")
    cprint(C.DIM, f"  pieces: {len(pieces) // 20}  webseeds: {len(webseeds)}")
    cprint(C.DIM, f"  name: {repo}  (info.name, no slash)")

    magnet = "&".join(
        [f"magnet:?xt=urn:btih:{infohash}", f"dn={quote(display_name)}",
         f"xl={total_size}"]
        + [f"tr={quote(t)}" for t in TRACKERS]
        + [f"ws={quote(w)}" for w in webseeds]
    )

    ts = int(time.time())
    manifest = {
        "ts": ts, "torrent_path": str(torrent_path.relative_to(Path.cwd())), "torrent_stem": stem,
        "infohash": infohash, "magnet": magnet, "name": repo,
        "torrent_name": repo,
        "total_size": total_size, "piece_length": pl,
        "piece_count": len(pieces) // 20, "webseeds": webseeds,
        "trackers": list(TRACKERS), "source": f"huggingface.co/{org}/{repo}",
        "display_name": display_name,
        "repo_id": f"{org}/{repo}",
        "lab": meta.get("lab"),
        "model_name": meta.get("model_name"),
        "version": "stream",
        "created_at": str(getattr(info, "created_at", "") or getattr(info, "createdAt", "")) or None,
        "file_class": meta.get("file_class"),
        "model_kind": meta.get("model_kind"),
        "quant_type": meta.get("quant_type"),
        "quant_dev": meta.get("quant_dev"),
        "quant_detail": meta.get("quant_detail"),
        "quant_bpw": meta.get("quant_bpw"),
        "base_model": meta.get("base_model"),
        "commit_sha": main_commit,
        "subfolder": url_subfolder,
        "pads": [{"filename": name, "size": size}
                 for name, size in pads if size > 0],
        "files": [
            {"path": f.path, "size": f.size, "index": i,
             "commit_sha": f.commit_sha}
            for i, f in enumerate(matched)
        ],
    }
    manifest = {k: v for k, v in manifest.items() if v}

    # --- materialize BEP-5 pad files so the seeder can verify to 100% ---
    if write_dir:
        pad_dir = Path(write_dir) / repo
        n_pads = 0
        for name, size in pads:
            if size <= 0:
                continue
            pad_path = pad_dir / name
            if pad_path.exists() and pad_path.stat().st_size == size:
                continue
            pad_path.parent.mkdir(parents=True, exist_ok=True)
            with open(pad_path, "wb") as pf:
                pf.truncate(size)
            n_pads += 1
        if n_pads:
            cprint(C.DIM, f"  wrote {n_pads} BEP-5 .pad file(s)")
        manifest["local_download_dir"] = str(Path(write_dir).resolve())

    qp = None
    if not args.no_manifest:
        queue_dir = Path(args.queue_dir).resolve()
        queue_dir.mkdir(parents=True, exist_ok=True)
        qp = queue_dir / f"{stem}.{ts}.json"
        tmp_qp = qp.with_suffix(".json.tmp")
        with open(tmp_qp, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2, ensure_ascii=False)
            f.write("\n")
        os.replace(str(tmp_qp), str(qp))
        cprint(C.GREEN, f"  manifest: {qp.name}")
    else:
        cprint(C.DIM, "  [no-manifest] skipped manifest write")

    cprint(C.GREEN, f"\n  {C.BOLD}done{C.R}  .torrent ready")
    if qp:
        cprint(C.DIM, f"  post:  python post-torr-nostr-j.py {qp}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        cprint(C.YELLOW, "\n  interrupted")
        sys.exit(130)

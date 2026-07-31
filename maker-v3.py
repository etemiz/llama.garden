#!/usr/bin/env python3
"""maker-v3.py — disk-based multi-branch HuggingFace torrent builder
=====================================================================

WHAT THIS IS
------------
llama.garden (https://llama.garden) is a torrent index of language models.
Models are mirrored from HuggingFace into BitTorrent swarms so they can be
fetched peer-to-peer, cached by seeders, and survive link rot. This script
turns one HuggingFace repo into one .torrent file that anyone can publish on
the Nostr relay network (kind 30099 listing events, see
`post-torr-nostr-j.py`) and that the `waifu-magnet` viewer renders as cards.

maker-v3 is the DISK-BASED builder: it downloads every selected file to a
local temp directory, then hashes them in place to compute BitTorrent v1
piece SHA1s. Use it when you want a local copy of the files anyway (e.g. you
will seed from the same machine via Transmission). For hash-only builds with
no disk I/O, use `maker-v6.py` instead (it streams bytes straight into a Go
hasher and writes only the .torrent).

This is the STANDALONE multi-branch builder. It is designed for repos like
`turboderp/Qwen3.6-27B-exl3` that publish multiple quantizations as BRANCHES
of a single repo. All selected branches are bundled into ONE torrent so
the swarm is unified across quant levels.

PREREQUISITES
-------------
  - Python 3.10+ with: huggingface_hub, torf, tqdm, gguf (optional, for
    .gguf repos), transrpc (the local module in this repo).
  - A HuggingFace read token if any branch is gated. Export it:
        export HF_TOKEN=hf_xxx
  - Optional: a local Transmission daemon if you want v3 to add the finished
    torrent to your seed client automatically:
        export MUSCLE_HOST=127.0.0.1
        export MUSCLE_USER=muscle
        export MUSCLE_PASS=...
        export MUSCLE_PORT=9091
    (All MUSCLE_* vars come from the environment; none are hardcoded.)

QUICK START
-----------
  # 1. Build a multi-branch exl3 repo into one torrent + seed it locally:
  export HF_TOKEN=hf_xxx MUSCLE_USER=muscle MUSCLE_PASS=...
  python maker-v3.py turboderp/Qwen3.6-27B-exl3 --yes --delete --no-manifest

  # 2. Build just one branch (e.g. a single-branch GGUF repo):
  python maker-v3.py TheBloke/TinyLlama-1.1B-Chat-v1.0-GGUF --branch main

  # 3. Point the redirector at your own pump-api instance instead of the
  #    production api.llama.garden redirector:
  python maker-v3.py org/repo --redirector-host 127.0.0.1:8083

  # 4. Publish the finished torrent to llama.garden's Nostr index:
  python post-torr-nostr-j.py <repo>            # reads ./queue/ manifest

After step 1 the .torrent and manifest are written to ./queue/ and (if
MUSCLE_* is set) added to Transmission. `--delete` removes the temp files
once hashing succeeds; `--no-manifest` skips writing the manifest (use it
when you are not going to publish to Nostr).

TORRENT LAYOUT (the key difference from the single-branch builder)
------------------------------------------------------------------
  info.name = <repo>              (e.g. "Qwen3.6-27B-exl3", no slash, no org)
  file path  = ["resolve", <commit_sha>, ...original_file_segments]

libtorrent appends <name>/<path> to trailing-slash webseeds:
  HF direct:   https://huggingface.co/<org>/  + <repo>/resolve/<commit>/<file>
               = https://huggingface.co/<org>/<repo>/resolve/<commit>/<file>  OK
  CNAME redir: http://z<i>.api.llama.garden/<org>/<repo>/<ih>/  + <repo>/resolve/<commit>/<file>
               = http://z<i>.api.llama.garden/<org>/<repo>/<ih>/<repo>/resolve/<commit>/<file>  OK
  N@ redir:    http://<i>@api.llama.garden/<org>/<repo>/<ih>/  + same
               = same  OK

pump-api-v3.py strips the "<repo>/resolve/" prefix from `rest` before
building the HF /resolve/ URL (backward-compatible: v2 torrents don't
have the prefix, so the strip is a no-op for them).

On-disk layout (both Transmission + qBittorrent, name has no "/"):
  <save_dir>/<repo>/resolve/<commit_sha>/<file>

WEBSEEDS (7 total: 1 HF direct + 2 CNAME redirector + 4 N@ userinfo)
  - 1 HF direct (https://huggingface.co/<org>/) — Transmission only
    (qBit 403s on HF 302->CDN Range drop, falls back to CNAME).
  - 2 CNAME (z1,z2) — both clients, qBit's primary.
  - 4 N@ (1@..4@) — Transmission only (qBit NXDOMAIN, ignored).
  The /api/resolve-cache/ webseed from v2 is dropped (its URL shape
  doesn't compose with name=<repo> + path=["resolve", commit, file]).

PIPELINE
--------
  1. Parse repo id (org/repo or full HF URL).
  2. Enumerate ALL branches via HfApi().list_repo_refs().
  3. For each branch: resolve commit SHA, list files, collect into one
     matched list. Dedup by (commit_sha, path) — branches sharing a
     commit include files once.
  4. Classify: GENERAL (no hard-coded EXL3). Detection order:
       a. GGUF present  -> quant_type="gguf", quant_detail from filename
          tokens (mixed -> "many"), cross-checked against GGUF header
          metadata (general.file_type / general.quantization_name).
       b. config.json across selected branches -> quant_type / quant_detail
          from quantization_config.quant_method. Different quant levels
          across branches -> quant_detail="many". Different families ->
          quant_type="mixed", quant_detail="many".
       c. Repo-name / HF-tag heuristics (classify_repo) -> fallback for
          repos with neither GGUF nor config.json quant info (catches
          -exl3, -mlx, -awq, -gptq, -fp8, -nvfp4, -mxfp4, -bnb, -onnx).
       d. None of the above -> quant_type=None, file_class from
          classify_repo (base / fine tune).
  5. Download (--downloader urllib uses raw streamer with Range resume;
     --downloader hf uses hf_hub_download). Per-file URL uses that file's
     branch commit. Files saved to tmp_dir/<repo>/resolve/<commit>/<path>.
  6. Hash BT v1 pieces in file order, verify each against HF checksum.
  7. Write .torrent + manifest, optionally add to local Transmission.

STANDALONE
----------
All helpers (colors, trackers, hashing, padding, download, transmission add,
classification) are defined inline here. The only runtime imports are
huggingface_hub, torf, tqdm, gguf, transrpc.

SEE ALSO
--------
  - maker-v6.py  : streaming hasher (no disk). Preferred for hash-only builds.
  - maker-v5.py  : sharded disk builder (same infohash across N machines).
  - post-torr-nostr-j.py : publish the .torrent + manifest to Nostr (kind 30099).
  - approve-submissions.py : review other people's kind 30099 submissions.
  - wip.md / map.md / docs/maker-v6-remote-build.md : full guides.
"""

import argparse
import base64  # noqa: E402 (used by add_to_local_transmission)
import fnmatch
import hashlib
import json
import multiprocessing
import os
import queue
import re
import shutil
import sys
import threading
import time
import urllib.request
import urllib.error
from pathlib import Path
from urllib.parse import quote, urlparse

# Must be set before huggingface_hub is imported: it reads this constant at
# import time into constants.HF_HUB_DISABLE_PROGRESS_BARS. Setting it in
# main() is too late and the per-file tqdm bars still render.
os.environ.setdefault("HF_HUB_DISABLE_PROGRESS_BARS", "1")

from tqdm.auto import tqdm as _tqdmBase

from huggingface_hub import HfApi, hf_hub_download
from huggingface_hub.hf_api import RepoFile
from torf import _flatbencode as _bencode

# gguf is optional — only needed when the repo contains .gguf files. If
# the import fails, GGUF header cross-check is skipped (filename tokens
# remain the primary source, so classification still works).
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


def _file_log(color, msg):
    """Log a per-file event (download / hash / verify) on its own row.

    The progress thread's `prog_loop` uses `\\r\\x1b[K` to redraw the
    status line in place (cursor ends at end-of-status). If we used a
    plain `cprint` here, the per-file message would be appended to
    that status line and the terminal would render them concatenated
    on the same row (e.g. `dl: 76% ...  |  hash: ...  [18/18]
    downloading: tokenizer.json`).

    Prepending `\\n` lands the message on a fresh row below the
    status. The next status tick (every 5s) will use `\\r\\x1b[K` to
    clear whatever line the cursor is on and redraw the status, so the
    status remains at the bottom of the terminal.

    Trade-off: if multiple per-file events fire in rapid succession
    within one 5s tick, each `\\n` adds a blank row between the old
    status and the next. That's accepted as the cost of separating
    log from status in a single-process terminal model without ANSI
    save/restore.
    """
    cprint(color, f"\n  {msg}")


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

GB = 1024 ** 3
CHUNK = 256 * 1024

REDIRECTOR_HOST = "api.llama.garden"
REDIRECTOR_CNAME_PREFIX = "z"  # z1..zN.api.llama.garden
# Number of CNAME redirector webseeds. 2 is the sweet spot: enough
# parallelism for ~20 MB/s in qBit, few enough to stay under the CDN
# per-IP rate limit. 8 (the old value) triggers 403 stalls after ~10s.
REDIRECTOR_CNAME_COUNT = 2
# Number of N@ userinfo redirector webseeds for Transmission-only. These
# use the bare api.llama.garden host with a userinfo prefix (1@, 2@, ...).
# Transmission resolves api.llama.garden fine and treats each N@ as a
# distinct connection (4 extra parallel pipes -> faster downloads).
# libtorrent 2.0 (qBittorrent) CANNOT resolve N@api.llama.garden
# (NXDOMAIN on the userinfo-prefixed authority) -> qBit silently ignores
# them, no rate-limit impact. This is the hybrid: qBit uses the 2 CNAMEs
# (rate-limit-safe), Transmission uses 2 CNAMEs + 4 N@ = 6 redirector
# pipes + 1 HF direct = 7 total.
REDIRECTOR_USERINFO_COUNT = 4

_PAD_SIZES_PATH = "pad-sizes.json"


# ---------------------------------------------------------------------------
# CLASSIFICATION (ported from url-to-torr-combo-v2.py / hf-trends-to-jobs.py)
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

# Map quantization_config.quant_method values (as seen in HF config.json)
# to our canonical quant_type strings. Keys are matched case-insensitively
# after lowercasing. Missing entries fall through to the raw lowercased
# value (so new methods still produce a sensible quant_type).
QUANT_METHOD_MAP = {
    "awq": "awq",
    "gptq": "gptq",
    "bitsandbytes": "bnb",
    "bnb": "bnb",
    "fp8": "fp8",
    "exl2": "exl2",
    "exl3": "exl3",
    "gguf": "gguf",
    "mlx": "mlx",
    "onnx": "onnx",
    "nvfp4": "nvfp4",
    "mxfp4": "mxfp4",
}

# Map GGUF general.file_type enum values (LlamaFileType) to the
# canonical GGUF quant token used in filenames. Built once at import
# time when the gguf package is available.
_GGUF_FILETYPE_TOKENS = {}
if _GGUF_OK:
    for _t in LlamaFileType:
        # e.g. MOSTLY_Q8_0 -> Q8_0, MOSTLY_BF16 -> BF16, MOSTLY_IQ4_XS -> IQ4_XS
        _name = _t.name
        if _name.startswith("MOSTLY_"):
            _tok = _name[len("MOSTLY_"):]
            _GGUF_FILETYPE_TOKENS[_t.value] = _tok

# Newer quant types not yet in the installed gguf package's LlamaFileType
# enum (added in later GGUF spec revisions). These are the mixed-precision
# Q4_0 block variants and others — without these the file_type→token
# lookup returns None for GGUFs using them (e.g. bartowski Q4_0_4_4).
_GGUF_FILETYPE_TOKENS.update({
    33: "Q4_0_4_4",
    34: "Q4_0_4_8",
    35: "Q4_0_8_8",
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
    # GGUF naming convention: the real quant token is the LAST one before
    # .gguf. A filename can contain several — e.g.
    #   Huihui-DeepSeek-V4-Flash-BF16-abliterated-ds4-IQ2_XXS.gguf
    # has both "BF16" (the base model's format, part of the model name) and
    # "IQ2_XXS" (the actual quantization). .search() returns the first match
    # (BF16) which is wrong. finditer + last picks the appended quant token.
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
        # "many" (mixed quants) isn't a specific detail — omit it.
        if quant_detail and quant_detail != "many":
            term = quant_detail
        else:
            term = quant_type
        if quant_detail == "many":
            # mixed-quant bundle: no single detail to show
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
    """Full classification from HF tags + repo name. Returns a dict with
    all metadata fields. quant_type may be None (base/fine-tune)."""
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

    # model_kind: probe base repo if quant
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

    # quant_detail + bpw
    quant_detail = None
    quant_bpw = None
    if quant_type == "gguf":
        # handled by caller after file matching (token from filename)
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
# GGUF header metadata reader (uses the `gguf` package if available)
# ---------------------------------------------------------------------------

def read_gguf_meta(source):
    """Parse a LOCAL GGUF file's header metadata and return a dict of
    useful `general.*` fields, or None on any failure.

    `source` is a `pathlib.Path` / str path to a local .gguf file. Uses
    the `gguf` package's GGUFReader (mmap-based), which requires the
    FULL file — it eagerly parses all header fields including tokenizer
    arrays, so it CANNOT read from a truncated buffer / HTTP Range.
    For pre-download peeks use `peek_gguf_header_http` which has its own
    minimal scalar-field walker that skips arrays by stride.

    Returns:
      {
        "architecture": str | None,        # general.architecture
        "name":          str | None,        # general.name
        "file_type":     str | None,        # general.file_type -> token (Q8_0, BF16, ...)
        "file_type_raw": int | None,        # raw enum value
        "quant_name":    str | None,        # general.quantization_name (newer GGUFs)
        "quant_version": int | None,        # general.quantization_version
      }
    or None on parse failure / missing fields.
    """
    if not _GGUF_OK:
        return None
    try:
        r = GGUFReader(str(source))

        def _str(key):
            f = r.get_field(key)
            if f is None:
                return None
            try:
                v = f.contents()
                if isinstance(v, str):
                    return v
                if isinstance(v, bytes):
                    return v.decode("utf-8", "replace")
                # numpy bytes / array of uint8
                return bytes(v).decode("utf-8", "replace")
            except Exception:
                return None

        def _int(key):
            f = r.get_field(key)
            if f is None:
                return None
            try:
                v = f.contents()
                if isinstance(v, int):
                    return v
                # numpy scalar
                return int(v)
            except Exception:
                return None

        arch = _str("general.architecture")
        name = _str("general.name")
        qname = _str("general.quantization_name")
        qver = _int("general.quantization_version")
        ft_raw = _int("general.file_type")
        ft_token = _GGUF_FILETYPE_TOKENS.get(ft_raw) if ft_raw is not None else None

        return {
            "architecture": arch,
            "name": name,
            "file_type": ft_token,
            "file_type_raw": ft_raw,
            "quant_name": qname,
            "quant_version": qver,
        }
    except Exception:
        return None


# Element byte sizes for the GGUF scalar value types (used by the
# minimal walker below to skip array bodies). STRING (8) is variable
# so it is handled specially in the walker.
_GGUF_SCALAR_SIZES = {0: 1, 1: 1, 2: 2, 3: 2, 4: 4, 5: 4, 6: 4, 7: 1,
                      10: 8, 11: 8, 12: 8}


def _gguf_scalar_fields(buf):
    """Minimal GGUF header walker that reads ONLY the general.* scalar
    fields we care about (architecture, name, file_type,
    quantization_name, quantization_version) from a byte buffer and
    SKIPS everything else (arrays, non-general fields) by computing
    their byte span. Returns the same dict shape as read_gguf_meta, or
    None if the buffer isn't a valid GGUF.

    Why this exists: the `gguf` package's GGUFReader eagerly parses all
    header fields (including the tokenizer token/score/type arrays,
    which can be tens of MiB) and crashes on a truncated buffer. For a
    pre-download HTTP Range peek we only have a few MiB, so we walk the
    KV pairs ourselves, read the scalars we want, and skip arrays by
    computing their element strides — never touching the array bodies.
    Stops early once all 5 target fields are found. Gracefully returns
    a partial dict (with None for unfound fields) if the buffer runs
    out mid-header."""
    import struct
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

        if key in want and vtype != 9:  # 9 = ARRAY -> skip path
            try:
                if vtype == 8:  # STRING
                    val, off = _read_str(off)
                elif vtype == 4:  # UINT32
                    val, = struct.unpack_from("<I", buf, off); off += 4
                elif vtype == 5:  # INT32
                    val, = struct.unpack_from("<i", buf, off); off += 4
                elif vtype == 10:  # UINT64
                    val, = struct.unpack_from("<Q", buf, off); off += 8
                elif vtype == 11:  # INT64
                    val, = struct.unpack_from("<q", buf, off); off += 8
                elif vtype == 6:  # FLOAT32
                    val, = struct.unpack_from("<f", buf, off); off += 4
                elif vtype == 12:  # FLOAT64
                    val, = struct.unpack_from("<d", buf, off); off += 8
                elif vtype == 0:  # UINT8
                    val, = struct.unpack_from("<B", buf, off); off += 1
                elif vtype == 1:  # INT8
                    val, = struct.unpack_from("<b", buf, off); off += 1
                elif vtype == 7:  # BOOL
                    val, = struct.unpack_from("<B", buf, off); off += 1
                elif vtype == 2:  # UINT16
                    val, = struct.unpack_from("<H", buf, off); off += 2
                elif vtype == 3:  # INT16
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
            # Skip this value's bytes. For arrays: elem_type(u32) +
            # count(u64) + count*stride. For scalars: fixed stride
            # (STRING = u64 len + len bytes).
            try:
                if vtype == 9:  # ARRAY
                    (etype,) = struct.unpack_from("<I", buf, off); off += 4
                    (acount,) = struct.unpack_from("<Q", buf, off); off += 8
                    if etype == 8:  # array of strings: walk each
                        for _ in range(acount):
                            _s, off = _read_str(off)
                    else:
                        off += acount * _GGUF_SCALAR_SIZES.get(etype, 0)
                elif vtype == 8:  # STRING scalar
                    _s, off = _read_str(off)
                else:
                    off += _GGUF_SCALAR_SIZES.get(vtype, 0)
            except struct.error:
                break
        if off > n:
            break

    return out


def peek_gguf_header_http(url, timeout=20):
    """Fetch the first 2 MiB of a GGUF file via HTTP Range and parse
    only the general.* scalar header fields with the minimal walker
    (_gguf_scalar_fields). Returns that dict or None on any failure
    (404, 416, timeout, not-GGUF magic, gguf package missing).

    2 MiB covers the general.* scalars for virtually all GGUFs — they
    are emitted before the tokenizer arrays in the header. The tokenizer
    token/score/type arrays can be tens of MiB and are SKIPPED by the
    walker (we compute their stride without touching their bodies), so
    truncation past 2 MiB never crashes us. Used for pre-download
    classification display; the post-download read_gguf_meta(local path)
    via the gguf package is the authoritative cross-check."""
    if not _GGUF_OK:
        return None
    try:
        req = urllib.request.Request(
            url, headers={"User-Agent": "maker-v3/1.0",
                          "Range": "bytes=0-2097151"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read(2097152)
        if not body or body[:4] != b"GGUF":
            return None
        return _gguf_scalar_fields(body)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# config.json reader (per-branch fetch + aggregation)
# ---------------------------------------------------------------------------

def fetch_branch_config(api, repo_id, commit_sha, hf_token=None):
    """Fetch config.json at a specific commit and return a dict of
    classification-relevant fields, or None on any failure (missing,
    non-JSON, network error). Silent — the caller aggregates and falls
    back to heuristics when no branch yields a config.

    Returns:
      {
        "quant_method": str | None,   # quantization_config.quant_method
        "bits":         float | None,  # quantization_config.bits
        "model_type":   str | None,    # model_type
        "architectures": [str] | None, # architectures[0] used for display
      }
    or None.
    """
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
    # Only return non-None if we found at least one useful field; an empty
    # config.json with no quant info and no model_type is uninteresting.
    if not any(v is not None for v in out.values()):
        return None
    return out


def aggregate_branch_configs(branch_configs):
    """Aggregate per-branch config.json dicts (from fetch_branch_config)
    into (quant_type, quant_detail) following the agreed convention:

      - All branches share the same quant_method AND same bits ->
        quant_type=<canonical>, quant_detail=<bits-or-variant>.
      - Same quant_method, different bits (e.g. EXL3 at 2.0/2.5/3.0 bpw)
        -> quant_type=<canonical>, quant_detail="many".
      - Different quant_method entirely (mixed families) ->
        quant_type="mixed", quant_detail="many".
      - No usable configs -> (None, None).

    `quant_method` strings are mapped through QUANT_METHOD_MAP to our
    canonical quant_type strings (awq/gptq/bnb/fp8/exl3/...).

    `bits` can be a float (exl3 publishes 2.5 meaning 2.5 bpw) or an int
    (awq/gptq typically 4). When all branches agree on a single bits
    value, we pick a detail string: for exl3 it's "{bits}bpw" when
    there's a single branch, else "many"; for awq/gptq/bnb it's
    "{bits}bit"; for others just str(bits).
    """
    # Filter to branches that actually had a quant_method
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
            # Normalize so 4 (int) and 4.0 (float) compare equal
            bitses.add(float(bits))

    if not methods:
        return None, None

    if len(methods) == 1:
        raw = next(iter(methods))
        qtype = QUANT_METHOD_MAP.get(raw, raw)
        if len(bitses) <= 1:
            # Single bits value (or no bits at all) — derive a detail.
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
        # Same family, multiple bit levels -> "many"
        return qtype, "many"

    # Multiple different quant_method values across branches
    return "mixed", "many"


# ---------------------------------------------------------------------------
# HF URL parsing
# ---------------------------------------------------------------------------

def parse_hf_url(url):
    """Parse a HF URL into (org, repo, revision, subfolder, file_mask).

    Supports three URL shapes:
      /org/repo                       -> whole repo, rev=main
      /org/repo/tree/<rev>[/<subdir>] -> subdir (or whole repo if no subdir),
                                         rev=<rev>
      /org/repo/blob/<rev>/<path>     -> single file at <path>, rev=<rev>
                                         (file_mask = basename of <path>;
                                          subfolder = dirname so match_files
                                          can locate it)
    Returns file_mask=None for the non-blob cases (match everything).
    """
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
        # /org/repo/blob/<rev>/<file-path...> -> single file
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


class DiskFullError(RuntimeError):
    """Raised when free space on the download filesystem drops below the
    configured threshold (--min-free-gb). Distinct from RuntimeError so
    main() can clean up partial downloads and exit 1."""
    pass


def check_free_space(path, min_bytes, label):
    """Raise DiskFullError if free space on `path`'s filesystem is below
    min_bytes. No-op if min_bytes is falsy. OSError on disk_usage is
    swallowed so a transient stat failure doesn't abort a download."""
    if not min_bytes:
        return
    try:
        usage = shutil.disk_usage(str(path))
    except OSError:
        return
    if usage.free < min_bytes:
        raise DiskFullError(
            f"insufficient disk space for {label}: "
            f"{_human_size(usage.free)} free < {_human_size(min_bytes)} "
            f"threshold (on {path})")


# ---------------------------------------------------------------------------
# Hash worker functions — must be module-level (not nested) because
# multiprocessing.Pool uses pickle to send them to worker processes.
# ---------------------------------------------------------------------------

_HASH_PROGRESS = None


def _init_hash_worker(progress_arr):
    global _HASH_PROGRESS
    _HASH_PROGRESS = progress_arr


def _hash_file_worker(path_str, file_size, piece_length, pad_size,
                      lfs_sha256, git_blob_id, idx, is_gguf=False):
    try:
        verify = None
        expected = None
        if lfs_sha256:
            verify = hashlib.sha256()
            expected = lfs_sha256
        elif git_blob_id:
            verify = hashlib.sha1()
            verify.update(f"blob {file_size}\0".encode())
            expected = git_blob_id

        pieces = bytearray()
        buf = bytearray()
        hashed = 0
        # GGUF header capture: collect the first 2 MiB of the file as we
        # read it for hashing, then parse the general.* scalar fields.
        # This avoids a second file open after hashing (the file may be
        # unlinked by the success callback before the main thread can
        # re-read it). The header is in the first few chunks we read
        # anyway, so this is essentially free.
        gguf_meta = None
        head_buf = bytearray() if is_gguf else None

        with open(path_str, "rb") as f:
            while True:
                chunk = f.read(CHUNK)
                if not chunk:
                    break
                buf += chunk
                if head_buf is not None and gguf_meta is None:
                    head_buf.extend(chunk)
                    if len(head_buf) >= 2 * 1024 * 1024:
                        gguf_meta = _gguf_scalar_fields(
                            bytes(head_buf[:2 * 1024 * 1024]))
                if verify:
                    verify.update(chunk)
                hashed += len(chunk)
                if _HASH_PROGRESS is not None:
                    _HASH_PROGRESS[idx] = hashed
                while len(buf) >= piece_length:
                    pieces += hashlib.sha1(bytes(buf[:piece_length])).digest()
                    del buf[:piece_length]

        # If the file is smaller than 2 MiB, parse whatever we got.
        if head_buf is not None and gguf_meta is None and head_buf:
            gguf_meta = _gguf_scalar_fields(bytes(head_buf))

        if pad_size > 0:
            buf.extend(b"\0" * pad_size)
            while len(buf) >= piece_length:
                pieces += hashlib.sha1(bytes(buf[:piece_length])).digest()
                del buf[:piece_length]

        if buf:
            pieces += hashlib.sha1(bytes(buf)).digest()

        verified = True
        if verify and expected:
            verified = verify.hexdigest() == expected

        if _HASH_PROGRESS is not None:
            _HASH_PROGRESS[idx] = hashed + pad_size

        return bytes(pieces), verified, hashed, None, gguf_meta
    except Exception as e:
        return b"", False, 0, str(e), None


def _start_hash_aggregator(hash_progress, hash_counter, stop_event):
    """Daemon thread that sums `hash_progress` (per-file bytes hashed by
    the multiprocessing pool) into `hash_counter[0]` so the outer progress
    display sees per-file hash progress live, instead of jumping in chunks
    when the main loop's `.get()` returns. Polls every 0.25s and applies
    only the delta since the last poll (no double-counting). Final flush
    after stop_event is set."""
    last = [0]

    def _loop():
        while True:
            cur = sum(hash_progress)
            delta = cur - last[0]
            if delta:
                hash_counter[0] += delta
                last[0] = cur
            if stop_event.wait(0.25):
                break
        cur = sum(hash_progress)
        delta = cur - last[0]
        if delta:
            hash_counter[0] += delta

    t = threading.Thread(target=_loop, daemon=True)
    t.start()
    return t


def _unlink_on_success_cb(path, keep_files):
    """Build a `multiprocessing.pool.AsyncResult` callback that unlinks
    `path` the moment the worker returns SUCCESS, so disk space is freed
    as each file finishes hashing — not after the collect loop catches
    up to it in matched order. No-op on error, verified=False, or
    keep_files=True."""
    def _cb(result):
        _piece_hashes, verified, _hashed, error, _gguf = result
        if error or not verified or keep_files:
            return
        try:
            Path(path).unlink(missing_ok=True)
        except Exception:
            pass
    return _cb


# ---------------------------------------------------------------------------
# Download primitives
# ---------------------------------------------------------------------------

def download_file_urllib(url, dst, expected_size, dl_counter, stop=None):
    """urllib streamer with HTTP Range resume, chunk-level stop checks,
    and a 90s per-recv socket timeout. Used by --downloader urllib."""
    have = dst.stat().st_size if dst.exists() else 0
    if have > expected_size:
        dst.unlink()
        have = 0
    headers = {"User-Agent": "maker-v3/1.0"}
    mode = "wb"
    if have:
        headers["Range"] = f"bytes={have}-"
        mode = "ab"
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=90) as r:
        if have and r.status == 200:
            # server ignored Range: restart from scratch
            have = 0
            mode = "wb"
        with open(dst, mode) as f:
            while True:
                if stop is not None and stop.is_set():
                    raise RuntimeError("aborted by stop event "
                                       "(another file failed)")
                chunk = r.read(CHUNK)
                if not chunk:
                    break
                f.write(chunk)
                dl_counter[0] += len(chunk)
    got = dst.stat().st_size
    if got != expected_size:
        raise RuntimeError(f"{url}: got {got} bytes, expected {expected_size}")


# Silent tqdm subclass factory: hf_hub_download instantiates one bar per
# file. We disable the visual bar and route byte increments into dl_counter.
def _make_counter_bar(dl_counter):
    class _CounterBar(_tqdmBase):
        def __init__(self, *a, **k):
            k["disable"] = True
            super().__init__(*a, **k)

        def update(self, n=1):
            dl_counter[0] += int(n)
            return super().update(n)
    return _CounterBar


# ---------------------------------------------------------------------------
# BEP-5 padding
# ---------------------------------------------------------------------------

def _bep5_padding_sizes(matched, piece_length):
    """BEP-5 padding: list of (filename, size) for each .pad file needed.

    Returns exactly ``len(matched)`` entries — one per file, positionally
    indexed by the file's position in ``matched``. A pad whose size is 0
    means that file already ends on a piece boundary (no pad file is
    emitted for it in the .torrent).

    For the i-th file, the pad fills the gap between the end of file i
    and the next piece boundary. A .pad is emitted in the .torrent only
    when its size > 0.

    Why this matters: libtorrent never fetches a piece that spans two
    real files. The spanning piece is requested per-file via Range GETs,
    and the "other file" part is never fetched -> permanent stall. Adding
    a BEP-5 .pad file (attr=p, zero-filled) at every unaligned boundary
    ensures every piece sits entirely within one real file (or a .pad).
    """
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
    """Merge {infohash: {pad_filename: size, ...}} into pad-sizes.json
    atomically. pump-api-v3 reads this to serve each BEP-5 .pad file as
    zero bytes to Transmission (which does not honor attr=p)."""
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
    """A file in a multi-branch torrent.

    Attributes:
        path         — repo-relative path (e.g. "model-00001-of-00002.safetensors")
        size         — file size in bytes
        lfs_sha256   — LFS sha256 hex (None for git blobs)
        blob_id      — git blob sha1 hex (None for LFS files)
        commit_sha   — commit SHA of the branch this file belongs to
        branch       — branch name (e.g. "2.50bpw", "main")
        _auto_pad    — set by _ensure_two_files (unused for multi-branch,
                       always >=2 files, but kept for sort compatibility)

    The torrent file path for this file is:
        ["resolve", commit_sha, ...path.split("/")]
    """
    __slots__ = ("path", "size", "lfs_sha256", "blob_id", "commit_sha",
                 "branch", "_auto_pad")

    def __init__(self, path, size, lfs_sha256=None, blob_id=None,
                 commit_sha=None, branch=None):
        self.path = path
        self.size = size
        self.lfs_sha256 = lfs_sha256
        self.blob_id = blob_id
        self.commit_sha = commit_sha
        self.branch = branch
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
        return (f"MBFile({self.branch}:{self.path} "
                f"{self.size} LFS={self.lfs_sha256 is not None})")


# ---------------------------------------------------------------------------
# Branch enumeration + file listing
# ---------------------------------------------------------------------------

def enumerate_branches(api, repo_id):
    """Return [(branch_name, commit_sha), ...] for ALL branches of a repo.

    Includes 'main' and every other branch. Excludes tags."""
    refs = api.list_repo_refs(repo_id=repo_id)
    branches = []
    for b in (refs.branches or []):
        name = b.name
        commit = getattr(b, "ref_commit_id", None) or getattr(
            b, "target_commit_oid", None)
        if not commit:
            info = api.repo_info(repo_id=repo_id, revision=name)
            commit = info.sha
        branches.append((name, commit))
    branches.sort(key=lambda x: (x[0] != "main", x[0]))
    return branches


def list_branch_files(api, repo_id, branch, commit_sha, subfolder=None):
    """List all RepoFile entries at a given branch/commit. If subfolder
    is set, only files whose path starts with that prefix are included."""
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
            commit_sha=commit_sha, branch=branch))
    return out


def collect_all_branch_files(api, repo_id, branch_filter=None, subfolder=None):
    """Enumerate all branches and collect files into one deduplicated list.

    Dedup by (commit_sha, path): if two branches point to the same commit,
    their files are identical and included only once.

    Returns (matched: [MBFile, ...], branches: [{name, commit_sha,
    file_count, total_size}, ...])."""
    branch_list = enumerate_branches(api, repo_id)
    if branch_filter:
        branch_list = [(n, c) for n, c in branch_list if n in branch_filter]
    if not branch_list:
        raise RuntimeError(f"no branches found for {repo_id}")

    cprint(C.CYAN, f"\n  {len(branch_list)} branch(es):")
    matched = []
    seen = set()
    branches_meta = []

    for branch_name, commit_sha in branch_list:
        files = list_branch_files(api, repo_id, branch_name, commit_sha,
                                  subfolder=subfolder)
        new_files = []
        for f in files:
            key = (f.commit_sha, f.path)
            if key in seen:
                continue
            seen.add(key)
            new_files.append(f)

        matched.extend(new_files)
        total_size = sum(f.size for f in new_files)
        branches_meta.append({
            "name": branch_name, "commit_sha": commit_sha,
            "file_count": len(new_files), "total_size": total_size,
        })
        cprint(C.DIM, f"    {branch_name:12s}  {commit_sha[:7]}  "
               f"{len(new_files):4d} files  {_human_size(total_size)}")

    return matched, branches_meta


# ---------------------------------------------------------------------------
# Download + hash (multi-branch variant)
# ---------------------------------------------------------------------------

def compute_pieces_multibranch(matched, mode, origin, org, repo,
                               piece_length, workers, max_retries, tmp_dir,
                               dl_counter, hash_counter, min_free_bytes,
                               keep_files=False, hf_token=None,
                               hash_workers=3):
    """Download + hash + verify all matched files across multiple branches.

    Each file is downloaded from:
        {origin}{org}/{repo}/resolve/{file.commit_sha}/{file.path}
    and saved to:
        {tmp_dir}/{repo}/resolve/{file.commit_sha}/{file.path}

    Hashing uses the same multiprocessing pool + BEP-5 padding as the
    single-branch builder. The hash worker is branch-agnostic.
    """
    n = len(matched)
    repo_id = f"{org}/{repo}"

    file_base = tmp_dir / repo / "resolve"
    file_base.mkdir(parents=True, exist_ok=True)

    tmp_paths = []
    for e in matched:
        p = file_base / e.commit_sha / e.path
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp_paths.append(p)

    errors = [None] * n
    events = [threading.Event() for _ in range(n)]
    final_paths = [None] * n
    stop = threading.Event()
    done_q = queue.Queue()
    _CounterBar = _make_counter_bar(dl_counter)

    def dl_urllib(i):
        e = matched[i]
        url = origin + f"{org}/{repo}/resolve/{e.commit_sha}/{e.path}"
        _file_log(C.GRAY, f"[{i+1}/{n}] downloading: {e.branch}:{e.path}")
        try:
            for attempt in range(1, max_retries + 1):
                if stop.is_set():
                    return
                try:
                    download_file_urllib(url, tmp_paths[i], e.size,
                                         dl_counter, stop=stop)
                    final_paths[i] = tmp_paths[i]
                    return
                except Exception as exc:
                    if stop.is_set():
                        return
                    if attempt < max_retries:
                        time.sleep(min(attempt * 2, 30))
                        continue
                    errors[i] = exc
                    stop.set()
                    cprint(C.RED, f"\n  [{i+1}/{n}] gave up on "
                           f"{e.branch}:{e.path}: {exc}")
        finally:
            if errors[i] is None and final_paths[i] is not None:
                done_q.put(i)
            events[i].set()

    def dl_hf(i):
        e = matched[i]
        _file_log(C.GRAY, f"[{i+1}/{n}] downloading: {e.branch}:{e.path}")
        parts = e.path.split("/")
        fname = parts[-1]
        sub = "/".join(parts[:-1]) if len(parts) > 1 else None
        file_dir = file_base / e.commit_sha
        file_dir.mkdir(parents=True, exist_ok=True)
        try:
            for attempt in range(1, max_retries + 1):
                if stop.is_set():
                    return
                try:
                    local = hf_hub_download(
                        repo_id=repo_id, filename=fname, subfolder=sub,
                        revision=e.commit_sha, local_dir=str(file_dir),
                        token=hf_token, endpoint=origin,
                        force_download=False, tqdm_class=_CounterBar)
                    final_paths[i] = Path(local)
                    return
                except Exception as exc:
                    if stop.is_set():
                        return
                    if attempt < max_retries:
                        msg = str(exc).splitlines()[0][:120]
                        cprint(C.YELLOW, f"\r  retry {attempt}/{max_retries} "
                               f"{e.branch}:{e.path}: {msg}                ")
                        time.sleep(min(attempt * 2, 30))
                        continue
                    errors[i] = exc
                    stop.set()
                    cprint(C.RED, f"\n  [{i+1}/{n}] gave up on "
                           f"{e.branch}:{e.path}: {exc}")
        finally:
            if errors[i] is None and final_paths[i] is not None:
                done_q.put(i)
            events[i].set()

    dl_fn = dl_urllib if mode == "urllib" else dl_hf

    pads = _bep5_padding_sizes(matched, piece_length)

    check_free_space(tmp_dir, min_free_bytes, "download (pre-flight)")

    hash_progress = multiprocessing.Array('q', n, lock=False)
    hw = max(1, min(hash_workers, n, os.cpu_count() or 1))
    pool = multiprocessing.Pool(hw, initializer=_init_hash_worker,
                                initargs=(hash_progress,))

    _hash_agg_stop = threading.Event()
    _hash_agg = _start_hash_aggregator(hash_progress, hash_counter,
                                       _hash_agg_stop)

    sem = threading.Semaphore(workers)

    def download_one(i):
        with sem:
            dl_fn(i)

    try:
        threads = []
        for i in range(n):
            t = threading.Thread(target=download_one, args=(i,), daemon=True)
            t.start()
            threads.append(t)

        results = [None] * n
        dispatched = 0
        while dispatched < n:
            try:
                i = done_q.get(timeout=30)
            except queue.Empty:
                check_free_space(tmp_dir, min_free_bytes, "download (periodic)")
                continue
            dispatched += 1
            if errors[i] or final_paths[i] is None:
                continue
            lfs_sha256 = matched[i].lfs_sha256
            blob_id = matched[i].blob_id
            pad_size = pads[i][1]
            is_gguf = matched[i].path.lower().endswith(".gguf")
            cb = _unlink_on_success_cb(tmp_paths[i], keep_files)
            results[i] = pool.apply_async(
                _hash_file_worker,
                (str(tmp_paths[i]), matched[i].size, piece_length,
                 pad_size, lfs_sha256, blob_id, i, is_gguf),
                callback=cb)

        pieces = bytearray()
        gguf_metas = {}  # idx → gguf header dict (from hash worker)
        for i in range(n):
            if results[i] is None:
                if errors[i]:
                    raise RuntimeError(f"download failed: {matched[i].path}: {errors[i]}")
                raise RuntimeError(f"no result for: {matched[i].path}")
            piece_hashes, verified, _hashed, error, gguf_meta = results[i].get()
            if error:
                raise RuntimeError(f"hash error: {matched[i].path}: {error}")
            if not verified:
                try:
                    tmp_paths[i].unlink(missing_ok=True)
                except Exception:
                    pass
                raise RuntimeError(f"checksum mismatch: {matched[i].branch}:{matched[i].path}")
            pieces += piece_hashes
            if gguf_meta is not None:
                gguf_metas[i] = gguf_meta
            if pads[i][1] > 0:
                _file_log(C.GRAY, f"[pad] {pads[i][0]} {pads[i][1]} bytes "
                                   f"(BEP-5 alignment)")
            _file_log(C.GRAY, f"[{i+1}/{n}] verified: {matched[i].branch}:{matched[i].path}")
    finally:
        stop.set()
        pool.terminate()
        pool.join()
        _hash_agg_stop.set()
        _hash_agg.join(timeout=2)

    return bytes(pieces), gguf_metas


# ---------------------------------------------------------------------------
# Webseeds (7 total: 1 HF + 2 CNAME + 4 N@)
# ---------------------------------------------------------------------------

def webseeds_hf_multibranch(org):
    """Single HF direct webseed (root of org, no repo in URL — libtorrent
    appends <repo>/resolve/<commit>/<file> via name+path)."""
    return [f"https://huggingface.co/{org}/"]


def webseeds_cname_multibranch(org, repo, infohash, redirector_host):
    """2 CNAME redirector webseeds. z1/z2 prefix for DNS multiplexing."""
    return [f"http://{REDIRECTOR_CNAME_PREFIX}{i}.{redirector_host}/{org}/{repo}/{infohash}/"
            for i in range(1, REDIRECTOR_CNAME_COUNT + 1)]


def webseeds_userinfo_multibranch(org, repo, infohash, redirector_host):
    """4 N@ userinfo redirector webseeds (Transmission-only)."""
    return [f"http://{i}@{redirector_host}/{org}/{repo}/{infohash}/"
            for i in range(1, REDIRECTOR_USERINFO_COUNT + 1)]


def all_webseeds_multibranch(org, repo, infohash, redirector_host):
    """All 7 webseeds: 1 HF + 2 CNAME + 4 N@."""
    return (webseeds_hf_multibranch(org)
            + webseeds_cname_multibranch(org, repo, infohash, redirector_host)
            + webseeds_userinfo_multibranch(org, repo, infohash, redirector_host))


# ---------------------------------------------------------------------------
# Pad materialization (multi-branch variant)
# ---------------------------------------------------------------------------

def materialize_pad_files_multibranch(download_dir, repo, pads):
    """Write BEP-5 .pad.N files into <download_dir>/<repo>/ (the info.name
    directory) so the local Transmission seeder can verify to 100%."""
    real_pads = [(name, size) for name, size in pads if size > 0]
    if not real_pads:
        return 0
    pad_dir = Path(download_dir) / repo
    pad_dir.mkdir(parents=True, exist_ok=True)
    written = 0
    for name, size in real_pads:
        dst = pad_dir / name
        if dst.exists() and dst.stat().st_size == size:
            continue
        tmp = dst.with_suffix(dst.suffix + ".tmp")
        with open(tmp, "wb") as f:
            f.truncate(size)
        os.replace(str(tmp), str(dst))
        written += 1
    return written


# ---------------------------------------------------------------------------
# Transmission add (local muscle)
# ---------------------------------------------------------------------------

def add_to_local_transmission(torrent_path, download_dir):
    """Add .torrent to local Transmission (paused), verify, then start.
    Returns torrent id or None."""
    try:
        from transrpc import TransClient
    except ImportError:
        cprint(C.YELLOW, "[warn] transrpc.py not found, skipping local add")
        return None

    host = os.environ.get("MUSCLE_HOST", "127.0.0.1")
    user = os.environ.get("MUSCLE_USER", "")
    password = os.environ.get("MUSCLE_PASS", "")
    port = int(os.environ.get("MUSCLE_PORT", "9091"))
    https = os.environ.get("MUSCLE_HTTPS", "0") == "1"

    if not user or not password:
        cprint(C.YELLOW, "[warn] MUSCLE_USER/MUSCLE_PASS not set, "
               "skipping local Transmission add")
        return None

    client = TransClient(host, user, password, port=port, https=https)
    with open(torrent_path, "rb") as f:
        metainfo = base64.b64encode(f.read()).decode()

    result = client.torrent_add(metainfo_b64=metainfo, download_dir=download_dir,
                                paused=True)
    tid = result.get("id")
    cprint(C.GREEN, f"  added to local Transmission: id={tid} "
           f"hash={result.get('hashString', '')[:12]}")

    if tid is not None:
        client._call("torrent-verify", {"ids": [tid]})
        for _ in range(60):
            t = client.torrent_get(ids=[tid], fields=["status", "percentDone"])
            if t and t[0]["status"] != 1:  # 1 = checking
                break
            time.sleep(2)
        client.torrent_start([tid])
        cprint(C.DIM, f"  verified + started: "
               f"{t[0]['percentDone']*100:.0f}% complete")

    return tid


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser(
        description="Build a multi-branch .torrent for a HF repo (any "
                    "quant family). All selected branches in ONE torrent. "
                    "Classification auto-detected from GGUF filenames/"
                    "headers, per-branch config.json, and repo-name "
                    "heuristics.")
    p.add_argument("repo", help="HF repo id (org/repo) or full HF URL")
    p.add_argument("--out", default="./torrents",
                   help="output directory (default: ./torrents)")
    p.add_argument("--queue-dir", default="./queue",
                   help="manifest output dir (default: ./queue)")
    p.add_argument("--workers", type=int, default=4,
                   help="parallel download workers (default: 4)")
    p.add_argument("--hash-workers", type=int, default=3,
                   help="parallel hash processes (default: 3)")
    p.add_argument("--max-retries", type=int, default=10)
    p.add_argument("--piece-length", type=int, default=None,
                   help="piece length in bytes (default: auto)")
    p.add_argument("--min-free-gb", type=int, default=50,
                   help="disk guard threshold GB, 0 to disable (default: 50)")
    p.add_argument("--keep-dir", default=None,
                   help="keep downloaded files in this dir (skips tmp)")
    p.add_argument("--downloader", default="urllib",
                   choices=("urllib", "hf"),
                   help="download method (default: urllib)")
    p.add_argument("--yes", action="store_true",
                   help="keep files + add to local Transmission (non-interactive)")
    p.add_argument("--delete", action="store_true",
                   help="delete files after build (non-interactive)")
    p.add_argument("--no-manifest", action="store_true",
                   help="skip writing manifest to ./queue/")
    p.add_argument("--branch", action="append", default=None,
                   help="only include this branch (repeatable; default: all)")
    p.add_argument("--mask", action="append", default=None, metavar="GLOB",
                   help="only include files whose basename matches this glob "
                        "(repeatable; e.g. '*.Q8_0.gguf')")
    p.add_argument("--redirector-host", default=REDIRECTOR_HOST,
                   help="redirector host for CNAME/N@ webseeds "
                        f"(default: {REDIRECTOR_HOST}; use 127.0.0.1:8083 "
                        "for local pump-api-v3 testing)")
    args = p.parse_args()

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

    branch_filter = None
    if args.branch:
        branch_filter = set(args.branch)
    elif url_rev and url_rev != "main":
        branch_filter = {url_rev}
        cprint(C.DIM, f"  URL revision -> branch filter: {url_rev}")
    elif url_rev == "main" and url_subfolder:
        branch_filter = {"main"}
        cprint(C.DIM, f"  URL revision -> branch filter: main (subfolder)")

    if url_subfolder:
        cprint(C.DIM, f"  URL subfolder filter: {url_subfolder}")

    cprint(C.BOLD + C.CYAN, f"\n  {org}/{repo}")
    cprint(C.DIM, f"  redirector: {args.redirector_host}")
    if args.no_manifest:
        cprint(C.YELLOW, "  [no-manifest] will NOT write to ./queue/ "
               "(orchestrator will not pick this up)")
    cprint(C.DIM, "  " + "-" * 50)

    # --- enumerate branches + collect files ---
    api = HfApi()
    repo_id = f"{org}/{repo}"
    try:
        info = api.repo_info(repo_id=repo_id)
        main_commit = info.sha
        tags = getattr(info, "tags", []) or []
    except Exception as exc:
        cprint(C.RED, f"[fail] could not resolve repo: {exc}")
        return 2
    cprint(C.GREEN, f"  main commit: {main_commit[:7]}")

    try:
        matched, branches_meta = collect_all_branch_files(
            api, repo_id, branch_filter, subfolder=url_subfolder)
    except Exception as exc:
        cprint(C.RED, f"[fail] {exc}")
        return 2

    if not matched:
        cprint(C.RED, "[fail] no files found across any branch")
        return 1

    # --- apply --mask (basename glob filter, same as v2's match_files) ---
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

    cprint(C.YELLOW, f"\n  {len(matched)} file(s) across {len(branches_meta)} "
           f"branch(es), {_human_size(sum(f.size for f in matched))}")

    # --- classify (GENERAL decision tree, no hard-coded EXL3) ---
    meta = classify(org, repo, tags, info)
    base_model = meta.get("base_model")
    num_parameters = meta.get("num_parameters")

    has_gguf = any(f.path.lower().endswith(".gguf") for f in matched)

    # 1. GGUF present -> quant_type="gguf", quant_detail from filenames
    #    (mixed -> "many"), cross-checked against GGUF header metadata.
    if has_gguf:
        meta["file_class"] = "quant"
        meta["quant_type"] = "gguf"
        gguf_files = [f for f in matched if f.path.lower().endswith(".gguf")]
        tokens = set()
        for f in gguf_files:
            tok = extract_gguf_token(f.path.rsplit("/", 1)[-1])
            if tok:
                tokens.add(tok)
        # Pre-download GGUF header peek for the first .gguf (early display).
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
            # Filename had no recognizable token — use the header's.
            meta["quant_detail"] = header_tok
        else:
            meta["quant_detail"] = None
        meta["quant_bpw"] = (bpw_for_token(next(iter(tokens)))
                             if len(tokens) == 1 else None)
        meta["model_kind"] = "base"
        if not meta.get("quant_dev"):
            meta["quant_dev"] = org
        # model_name: strip quant suffix from repo name
        stripped = QUANT_NAME_STRIP.sub("", repo) or repo
        meta["model_name"] = stripped
        meta["lab"] = org
    else:
        # 2. config.json across selected branches -> quantization_config
        hf_token = os.environ.get("HF_TOKEN") or None
        branch_configs = []
        cprint(C.DIM, "  fetching config.json per branch ...")
        for b in branches_meta:
            bc = fetch_branch_config(api, repo_id, b["commit_sha"],
                                     hf_token=hf_token)
            branch_configs.append(bc)
            if bc and bc.get("quant_method"):
                cprint(C.DIM, f"    {b['name']:12s}  "
                       f"{bc['quant_method']} bits={bc.get('bits')}")
        qtype, qdetail = aggregate_branch_configs(branch_configs)
        if qtype:
            meta["file_class"] = "quant"
            meta["quant_type"] = qtype
            meta["quant_detail"] = qdetail
            # bits-derived bpw: only meaningful when single bits value
            bitses = {float(c["bits"]) for c in branch_configs
                      if c and c.get("quant_method") and c.get("bits") is not None}
            if len(bitses) == 1:
                meta["quant_bpw"] = next(iter(bitses))
            else:
                meta["quant_bpw"] = None
            meta["model_kind"] = "base"
            if not meta.get("quant_dev"):
                meta["quant_dev"] = org
            # Strip quant suffix from repo name for model_name
            stripped = QUANT_NAME_STRIP.sub("", repo) or repo
            meta["model_name"] = stripped
            meta["lab"] = org
        else:
            # 3. Repo-name / HF-tag heuristics (classify_repo) —
            #    fallback for repos with neither GGUF nor config.json
            #    quant info. meta was already populated by classify().
            if meta["quant_type"] in ("exl3", "exl2"):
                # Multi-branch EXL repos: detail is "many" unless single branch
                if len(branches_meta) > 1:
                    meta["quant_detail"] = "many"
                else:
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
    cprint(C.YELLOW, f"\n  {len(matched)} file(s), {_human_size(total_size)}, "
           f"piece length {pl // 1024} KiB")

    # --- decide keep vs delete BEFORE downloading ---
    keep = False
    if args.yes:
        keep = True
    elif args.delete:
        keep = False
    elif args.keep_dir:
        keep = True
    else:
        cprint(C.BOLD, "\n  Keep downloaded files and add to local Transmission?")
        cprint(C.DIM, "  [y] keep files, add to Transmission (becomes origin seeder)")
        cprint(C.DIM, "  [N] delete files, keep only .torrent + manifest")
        try:
            answer = input(f"  {C.YELLOW}choice [y/N]: {C.R}").strip().lower()
            keep = answer in ("y", "yes")
        except EOFError:
            keep = False
        except KeyboardInterrupt:
            cprint(C.YELLOW, "\n  aborted, files left in tmp dir for resume")
            sys.exit(130)

    # --- hash + verify (download) ---
    hash_counter = [0]

    if args.downloader == "urllib":
        origin = "https://huggingface.co/"
    else:
        origin = "https://huggingface.co"
    hf_token = os.environ.get("HF_TOKEN") or None
    Path("./tmp").mkdir(parents=True, exist_ok=True)

    if args.keep_dir:
        tmp_dir = Path(args.keep_dir).resolve()
        tmp_dir.mkdir(parents=True, exist_ok=True)
    else:
        tmp_dir = Path("./tmp") / f"multibranch-{safe_slug(repo)}"
        if tmp_dir.exists():
            cprint(C.GRAY, f"  tmp dir: {tmp_dir} (resuming)")
        else:
            tmp_dir.mkdir(parents=True, exist_ok=True)
            cprint(C.GRAY, f"  tmp dir: {tmp_dir}")

    dl_counter = [0]
    last_t = time.monotonic()
    last_dl = 0
    last_h = 0
    stop_prog = threading.Event()

    def prog_loop():
        nonlocal last_t, last_dl, last_h
        stall_count = 0
        while not stop_prog.wait(5):
            now = time.monotonic()
            dt = now - last_t
            if dt <= 0:
                continue
            cur = dl_counter[0]
            hcur = hash_counter[0]
            d = cur - last_dl
            hd = hcur - last_h
            rate = d / dt if d >= 0 else 0
            hrate = hd / dt if hd >= 0 else 0
            last_dl = cur
            last_h = hcur
            last_t = now
            shown = min(cur, total_size)
            pct = 100.0 * shown / total_size if total_size else 0
            hshown = min(hcur, total_size)
            hpct = 100.0 * hshown / total_size if total_size else 0
            if rate < 1024 and shown < total_size:
                stall_count += 1
                stall_tag = f"  [stall {stall_count*5}s]" if stall_count >= 2 else ""
            else:
                stall_count = 0
                stall_tag = ""
            cprint(C.GRAY, f"\r\x1b[K  dl: {pct:5.1f}% {_human_size(shown)}/"
                   f"{_human_size(total_size)} {_human_size(rate)}/s"
                   f"{stall_tag}"
                   f"  |  hash: {hpct:5.1f}% {_human_size(hshown)}/"
                   f"{_human_size(total_size)} {_human_size(hrate)}/s",
                   end="")

    pt = threading.Thread(target=prog_loop, daemon=True)
    pt.start()

    try:
        pieces, gguf_metas = compute_pieces_multibranch(
            matched, args.downloader, origin, org, repo, pl,
            args.workers, args.max_retries, tmp_dir,
            dl_counter, hash_counter, args.min_free_gb * GB,
            keep_files=keep, hf_token=hf_token,
            hash_workers=args.hash_workers)
    except KeyboardInterrupt:
        cprint(C.YELLOW, "\n  interrupted — cleaning up")
        stop_prog.set()
        if not args.keep_dir:
            shutil.rmtree(tmp_dir, ignore_errors=True)
        cprint(C.YELLOW, "  done")
        os._exit(130)
    except DiskFullError as exc:
        cprint(C.RED, f"\n[disk full] {exc}")
        stop_prog.set()
        if not args.keep_dir:
            shutil.rmtree(tmp_dir, ignore_errors=True)
            cprint(C.YELLOW, f"  removed partial downloads in {tmp_dir}")
        return 1
    except Exception as exc:
        cprint(C.RED, f"\n[fail] {exc}")
        if not args.keep_dir:
            cprint(C.YELLOW, f"  partial files kept in {tmp_dir} — re-run to resume")
        return 1
    finally:
        stop_prog.set()
        pt.join(timeout=2)

    cprint(C.GREEN, f"\r\x1b[K  downloaded + hashed {_human_size(total_size)}, "
           f"{len(pieces) // 20} pieces                    ")

    # --- post-download GGUF header cross-check (authoritative) ---
    # gguf_metas was collected by the hash workers (they read the first
    # 2 MiB during hashing and parsed it with _gguf_scalar_fields). This
    # works even in --delete mode where files are unlinked by the success
    # callback before main() could re-read them.
    if has_gguf and gguf_metas:
        # Pick the first GGUF file's metadata (idx → dict).
        gguf_files = [(i, f) for i, f in enumerate(matched)
                      if f.path.lower().endswith(".gguf")]
        for gi, gf in gguf_files:
            local_meta = gguf_metas.get(gi)
            if not local_meta:
                continue
            local_tok = (local_meta.get("file_type")
                         or local_meta.get("quant_name"))
            cprint(C.DIM, f"  gguf (local): {gf.path.rsplit('/',1)[-1]} "
                   f"file_type={local_meta.get('file_type')} "
                   f"qname={local_meta.get('quant_name')} "
                   f"arch={local_meta.get('architecture')}")
            # Filename token is primary on disagreement (matches HF repo
            # listing). Only fill in if filename had no token.
            if not meta.get("quant_detail") and local_tok:
                meta["quant_detail"] = local_tok
                display_name = make_display_name(
                    meta["file_class"], meta["lab"], meta["model_name"],
                    meta["quant_type"], meta["quant_dev"], meta["quant_detail"])
            break  # one is enough for display

    # --- build .torrent ---
    pads = _bep5_padding_sizes(matched, pl)
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

    unique_commits = sorted(set(b["commit_sha"] for b in branches_meta))
    commit_summary = ",".join(c[:7] for c in unique_commits)

    metainfo = {
        b"info": info_dict,
        b"announce": TRACKERS[0].encode(),
        b"announce-list": [[t.encode()] for t in TRACKERS],
        b"url-list": [w.encode() for w in webseeds],
        b"creation date": int(time.time()),
        b"comment": f"HF: {org}/{repo} branches=[{commit_summary}]".encode(),
        b"created by": b"maker-v3.py",
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
        "ts": ts, "torrent_path": str(torrent_path), "torrent_stem": stem,
        "infohash": infohash, "magnet": magnet, "name": repo,
        "torrent_name": repo,
        "total_size": total_size, "piece_length": pl,
        "piece_count": len(pieces) // 20, "webseeds": webseeds,
        "trackers": list(TRACKERS), "source": f"huggingface.co/{org}/{repo}",
        "display_name": display_name,
        "repo_id": f"{org}/{repo}",
        "lab": meta.get("lab"),
        "model_name": meta.get("model_name"),
        "version": "multi-branch",
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
        "branches": branches_meta,
        "pads": [{"filename": name, "size": size}
                 for name, size in pads if size > 0],
        "files": [
            {"path": f.path, "size": f.size, "index": i,
             "branch": f.branch, "commit_sha": f.commit_sha}
            for i, f in enumerate(matched)
        ],
    }
    manifest = {k: v for k, v in manifest.items() if v}

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

    # --- keep or delete? ---
    if keep:
        if args.keep_dir:
            download_dir = str(tmp_dir.resolve())
        else:
            shared_dir = Path("./muscle-shared").resolve()
            shared_dir.mkdir(parents=True, exist_ok=True)
            target = shared_dir / f"multibranch-{safe_slug(repo)}"
            if target != tmp_dir:
                if target.exists():
                    shutil.rmtree(target, ignore_errors=True)
                shutil.move(str(tmp_dir), str(target))
                cprint(C.CYAN, f"  moved files to: {target}")
            download_dir = str(target.resolve())
        cprint(C.CYAN, f"\n  keeping files in: {download_dir}")
        n_pads = materialize_pad_files_multibranch(download_dir, repo, pads)
        if n_pads:
            cprint(C.DIM, f"  wrote {n_pads} BEP-5 .pad file(s) for seeder verify")
        tid = add_to_local_transmission(torrent_path, download_dir)
        if tid:
            if not args.no_manifest:
                manifest["local_torrent_id"] = tid
                manifest["local_download_dir"] = download_dir
                with open(qp, "w", encoding="utf-8") as f:
                    json.dump(manifest, f, indent=2, ensure_ascii=False)
                    f.write("\n")
                cprint(C.GREEN, "  updated manifest with local Transmission info")
            else:
                cprint(C.DIM, f"  [no-manifest] skipped manifest update "
                       f"(local tid={tid})")
        cprint(C.GREEN, f"\n  {C.BOLD}done{C.R}  files kept, origin seeder active")
    else:
        if not args.keep_dir:
            shutil.rmtree(tmp_dir, ignore_errors=True)
        cprint(C.YELLOW, f"\n  {C.BOLD}done{C.R}  files deleted, .torrent ready")

    if not args.no_manifest:
        cprint(C.DIM, f"  or:   python post-torr-nostr-j.py {qp}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        cprint(C.YELLOW, "\n  aborted")
        os._exit(130)

#!/usr/bin/env python3
"""post-torr-nostr-j.py
========================
Publishes a torrent listing to the llama.garden Nostr index (kind 30099
parameterized-replaceable events). It is the PUBLISH stage of the
llama.garden pipeline — it does not build torrents itself.

RELATIONSHIP WITH maker-v3 AND maker-v6
---------------------------------------
This script consumes queue job JSON manifests produced by the torrent
builders. It is compatible with BOTH current builders:

  - maker-v6.py  — streaming in-memory builder. Hashes a HuggingFace
                   repo straight over HTTP into the Go piece-hasher, no
                   model bytes touched on disk. Writes its manifest to
                   ./queue/<stem>.<ts>.json. Manifest version tag =
                   "stream".

  - maker-v3.py  — disk-based multi-branch builder. Downloads files to
                   disk first, then hashes them. Also writes manifests
                   to ./queue/<stem>.<ts>.json. Manifest version tag =
                   "multi-branch". Its manifest additionally carries a
                   `branches` array and per-file `branch` fields, which
                   this script ignores.

Both builders emit the SAME required manifest keys (torrent_path,
infohash, magnet, name, total_size, piece_length, piece_count,
webseeds, trackers, source) and the SAME enriched metadata keys
(display_name, file_class, model_kind, quant_type, quant_dev,
quant_detail, quant_bpw, lab, model_name, repo_id, base_model,
created_at, version, commit_sha, subfolder, torrent_name). Enriched
tags are optional on both sides: each is emitted as a Nostr tag only
when present in the manifest, so older manifests (or builders that
don't populate a field) post cleanly without it.

Typical pipeline:
    # 1. Build a .torrent + manifest (pick one builder):
    python maker-v6.py org/repo --num-workers 16
    #   or:
    python maker-v3.py org/repo

    # 2. Publish the finished manifest to Nostr:
    export NSEC=nsec1...
    python post-torr-nostr-j.py              # all jobs in ./queue/
    python post-torr-nostr-j.py queue/foo.json   # one specific job

WHAT THIS SCRIPT DOES (per job)
-------------------------------
  1. Loads the job JSON (required keys + optional enriched metadata).
  2. Uploads the .torrent bytes to every server in BLOSSOM_SERVERS
     using BUD-02 (kind 24242 auth event, PUT /upload). Fail-soft.
  3. Signs a kind 30099 parameterized-replaceable Nostr event whose
     `d` tag is the infohash. Tags carry the magnet, blossom URLs,
     webseeds, trackers, size, piece count, source, AND (when present)
     the enriched model metadata tags.
  4. Fans the signed event out to every relay in NOSTR_RELAYS via
     nostr_sdk.Client.send_event() (strict per-relay OK verification).
  5. Moves the job JSON to ./queue/done/ on success (>=1 relay
     accepted) or ./queue/failed/ on failure.

USAGE
-----
    export NSEC=nsec1...           # REQUIRED (see .env)
    python post-torr-nostr-j.py                  # process ALL jobs in ./queue/
    python post-torr-nostr-j.py queue/foo.json   # process ONE job
    python post-torr-nostr-j.py --queue-dir /other/queue

EXIT CODES
----------
    0   signed + >=1 relay accepted the event
    1   signing failed, or no relay accepted
    2   startup guard failed (NSEC unset / nostr-sdk missing)
"""

import argparse
import asyncio
import base64
import glob
import hashlib
import json
import os
import re
import shutil
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from pathlib import Path

try:
    import nostr_sdk
    _NOSTR_SDK_AVAILABLE = True
except ImportError:
    nostr_sdk = None
    _NOSTR_SDK_AVAILABLE = False

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR    = os.path.join(SCRIPT_DIR, "log")

# Blossom servers that accept application/x-bittorrent (probed 2026-06).
BLOSSOM_SERVERS = [
    "https://nostr.download",
    "https://blossom.primal.net",
    "https://cdn.hzrd149.com",
]

# Fallback relay list used only when relays.txt is missing/empty.
_FALLBACK_RELAYS = [
    "wss://nos.lol/",
    "wss://nostr-01.yakihonne.com/",
    "wss://nostr.land/",
    "wss://nostr.mom/",
    "wss://relay.damus.io/",
    "wss://relay.primal.net/",
    "wss://theforest.nostr1.com/",
    "wss://relay.snort.social",
    "wss://relay.mostr.pub",
    "wss://no.str.cr",
    "wss://offchain.pub",
]

# Relays loaded from relays.txt at startup (one URL per line, # comments /
# blanks ignored). Falls back to _FALLBACK_RELAYS if the file is missing.
NOSTR_RELAYS = []

def load_relays():
    relays_path = os.path.join(SCRIPT_DIR, "relays.txt")
    if not os.path.isfile(relays_path):
        print(f"[warn] {relays_path} not found; using fallback relay list",
              file=sys.stderr)
        return list(_FALLBACK_RELAYS)
    relays = []
    with open(relays_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            relays.append(line)
    if not relays:
        print(f"[warn] no relays in {relays_path}; using fallback relay list",
              file=sys.stderr)
        return list(_FALLBACK_RELAYS)
    return relays

TORRENT_KIND         = 30099      # parameterized replaceable "torrent listing"
BLOSSOM_AUTH_KIND    = 24242      # BUD-02
AUTH_EXPIRY_SECONDS  = 300

NOSTR_CONNECT_TIMEOUT = 10        # seconds to wait for relay Connected status
NOSTR_SEND_TIMEOUT    = 15        # seconds per (event, relay) send

LOG_KEEP             = 10
LOG_GLOB             = os.path.join(LOG_DIR, "post_torr_log_*.jsonl")
SEND_LOG_GLOB        = os.path.join(LOG_DIR, "nostr_send_log_*.jsonl")
SEND_LOG_PREFIX      = "nostr_send_log_"
SEND_LOG_KEEP        = 10


# ---------------------------------------------------------------------------
# STARTUP GUARDS
# ---------------------------------------------------------------------------

def check_setup():
    """Hard-fail at startup if NSEC isn't set or nostr-sdk isn't installed.
    Never prints the nsec value (full or truncated). Also ensures LOG_DIR
    exists so per-send log writes don't crash mid-run."""
    os.makedirs(LOG_DIR, exist_ok=True)
    if not _NOSTR_SDK_AVAILABLE:
        print("ERROR: nostr-sdk not installed; pip install nostr-sdk", file=sys.stderr)
        sys.exit(2)
    nsec = os.environ.get("NSEC")
    if not nsec:
        print("ERROR: NSEC environment variable not set", file=sys.stderr)
        print("       export NSEC=nsec1...   before running (see .env).", file=sys.stderr)
        sys.exit(2)
    try:
        nostr_sdk.Keys.parse(nsec)
    except Exception as e:
        print(f"ERROR: NSEC could not be parsed: {e}", file=sys.stderr)
        sys.exit(2)


# ---------------------------------------------------------------------------
# JOB LOADING
# ---------------------------------------------------------------------------

def load_job(job_path):
    """Load and validate the queue job JSON. Returns the dict.
    Required keys are the same as post-torr-nostr.py so both old (url-to-torr-q)
    and new (url-to-torr-j) manifests work."""
    with open(job_path, "r", encoding="utf-8") as f:
        job = json.load(f)
    required = ["torrent_path", "infohash", "magnet", "name", "total_size",
                "piece_length", "piece_count", "webseeds", "trackers"]
    missing = [k for k in required if k not in job]
    if missing:
        raise ValueError(f"job missing keys: {missing}")
    if not os.path.exists(job["torrent_path"]):
        raise FileNotFoundError(f"torrent file not found: {job['torrent_path']}")
    return job


# ---------------------------------------------------------------------------
# PHASE A: BLOSSOM UPLOAD
# ---------------------------------------------------------------------------

async def make_blossom_auth(nsec, sha256_hex, size):
    """Sign a kind 24242 BUD-02 auth event for upload.
    Minimal tags: t=upload, expiration, x=<sha256>, size=<bytes>.
    The `server` tag is omitted (nostr.download rejects it)."""
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
    """PUT /upload to one blossom server. Returns (ok, url_or_None, error_str)."""
    auth_b64 = base64.b64encode(auth_event.as_json().encode()).decode()
    url = server.rstrip("/") + "/upload"
    req = urllib.request.Request(
        url,
        data=torrent_bytes,
        method="PUT",
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
    """Upload to all blossom servers. Returns list of (server, ok, url, error)."""
    with open(torrent_path, "rb") as f:
        torrent_bytes = f.read()
    sha256_hex = hashlib.sha256(torrent_bytes).hexdigest()
    size = len(torrent_bytes)

    print(f"[blossom] uploading to {len(BLOSSOM_SERVERS)} server(s)...")
    auth_event = await make_blossom_auth(nsec, sha256_hex, size)

    results = []
    for server in BLOSSOM_SERVERS:
        label = server.replace("https://", "").replace("http://", "").rstrip("/")
        ok, url, err = upload_to_blossom(server, torrent_bytes, auth_event)
        results.append((server, ok, url, err))
        if ok:
            print(f"  [OK] {label:24s} -> {url}")
        else:
            print(f"  [FAIL] {label:24s} -> {err}")
    succeeded = sum(1 for _, ok, _, _ in results if ok)
    print(f"[blossom] {succeeded}/{len(BLOSSOM_SERVERS)} succeeded")
    return results


# ---------------------------------------------------------------------------
# PHASE B: SIGN LISTING EVENT (enriched)
# ---------------------------------------------------------------------------

def _opt_tag(job, key, tag_name=None):
    """Build a single Nostr tag from job[key] if that key exists and is not
    None. Returns a list with one nostr_sdk.Tag, or an empty list."""
    if tag_name is None:
        tag_name = key
    val = job.get(key)
    if val is None:
        return []
    # Skip empty strings too; keep 0 / False truthy-falsy edge: only None and
    # "" are dropped so quant_bpw=0.0 (unlikely but valid) would still emit.
    if isinstance(val, str) and val == "":
        return []
    return [nostr_sdk.Tag.parse([tag_name, str(val)])]


async def sign_listing_event(nsec, job, blossom_urls, torrent_sha256, torrent_size):
    """Sign a kind 30099 parameterized-replaceable torrent listing event.
    Emits the original torrent tags PLUS enriched metadata tags (when present
    in the manifest) so waifu-magnet-11.html can render rich cards + toggles.

    Returns the nostr_sdk.Event, or None on failure.

    Tag schema (enriched tags marked *):
        d            = infohash
        magnet       = magnet URI
        name         = torrent_stem | name          (compat; stays the stem)
        size         = total bytes (model size)
        pieces       = piece count
        piece_length = piece length bytes
        x            = .torrent sha256
        torrent_size = .torrent file size in bytes
        torrent_created = .torrent file mtime (ISO 8601)
        m            = application/x-bittorrent
        url          = blossom URL (one per successful upload)
        webseed      = webseed URL (one per webseed)
        tracker      = tracker URL (deduped, one per tracker)
        source       = huggingface.co/<org>/<repo>
        display_name*  = human-friendly name (lab · model_name · quant · dev)
        file_class*    = base | fine tune | quant
        model_kind*    = base | fine tune (fundamental kind; for quants this
                         is the kind of the underlying base_model repo, so
                         quants can be filtered by what they are a quant OF)
        quant_type*    = gguf | mlx | awq | gptq | fp8 | nvfp4 | mxfp4 | bnb | onnx
        quant_dev*     = quantizer org (e.g. unsloth)
        quant_detail*  = quant token (e.g. UD-IQ2_M, BF16, MLX-4bit)
        quant_bpw*     = approximate bits-per-weight
        lab*           = base model lab org
        model_name*    = base model name
        repo_id*       = huggingface repo id this torrent was built from
        base_model*    = base model repo id (org/name)
        created_at*    = HF model createdAt (ISO 8601)
        version*       = HF revision (e.g. main)
    """
    keys = nostr_sdk.Keys.parse(nsec)
    signer = nostr_sdk.NostrSigner.keys(keys)
    builder = nostr_sdk.EventBuilder(nostr_sdk.Kind(TORRENT_KIND), "")

    tags = [
        nostr_sdk.Tag.parse(["d", job["infohash"]]),
        nostr_sdk.Tag.parse(["magnet", job["magnet"]]),
        nostr_sdk.Tag.parse(["name", job.get("torrent_stem") or job["name"]]),
        nostr_sdk.Tag.parse(["size", str(job["total_size"])]),
        nostr_sdk.Tag.parse(["pieces", str(job["piece_count"])]),
        nostr_sdk.Tag.parse(["piece_length", str(job["piece_length"])]),
        nostr_sdk.Tag.parse(["x", torrent_sha256]),
        nostr_sdk.Tag.parse(["torrent_size", str(torrent_size)]),
        nostr_sdk.Tag.parse(["torrent_created", datetime.fromtimestamp(
            os.path.getmtime(job["torrent_path"]), tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")]),
        nostr_sdk.Tag.parse(["m", "application/x-bittorrent"]),
    ]
    for u in blossom_urls:
        tags.append(nostr_sdk.Tag.parse(["url", u]))
    for ws in job["webseeds"]:
        tags.append(nostr_sdk.Tag.parse(["webseed", ws]))
    # Dedup trackers (announce + announce-list may overlap).
    seen_trackers = set()
    for tr in job["trackers"]:
        if tr not in seen_trackers:
            seen_trackers.add(tr)
            tags.append(nostr_sdk.Tag.parse(["tracker", tr]))
    if job.get("source"):
        tags.append(nostr_sdk.Tag.parse(["source", job["source"]]))

    # --- enriched metadata tags (only when present; old manifests unaffected) ---
    tags += _opt_tag(job, "display_name")
    tags += _opt_tag(job, "file_class")
    tags += _opt_tag(job, "model_kind")
    tags += _opt_tag(job, "quant_type")
    tags += _opt_tag(job, "quant_dev")
    tags += _opt_tag(job, "quant_detail")
    tags += _opt_tag(job, "quant_bpw")
    tags += _opt_tag(job, "lab")
    tags += _opt_tag(job, "model_name")
    tags += _opt_tag(job, "repo_id")
    tags += _opt_tag(job, "subfolder")
    tags += _opt_tag(job, "torrent_name")
    # base_model is a dict {repo, lab, name} in the manifest; emit the repo id.
    bm = job.get("base_model")
    if isinstance(bm, dict) and bm.get("repo"):
        tags.append(nostr_sdk.Tag.parse(["base_model", bm["repo"]]))
    elif isinstance(bm, str) and bm:
        tags.append(nostr_sdk.Tag.parse(["base_model", bm]))
    tags += _opt_tag(job, "created_at")
    tags += _opt_tag(job, "version")
    tags += _opt_tag(job, "commit_sha")

    builder = builder.tags(tags)
    try:
        return await builder.sign(signer)
    except Exception as e:
        print(f"[nostr] signing failed: {e}", file=sys.stderr)
        return None


# ---------------------------------------------------------------------------
# PHASE C: RELAY FAN-OUT (strict, send_event waits for OK)
# ---------------------------------------------------------------------------

async def connect_relay(client, relay_url, timeout):
    """Add relay + connect + poll is_connected(). Raises on failure."""
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


async def send_to_relay(relay_url, event_obj, log_path):
    """Send one event to one relay. Returns (ok, latency_ms, error_str).
    Uses client.send_event() which waits for the relay's OK internally."""
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
        print(f"  [FAIL] {label:30s} ({err})")
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
            print(f"  [OK]   {label:30s} ({latency_ms}ms)")
            _record_send(log_path, event_id, relay_url, True, latency_ms, "")
            return True, latency_ms, ""
        elif target in failed_urls:
            reason = out.failed[next(u for u in out.failed if str(u) == target)]
            err = f"rejected: {reason}"
            print(f"  [FAIL] {label:30s} ({err})")
            _record_send(log_path, event_id, relay_url, False, latency_ms, err)
            return False, latency_ms, err
        else:
            err = "no OK from relay (not in success or failed)"
            print(f"  [FAIL] {label:30s} ({err})")
            _record_send(log_path, event_id, relay_url, False, latency_ms, err)
            return False, latency_ms, err
    except asyncio.TimeoutError:
        latency_ms = int((time.monotonic() - t0) * 1000)
        err = "timeout (send_event did not return)"
        print(f"  [FAIL] {label:30s} ({err})")
        _record_send(log_path, event_id, relay_url, False, latency_ms, err)
        return False, latency_ms, err
    except Exception as e:
        latency_ms = int((time.monotonic() - t0) * 1000)
        err = str(e)
        print(f"  [FAIL] {label:30s} ({err})")
        _record_send(log_path, event_id, relay_url, False, latency_ms, err)
        return False, latency_ms, err
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass


async def phase_relay(nsec, event_obj, log_path):
    """Fan out to all relays concurrently. Returns (ok_count, fail_count)."""
    print(f"[nostr] fan-out to {len(NOSTR_RELAYS)} relay(s)...")
    tasks = [send_to_relay(url, event_obj, log_path) for url in NOSTR_RELAYS]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    ok_count = sum(1 for r in results if isinstance(r, tuple) and r[0])
    fail_count = len(NOSTR_RELAYS) - ok_count
    print(f"[nostr] {ok_count}/{len(NOSTR_RELAYS)} accepted")
    return ok_count, fail_count


# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------

def _record_send(log_path, event_id, relay_url, ok, latency_ms, error):
    record = {
        "ts":         int(time.time()),
        "event_id":   event_id,
        "relay":      relay_url,
        "ok":         ok,
        "latency_ms": latency_ms,
        "error":      error,
    }
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def append_run_log(log_path, record):
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def prune_old_logs():
    """Keep only the most recent LOG_KEEP post_torr_log_*.jsonl files."""
    os.makedirs(LOG_DIR, exist_ok=True)
    for glob_pat, keep in ((LOG_GLOB, LOG_KEEP), (SEND_LOG_GLOB, SEND_LOG_KEEP)):
        files = glob.glob(glob_pat)
        if len(files) <= keep:
            continue
        def ts_of(path, _pat=glob_pat):
            m = re.search(r"(\d+)\.jsonl$", os.path.basename(path))
            return int(m.group(1)) if m else 0
        files.sort(key=ts_of)
        for f in files[: len(files) - keep]:
            try:
                os.remove(f)
            except OSError:
                pass


# ---------------------------------------------------------------------------
# JOB LIFECYCLE
# ---------------------------------------------------------------------------

def move_job(job_path, dest_dir):
    """Move the job JSON to dest_dir (created lazily). Returns new path."""
    os.makedirs(dest_dir, exist_ok=True)
    dest = os.path.join(dest_dir, os.path.basename(job_path))
    shutil.move(job_path, dest)
    return dest


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

async def process_one_job(nsec, job_path):
    """Process a single queue job: blossom upload + sign + relay fan-out + move.
    Returns True on success (>=1 relay accepted), False on failure."""
    job_path = os.path.abspath(job_path)
    print(f"[info] loaded job: {job_path}")
    try:
        job = load_job(job_path)
    except Exception as e:
        print(f"[fail] could not load job: {e}", file=sys.stderr)
        move_job(job_path, os.path.join(SCRIPT_DIR, "queue", "failed"))
        return False

    torrent_path = job["torrent_path"]
    torrent_size = os.path.getsize(torrent_path)
    with open(torrent_path, "rb") as f:
        torrent_sha256 = hashlib.sha256(f.read()).hexdigest()
    print(f"[info] torrent: {torrent_path} ({torrent_size} bytes)")
    print(f"[info] infohash: {job['infohash']}")
    print(f"[info] source: {job.get('source', '?')}")
    if job.get("display_name"):
        print(f"[info] display_name: {job['display_name']}")
    if job.get("file_class"):
        print(f"[info] file_class: {job['file_class']}"
              + (f"  quant_type={job.get('quant_type')}"
                 if job.get("quant_type") else ""))
    print()

    # Phase A: Blossom upload
    blossom_results = await phase_blossom(nsec, torrent_path)
    blossom_urls = [url for _, ok, url, _ in blossom_results if ok and url]
    blossom_ok = sum(1 for _, ok, _, _ in blossom_results if ok)
    print()

    # Phase B: Sign listing event
    print("[nostr] signing kind 30099 listing event...")
    event_obj = await sign_listing_event(nsec, job, blossom_urls, torrent_sha256, torrent_size)
    if event_obj is None:
        print("[fail] could not sign event", file=sys.stderr)
        move_job(job_path, os.path.join(SCRIPT_DIR, "queue", "failed"))
        return False
    event_id = event_obj.id().to_hex()
    print(f"[nostr] event id: {event_id}")
    print()

    # Phase C: Relay fan-out
    ts = int(time.time())
    send_log_path = os.path.join(LOG_DIR, f"nostr_send_log_{ts}.jsonl")
    run_log_path = os.path.join(LOG_DIR, f"post_torr_log_{ts}.jsonl")
    ok_count, fail_count = await phase_relay(nsec, event_obj, send_log_path)
    print()

    # Phase D: Job lifecycle
    success = ok_count >= 1
    if success:
        dest = move_job(job_path, os.path.join(SCRIPT_DIR, "queue", "done"))
        print(f"[done] job moved to {dest}")
    else:
        dest = move_job(job_path, os.path.join(SCRIPT_DIR, "queue", "failed"))
        print(f"[fail] job moved to {dest}")

    # Summary line
    print(f"[{'done' if success else 'fail'}] "
          f"blossom: {blossom_ok}/{len(BLOSSOM_SERVERS)}  "
          f"relays: {ok_count}/{len(NOSTR_RELAYS)}  "
          f"event: {event_id}")

    # Run log
    append_run_log(run_log_path, {
        "ts":            ts,
        "job":           os.path.basename(job_path),
        "torrent_path":  torrent_path,
        "infohash":      job["infohash"],
        "event_id":      event_id,
        "blossom_ok":    blossom_ok,
        "blossom_total": len(BLOSSOM_SERVERS),
        "blossom_urls":  blossom_urls,
        "relays_ok":     ok_count,
        "relays_total":  len(NOSTR_RELAYS),
        "success":       success,
        "display_name":  job.get("display_name"),
        "file_class":    job.get("file_class"),
        "quant_type":    job.get("quant_type"),
        "quant_detail":  job.get("quant_detail"),
    })
    prune_old_logs()

    return success


def discover_jobs(queue_dir):
    """Return all *.json files directly in queue_dir (not in done/ or failed/),
    sorted oldest-first by the embedded unix timestamp in the filename.
    Falls back to mtime sort if the filename has no timestamp."""
    import re as _re
    qd = Path(queue_dir)
    if not qd.is_dir():
        return []
    jobs = []
    for p in qd.iterdir():
        if not p.is_file() or p.suffix != ".json":
            continue
        m = _re.search(r"(\d+)\.json$", p.name)
        ts = int(m.group(1)) if m else int(p.stat().st_mtime)
        jobs.append((ts, str(p)))
    jobs.sort(key=lambda x: x[0])
    return [j for _, j in jobs]


async def run_jobs(nsec, jobs):
    """Process a list of job paths sequentially. Returns (ok_count, fail_count)."""
    ok_count = 0
    fail_count = 0
    total = len(jobs)
    for i, job_path in enumerate(jobs, 1):
        print(f"\n{'='*60}")
        print(f"[batch] job {i}/{total}")
        print(f"{'='*60}")
        try:
            success = await process_one_job(nsec, job_path)
        except Exception as e:
            print(f"[fail] unexpected error: {e}", file=sys.stderr)
            try:
                move_job(job_path, os.path.join(SCRIPT_DIR, "queue", "failed"))
            except Exception:
                pass
            success = False
        if success:
            ok_count += 1
        else:
            fail_count += 1
    return ok_count, fail_count


def main():
    p = argparse.ArgumentParser(
        description="Post torrent(s) to Blossom + Nostr (enriched kind 30099). "
                    "Processes every pending job in the queue dir by default.",
    )
    p.add_argument("job", nargs="?", default=None,
                   help="path to a single queue job JSON (optional). "
                        "If omitted, all *.json in --queue-dir are processed.")
    p.add_argument("--queue-dir", default=os.path.join(SCRIPT_DIR, "queue"),
                   help="directory to scan for pending jobs (default: ./queue)")
    args = p.parse_args()

    check_setup()
    nsec = os.environ["NSEC"]

    global NOSTR_RELAYS
    NOSTR_RELAYS = load_relays()
    print(f"[info] relays: {len(NOSTR_RELAYS)} (from relays.txt)")

    # Build the job list: explicit arg wins, else scan the queue dir.
    if args.job is not None:
        if not os.path.exists(args.job):
            print(f"[fail] job file not found: {args.job}", file=sys.stderr)
            return 2
        jobs = [os.path.abspath(args.job)]
    else:
        jobs = discover_jobs(args.queue_dir)
        if not jobs:
            print(f"[info] no pending jobs in {args.queue_dir}")
            return 0
        print(f"[info] found {len(jobs)} pending job(s) in {args.queue_dir}")

    try:
        ok_count, fail_count = asyncio.run(run_jobs(nsec, jobs))
    except KeyboardInterrupt:
        print("\n[interrupt] aborted", file=sys.stderr)
        return 130

    print(f"\n[summary] {ok_count} succeeded, {fail_count} failed "
          f"out of {len(jobs)} job(s)")
    return 0 if fail_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())

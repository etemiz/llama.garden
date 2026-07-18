#!/usr/bin/env python3
"""publish-waifu-magnet.py
============================
Upload a waifu-magnet HTML release to Blossom servers and announce it on
Nostr via a kind 30100 parameterized-replaceable event.

USAGE
-----
    export NSEC=nsec1...
    python publish-waifu-magnet.py waifu-magnet-1.html --version 1

Relays are loaded from relays.txt in the script directory (one URL per
line, # comments / blanks ignored). If relays.txt is missing or empty,
a built-in fallback list of 11 public relays is used.

WHAT IT DOES
------------
  1. Reads the HTML file, computes sha256 + size.
  2. Uploads to all BLOSSOM_SERVERS (BUD-02 PUT /upload,
     Content-Type: text/html), authenticated with a kind 24242
     Blossom auth event (t=upload, expiration, x=<sha256>, size) signed
     with NSEC and base64-encoded in the Authorization: Nostr header.
  3. Signs a kind 30100 parameterized-replaceable event:
       d       = "waifu-magnet"
       version = <from --version arg>
       url     = <blossom URL> (one tag per successful upload)
       sha256  = <html file sha256>
       size    = <html file bytes>
  4. Fans out to all NOSTR_RELAYS concurrently (strict send_event OK
     verification — the relay's success set must contain the target URL
     or the send is counted as failed). Per-relay result, latency, and
     error are appended as one JSONL line per relay to
     log/nostr_send_log_<unix_ts>.jsonl.

STARTUP GUARDS
--------------
  - Exits 2 if nostr-sdk is not importable.
  - Exits 2 if NSEC env var is unset or fails to parse as a valid key.

EXIT CODES
----------
    0   signed + >=1 relay accepted
    1   signing failed, or no relay accepted
    2   startup guard (NSEC / nostr-sdk missing)
    130 Ctrl-C during run
"""

import argparse
import asyncio
import base64
import hashlib
import json
import os
import sys
import time
import urllib.request
import urllib.error

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

CLIENT_KIND          = 30100
BLOSSOM_AUTH_KIND    = 24242
AUTH_EXPIRY_SECONDS  = 300
NOSTR_CONNECT_TIMEOUT = 10
NOSTR_SEND_TIMEOUT    = 15


# ---------------------------------------------------------------------------
# STARTUP GUARDS
# ---------------------------------------------------------------------------

def check_setup():
    if not _NOSTR_SDK_AVAILABLE:
        print("ERROR: nostr-sdk not installed; pip install nostr-sdk", file=sys.stderr)
        sys.exit(2)
    nsec = os.environ.get("NSEC")
    if not nsec:
        print("ERROR: NSEC environment variable not set", file=sys.stderr)
        sys.exit(2)
    try:
        nostr_sdk.Keys.parse(nsec)
    except Exception as e:
        print(f"ERROR: NSEC could not be parsed: {e}", file=sys.stderr)
        sys.exit(2)
    os.makedirs(LOG_DIR, exist_ok=True)


# ---------------------------------------------------------------------------
# BLOSSOM UPLOAD
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


def upload_to_blossom(server, html_bytes, auth_event):
    auth_b64 = base64.b64encode(auth_event.as_json().encode()).decode()
    url = server.rstrip("/") + "/upload"
    req = urllib.request.Request(
        url, data=html_bytes, method="PUT",
        headers={
            "Authorization": f"Nostr {auth_b64}",
            "Content-Type": "text/html",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            body = r.read().decode("utf-8", errors="replace")
            resp = json.loads(body)
            blob_url = resp.get("url")
            if blob_url:
                return True, blob_url, None
            return False, None, f"no url in response: {body[:200]}"
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8", errors="replace")[:200]
        except Exception:
            pass
        return False, None, f"HTTP {e.code}: {body}"
    except Exception as e:
        return False, None, str(e)


async def phase_blossom(nsec, html_bytes):
    sha256_hex = hashlib.sha256(html_bytes).hexdigest()
    size = len(html_bytes)
    print(f"[blossom] uploading to {len(BLOSSOM_SERVERS)} server(s)...")
    auth_event = await make_blossom_auth(nsec, sha256_hex, size)
    results = []
    for server in BLOSSOM_SERVERS:
        label = server.replace("https://", "").rstrip("/")
        ok, url, err = upload_to_blossom(server, html_bytes, auth_event)
        results.append((server, ok, url, err))
        if ok:
            print(f"  [OK]   {label:24s} -> {url}")
        else:
            print(f"  [FAIL] {label:24s} -> {err}")
    succeeded = sum(1 for _, ok, _, _ in results if ok)
    print(f"[blossom] {succeeded}/{len(BLOSSOM_SERVERS)} succeeded")
    return results


# ---------------------------------------------------------------------------
# SIGN RELEASE EVENT
# ---------------------------------------------------------------------------

async def sign_release_event(nsec, version, blossom_urls, sha256_hex, size):
    keys = nostr_sdk.Keys.parse(nsec)
    signer = nostr_sdk.NostrSigner.keys(keys)
    builder = nostr_sdk.EventBuilder(nostr_sdk.Kind(CLIENT_KIND), "")
    tags = [
        nostr_sdk.Tag.parse(["d", "waifu-magnet"]),
        nostr_sdk.Tag.parse(["version", str(version)]),
        nostr_sdk.Tag.parse(["sha256", sha256_hex]),
        nostr_sdk.Tag.parse(["size", str(size)]),
    ]
    for u in blossom_urls:
        tags.append(nostr_sdk.Tag.parse(["url", u]))
    builder = builder.tags(tags)
    try:
        return await builder.sign(signer)
    except Exception as e:
        print(f"[nostr] signing failed: {e}", file=sys.stderr)
        return None


# ---------------------------------------------------------------------------
# RELAY FAN-OUT
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
        raise RuntimeError("relay not found in client.relays()")
    deadline = time.monotonic() + timeout
    while True:
        try:
            if relay.is_connected():
                return
        except Exception:
            pass
        if time.monotonic() >= deadline:
            raise TimeoutError("relay did not reach Connected")
        await asyncio.sleep(0.1)


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
            err = "no OK from relay"
            print(f"  [FAIL] {label:30s} ({err})")
            _record_send(log_path, event_id, relay_url, False, latency_ms, err)
            return False, latency_ms, err
    except asyncio.TimeoutError:
        latency_ms = int((time.monotonic() - t0) * 1000)
        err = "timeout"
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


async def phase_relay(event_obj, log_path):
    print(f"[nostr] fan-out to {len(NOSTR_RELAYS)} relay(s)...")
    tasks = [send_to_relay(url, event_obj, log_path) for url in NOSTR_RELAYS]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    ok_count = sum(1 for r in results if isinstance(r, tuple) and r[0])
    print(f"[nostr] {ok_count}/{len(NOSTR_RELAYS)} accepted")
    return ok_count


def _record_send(log_path, event_id, relay_url, ok, latency_ms, error):
    record = {
        "ts": int(time.time()), "event_id": event_id, "relay": relay_url,
        "ok": ok, "latency_ms": latency_ms, "error": error,
    }
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

async def async_main(args, nsec):
    html_path = os.path.abspath(args.html)
    if not os.path.exists(html_path):
        print(f"[fail] file not found: {html_path}", file=sys.stderr)
        return 2
    with open(html_path, "rb") as f:
        html_bytes = f.read()
    sha256_hex = hashlib.sha256(html_bytes).hexdigest()
    size = len(html_bytes)
    version = args.version
    print(f"[info] file: {html_path} ({size} bytes)")
    print(f"[info] sha256: {sha256_hex}")
    print(f"[info] version: {version}")
    print()

    # Phase A: Blossom upload
    blossom_results = await phase_blossom(nsec, html_bytes)
    blossom_urls = [url for _, ok, url, _ in blossom_results if ok and url]
    blossom_ok = sum(1 for _, ok, _, _ in blossom_results if ok)
    print()

    # Phase B: Sign release event
    print("[nostr] signing kind 30100 release event...")
    event_obj = await sign_release_event(nsec, version, blossom_urls, sha256_hex, size)
    if event_obj is None:
        return 1
    event_id = event_obj.id().to_hex()
    print(f"[nostr] event id: {event_id}")
    print()

    # Phase C: Relay fan-out
    ts = int(time.time())
    send_log_path = os.path.join(LOG_DIR, f"nostr_send_log_{ts}.jsonl")
    ok_count = await phase_relay(event_obj, send_log_path)
    print()

    success = ok_count >= 1
    print(f"[{'done' if success else 'fail'}] "
          f"blossom: {blossom_ok}/{len(BLOSSOM_SERVERS)}  "
          f"relays: {ok_count}/{len(NOSTR_RELAYS)}  "
          f"event: {event_id}")

    return 0 if success else 1


def main():
    p = argparse.ArgumentParser(
        description="Publish a waifu-magnet HTML release to Blossom + Nostr (kind 30100).",
    )
    p.add_argument("html", help="path to the waifu-magnet HTML file")
    p.add_argument("--version", type=int, required=True,
                   help="release version number (integer, e.g. 1, 2, 3)")
    args = p.parse_args()

    check_setup()
    nsec = os.environ["NSEC"]

    global NOSTR_RELAYS
    NOSTR_RELAYS = load_relays()
    print(f"[info] relays: {len(NOSTR_RELAYS)} (from relays.txt)")

    try:
        return asyncio.run(async_main(args, nsec))
    except KeyboardInterrupt:
        print("\n[interrupt] aborted", file=sys.stderr)
        return 130


if __name__ == "__main__":
    sys.exit(main())

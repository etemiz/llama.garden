#!/usr/bin/env python3
"""pump-api-v3.py — HTTP API for infohash → pump location data (v3).

Multi-branch variant of pump-api-v2.py. Runs on port 8083 during parallel
testing (v2 stays on 8082 untouched). Shares pumps.lmdb + downloads.lmdb +
downloads-ts.lmdb + resolve-cache-v2.lmdb + pad-sizes.json (read-only or
atomic-increment, safe for concurrent access).

DIFFERENCE from v2: handles the multi-branch torrent layout where
info.name = <repo> (no slash) and file paths = ["resolve", <commit>, <file>].
libtorrent appends <name>/<path> to trailing-slash webseeds, so the
redirector sees:
  /<org>/<repo>/<ih>/<repo>/resolve/<commit>/<file>
→ split("/",3) gives rest = "<repo>/resolve/<commit>/<file>".

The <repo>/resolve/ prefix must be stripped before building the HF
/resolve/ URL (otherwise it doubles to /resolve/<repo>/resolve/...).
This strip is backward-compatible: v2 torrents send rest = "<commit>/<file>"
which doesn't start with "<repo>/resolve/", so the strip is a no-op for
them. Both v2 and v3 torrents are handled correctly by v3.

The pump redirect path is NOT stripped: pumps serve files from their
download dir which follows the on-disk layout <repo>/resolve/<commit>/<file>,
so the pump's /seed/ path needs the full rest.

Reads LMDB written by report.py (./pumps.lmdb/). Designed to run behind
nginx reverse proxy.

Endpoints:
  POST /pumps
    body:   {"infohashes": ["<hex40>", ...]}
    return: {"results": {"<hex40>": [{"percent_done": N}, ...]}}
    NOTE: pump/muscle IPs are never exposed — only percent_done counts.

  POST /pumps?downloading=1
    Same body/return, but also increments a per-infohash download counter
    in ./downloads.lmdb and includes a "downloads" map in the response:
    {"results": {...}, "downloads": {"<hex40>": N}}
    Schema: infohash (raw 20 bytes) → integer download count.

    Each download is ALSO recorded by timestamp in ./downloads-ts.lmdb
    so demand can be estimated over time windows (hourly / 24h).
    Schema: key = 8-byte big-endian unix_ts_seconds + 20-byte infohash
            value = 8-byte little-endian count (incremented per hit)

  GET /<org>/<repo>/<infohash>/<rest...>   (webseed redirector, legacy)
    Same as /v2/ below but kept for backcompat with existing .torrents
    whose webseeds point at http://<N>@api.llama.garden/<org>/<repo>/<ih>/

  GET /v2/<org>/<repo>/<infohash>/<rest...>   (webseed redirector, canonical)
    BT client requests a webseed piece. The url-list in the .torrent is
    http://z<i>.api.llama.garden/<org>/<repo>/<infohash>/  (i=1..8, CNAME
    multiplexing — z1..z8 are CNAMEs to api.llama.garden, so each is a
    distinct authority = 8 parallel connections in both Transmission and
    libtorrent without the N@ userinfo trick that libtorrent can't resolve).
    libtorrent appends <info.name>/<file.subpath> to the webseed URL,
    so this server sees: /<org>/<repo>/<infohash>/<info.name>/<file>
    Looks up infohash in pumps.lmdb:
      - found: 302 redirect to http://<pump-host>/seed/<info.name>/<file>
               (HTTP not HTTPS — pumps use self-signed certs that BT
                clients refuse. Pick a random pump that has the torrent;
                prefer percent_done >= 70%, else fall back to HF.)
      - not found: resolve-cache lookup. If cached and not expired → 302
               to the cached signed CDN URL (us.aws.cdn.hf.co/…, ~7h TTL,
               NOT rate-limited, serves any Range). On cache miss →
               server-side resolve huggingface.co/<org>/<repo>/resolve/<rest>
               (follows 302→signed CDN or 307→/api/resolve-cache/), stores
               the final URL with TTL, then 302 the BT client to it.
               This cuts /resolve/ hits from ~1200/torrent to ~1/file.
               A token-bucket self-guard (~1 resolve/sec, burst 10) keeps
               pump-api's own HF resolver usage well under the 3000/300s
               budget. Stale-while-revalidate: expired entries are served
               immediately while a background refresh runs.

Usage (dev):
  python pump-api-v3.py [--port 8083] [--db pumps.lmdb] [--downloads-db downloads.lmdb]
                     [--downloads-ts-db downloads-ts.lmdb]
                     [--resolve-cache-db resolve-cache-v2.lmdb]

Production (merge phase): replace v2 on 8082, point nginx proxy_pass to
http://127.0.0.1:8082; retire pump-api-v2.py.
"""

import argparse
import json
import os
import re
import secrets
import struct
import time
from http.server import HTTPServer, ThreadingHTTPServer
from socketserver import ThreadingMixIn
from pathlib import Path
from urllib.parse import urlparse, parse_qs, quote, urlencode, parse_qs as _pqs
import threading
import urllib.request
import urllib.error
import hashlib

from lmdbm import Lmdb

# A valid lowercase-hex infohash (40 chars).
_INFOHASH_RE = re.compile(r"^[0-9a-f]{40}$")


# ---------------------------------------------------------------------------
# Minimal bencode decoder — extracts only the `info` dict from a .torrent
# file so we can compute the infohash and read pad file sizes. Used as a
# fallback when pad-sizes.json doesn't have an entry (e.g. torrent built
# on a different machine). Avoids importing a full bencode library.
# ---------------------------------------------------------------------------

def _bdecode(buf, pos=0):
    """Decode a bencoded value starting at buf[pos]. Returns (value, new_pos).
    Raises ValueError on malformed input."""
    c = buf[pos:pos + 1]
    if c == b'i':
        end = buf.index(b'e', pos + 1)
        val = int(buf[pos + 1:end])
        return val, end + 1
    if c == b'l':
        pos += 1
        lst = []
        while buf[pos:pos + 1] != b'e':
            v, pos = _bdecode(buf, pos)
            lst.append(v)
        return lst, pos + 1
    if c == b'd':
        pos += 1
        d = {}
        while buf[pos:pos + 1] != b'e':
            k, pos = _bdecode(buf, pos)
            v, pos = _bdecode(buf, pos)
            d[k] = v
        return d, pos + 1
    # string: "<len>:<bytes>"
    colon = buf.index(b':', pos)
    length = int(buf[pos:colon])
    start = colon + 1
    return buf[start:start + length], start + length


def _extract_info_and_pads(torrent_path):
    """Bdecode a .torrent file, return (infohash_hex, {pad_filename: size})
    or (None, None) on failure. Pad files are those with attr=p or whose
    path is a single segment starting with '.pad'."""
    try:
        raw = open(torrent_path, 'rb').read()
        meta, _ = _bdecode(raw)
        info = meta[b'info']
        infohash = hashlib.sha1(
            _bencode_info(info)).hexdigest() if False else None
        # We need to re-encode the info dict to compute the infohash.
        # Use hashlib over the raw info-dict bytes instead.
        infohash = _infohash_from_raw(raw)
        pads = {}
        files = info.get(b'files', [])
        for f in files:
            path = f.get(b'path', [])
            attrs = f.get(b'attr', b'')
            if attrs == b'p' or (len(path) == 1 and
                                  path[0].startswith(b'.pad')):
                name = path[0].decode('utf-8', 'replace')
                pads[name] = f[b'length']
        return infohash, pads
    except Exception:
        return None, None


def _infohash_from_raw(raw):
    """Find the info dict's byte span in raw torrent data and SHA-1 it."""
    try:
        # Locate b"4:infod" marker and bdecode from the 'd' after it.
        marker = b'4:info'
        idx = raw.index(marker) + len(marker)
        _val, end = _bdecode(raw, idx)
        return hashlib.sha1(raw[idx:end]).hexdigest()
    except Exception:
        return None

# Resolve-cache TTLs (seconds).
# HF signed CDN URLs have ~1h Expires; use a 5-min safety so the
# effective cache TTL is ~55min — enough for thousands of piece requests
# to be served from cache without re-resolving.
_CDN_TTL_SAFETY = 300         # subtract 5 min from signed-URL Expires
_CDN_TTL_DEFAULT = 1800       # 30 min if we can't parse Expires
_RESOLVECACHE_TTL = 604800    # 7 days (commit-pinned, etag-stable, CF-cached)
_RESOLVECACHE_MARKER = "/api/resolve-cache/"

# Token bucket for HF /resolve/ self-guard.
_TOKEN_RATE = 1.0       # 1 token/sec
_TOKEN_BURST = 10       # max 10 accumulated

# Proactive refresh: track which cache keys are being actively requested
# and pre-refresh them before expiry so BT clients never hit a 403.
_REFRESH_INTERVAL = 30          # refresh loop runs every 30s
_REFRESH_MARGIN = 120          # refresh if expiring within 2 min
_REQUEST_TRACK_TTL = 600        # forget a key if not requested in 10 min


class JsonLmdb(Lmdb):
    def _pre_value(self, value):
        return json.dumps(value).encode("utf-8")
    def _post_value(self, value):
        return json.loads(value.decode("utf-8"))


class IntLmdb(Lmdb):
    """LMDB storing raw bytes keys → integer values (as 8-byte little-endian)."""
    def _pre_value(self, value):
        return int(value).to_bytes(8, "little")
    def _post_value(self, value):
        return int.from_bytes(value, "little")


def _ts_key(ts, raw_ih):
    """8-byte big-endian unix seconds + 20-byte infohash → 28-byte LMDB key.

    Big-endian timestamp keeps keys in chronological order so the stats
    script can range-scan a time window efficiently.
    """
    return struct.pack(">Q", int(ts)) + raw_ih


class TokenBucket:
    """Simple thread-safe token bucket. acquire(timeout) blocks until a
    token is available or timeout; returns True on success, False on timeout."""
    def __init__(self, rate, burst):
        self.rate = rate
        self.burst = burst
        self.tokens = float(burst)
        self.last = time.monotonic()
        self.cond = threading.Condition()

    def _refill(self):
        now = time.monotonic()
        elapsed = now - self.last
        self.last = now
        self.tokens = min(self.burst, self.tokens + elapsed * self.rate)

    def acquire(self, timeout=30):
        deadline = time.monotonic() + timeout
        with self.cond:
            while True:
                self._refill()
                if self.tokens >= 1:
                    self.tokens -= 1
                    return True
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self.cond.wait(timeout=min(remaining, 1.0 / self.rate + 0.1))


def _parse_cdn_expires(url):
    """Extract the Expires= unix timestamp from a signed HF CDN URL.
    Returns None if not found."""
    m = re.search(r"[?&]Expires=(\d+)", url)
    if m:
        try:
            return int(m.group(1))
        except ValueError:
            pass
    return None


def _strip_resolve_prefix(repo, rest):
    """Strip a leading '<repo>/resolve/' from rest for multi-branch torrents
    (v3 layout: info.name=<repo>, file path=['resolve', commit, file]).

    v2 torrents have rest='<commit>/<file>' (no prefix) → no-op.
    v3 torrents have rest='<repo>/resolve/<commit>/<file>' → strip to
    '<commit>/<file>' so the HF /resolve/ URL is built correctly.

    The pump redirect path does NOT call this (pumps serve from the full
    on-disk path <repo>/resolve/<commit>/<file>).
    """
    prefix = f"{repo}/resolve/"
    if rest.startswith(prefix):
        return rest[len(prefix):]
    return rest


class PumpHandler:
    def __init__(self, db_path, downloads_db_path, downloads_ts_db_path,
                 resolve_cache_db_path, pad_sizes_db_path="pad-sizes.json",
                 reload_interval=60, torrents_dir="torrents"):
        self.db_path = db_path
        self.downloads_db_path = downloads_db_path
        self.downloads_ts_db_path = downloads_ts_db_path
        self.resolve_cache_db_path = resolve_cache_db_path
        self.pad_sizes_db_path = pad_sizes_db_path
        self.torrents_dir = torrents_dir
        self._downloads_lock = threading.Lock()
        self._pumps_lock = threading.Lock()
        self._resolve_cache_lock = threading.Lock()
        self._pad_sizes_lock = threading.Lock()
        self._pad_sizes = {}            # infohash hex → pad bytes (int)
        self._pad_sizes_mtime = 0.0
        self._resolve_inflight = {}          # key → threading.Event
        self._resolve_bucket = TokenBucket(_TOKEN_RATE, _TOKEN_BURST)
        self._request_track = {}             # cache_key → last_request_ts
        self._request_track_lock = threading.Lock()
        # Open pumps.lmdb read-only once at startup, but refresh it
        # periodically so report.py changes are picked up without a
        # restart. Opening per-request segfaults under concurrent webseed
        # bursts (16+ GETs hitting simultaneously, each doing its own LMDB
        # env open). LMDB readers are thread-safe within a shared env.
        self._pumps_db = None
        if Path(db_path).is_dir():
            try:
                self._pumps_db = JsonLmdb.open(db_path, "r")
            except Exception:
                self._pumps_db = None
        # Ensure the downloads DBs exist (create if missing).
        if not Path(downloads_db_path).is_dir():
            with IntLmdb.open(downloads_db_path, "c") as db:
                pass
        if not Path(downloads_ts_db_path).is_dir():
            with IntLmdb.open(downloads_ts_db_path, "c") as db:
                pass
        # Ensure resolve-cache DB exists (read-write, persistent).
        if not Path(resolve_cache_db_path).is_dir():
            with JsonLmdb.open(resolve_cache_db_path, "c") as db:
                pass
        # Open resolve-cache LMDB once at startup (read-write). Opening
        # per-request segfaults under concurrent webseed bursts, same
        # lesson as pumps.lmdb. lmdbm readers are thread-safe within a
        # shared env; writes need a lock.
        self._resolve_cache_db = None
        try:
            self._resolve_cache_db = JsonLmdb.open(resolve_cache_db_path, "w")
        except Exception as e:
            print(f"  warning: failed to open resolve-cache DB: {e}")

        # Load pad-sizes (BEP-5 padding file sizes keyed by infohash hex).
        # url-to-torr-combo-v2.py writes this when it generates a BEP-5 .pad
        # file. pump-api-v2 serves the .pad as zero bytes so Transmission
        # (which does NOT honor the BEP-5 attr=p "skip" flag and tries to
        # download the .pad from webseeds) can complete. qBittorrent honors
        # attr=p and never requests the .pad. See unified-webseeds.md.
        self._reload_pad_sizes()

        self._reload_interval = reload_interval
        self._reload_thread = None
        if self._reload_interval > 0:
            t = threading.Thread(target=self._reload_loop, daemon=True)
            t.start()
            self._reload_thread = t

        self._refresh_thread = threading.Thread(target=self._refresh_loop, daemon=True)
        self._refresh_thread.start()

    def _reload_pumps_db(self):
        if not Path(self.db_path).is_dir():
            return
        # LMDB does not allow two Environment handles for the same path
        # in the same process, so close the old handle before opening a
        # fresh one. Lookups briefly see None while we swap.
        with self._pumps_lock:
            old_db = self._pumps_db
            self._pumps_db = None
        if old_db is not None:
            try:
                old_db.close()
            except Exception:
                pass
        try:
            new_db = JsonLmdb.open(self.db_path, "r")
        except Exception as e:
            print(f"  warning: failed to reload pumps.lmdb: {e}")
            return
        with self._pumps_lock:
            self._pumps_db = new_db

    def _reload_loop(self):
        while True:
            time.sleep(self._reload_interval)
            self._reload_pumps_db()

    def _refresh_loop(self):
        """Background thread: proactively re-resolve cache entries that are
        expiring soon, but only for keys that have been recently requested
        by BT clients. Keys nobody wants are left to expire naturally."""
        while True:
            time.sleep(_REFRESH_INTERVAL)
            try:
                self._do_refresh_cycle()
            except Exception as e:
                print(f"  refresh cycle error: {e}")

    def _do_refresh_cycle(self):
        now = int(time.time())
        cutoff = now - _REQUEST_TRACK_TTL

        # Snapshot of recently-requested keys and their cache expiry.
        with self._request_track_lock:
            stale_tracks = [k for k, ts in self._request_track.items() if ts < cutoff]
            for k in stale_tracks:
                del self._request_track[k]
            candidates = list(self._request_track.keys())

        if not candidates:
            return

        # Check each candidate: if cached and expiring soon, re-resolve.
        refreshed = 0
        for cache_key in candidates:
            cached = self._cache_get(cache_key)
            if cached is None:
                continue
            url, expires_at = cached
            if expires_at > now + _REFRESH_MARGIN:
                continue

            # Parse org/repo/rest from cache_key (format: "org/repo/rest")
            parts = cache_key.split("/", 2)
            if len(parts) < 3:
                continue
            org, repo, rest = parts

            # Skip if another thread is already resolving this key.
            with self._resolve_cache_lock:
                if cache_key in self._resolve_inflight:
                    continue

            if not self._resolve_bucket.acquire(timeout=5):
                break

            try:
                final_url, new_expires = self._do_resolve(org, repo, rest)
            except Exception:
                final_url, new_expires = None, 0
            if final_url is not None:
                self._cache_put(cache_key, final_url, new_expires)
                refreshed += 1

        if refreshed:
            print(f"  proactive refresh: {refreshed} URLs re-resolved")

    def _pump_lookup(self, raw_ih):
        """Look up raw_infohash in pumps.lmdb → list of {ip, percent_done}
        or None. Thread-safe via lock (LMDB reads are fast)."""
        if self._pumps_db is None:
            return None
        with self._pumps_lock:
            try:
                return self._pumps_db[raw_ih]
            except KeyError:
                return None
            except Exception:
                return None

    # ------------------------------------------------------------------
    # Pad-sizes (BEP-5 .pad zero-serving for Transmission)
    # ------------------------------------------------------------------

    def _reload_pad_sizes(self):
        """Reload pad-sizes.json if its mtime changed. Maps infohash hex
        → {pad_filename: size_bytes} (dict).

        Backward compat: old format stored `{infohash: int_size}` (single
        .pad per torrent, filename = ".pad"). New format is
        `{infohash: {filename: size, ...}}` for multi-pad torrents
        (filenames = ".pad.0", ".pad.1", …). Both are normalized to dicts
        in memory so the lookup path doesn't need to branch.
        """
        p = Path(self.pad_sizes_db_path)
        try:
            mtime = p.stat().st_mtime
        except OSError:
            with self._pad_sizes_lock:
                self._pad_sizes = {}
                self._pad_sizes_mtime = 0.0
            return
        if mtime == self._pad_sizes_mtime:
            return
        try:
            with open(p, "r", encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, dict):
                data = {}
            sizes = {}
            for ih, v in data.items():
                if isinstance(v, dict):
                    sizes[ih] = {k: int(sz) for k, sz in v.items()}
                elif isinstance(v, (int, float)):
                    # Legacy single-pad format — assume filename ".pad".
                    sizes[ih] = {".pad": int(v)}
                # else: ignore unknown shapes
        except (OSError, ValueError, TypeError) as e:
            print(f"  warning: failed to load pad-sizes: {e}")
            return
        with self._pad_sizes_lock:
            self._pad_sizes = sizes
            self._pad_sizes_mtime = mtime
        n_pads = sum(len(v) for v in sizes.values())
        print(f"  loaded {n_pads} pad-size(s) across {len(sizes)} "
              f"torrent(s) from {p.name}")

    def _pad_size_lookup(self, infohash_hex, pad_filename):
        """Return the BEP-5 pad size (bytes) for an infohash + pad filename,
        or None. Reloads pad-sizes.json on mtime change first.

        `pad_filename` is the last path segment of the requested .pad
        file (e.g. ".pad", ".pad.0", ".pad.1"). Legacy torrents use ".pad";
        new multi-pad torrents use ".pad.N" where N is the position.
        """
        self._reload_pad_sizes()
        with self._pad_sizes_lock:
            entry = self._pad_sizes.get(infohash_hex.lower())
        if entry is None:
            # Fallback: torrent may have been built on a different
            # machine. Scan the local torrents/ dir for this infohash
            # and extract pad sizes from the .torrent file.
            entry = self._scan_torrents_for_pads(infohash_hex)
            if entry is None:
                return None
        if pad_filename in entry:
            return entry[pad_filename]
            # Fallback: legacy torrents may store the pad under ".pad" but
            # the BT client requests ".pad.0" (or vice versa). If the
            # entry has exactly one pad, return its size regardless of
            # filename — the BT client can't have multiple .pad files
            # with the same name in a single torrent.
            if len(entry) == 1:
                return next(iter(entry.values()))
            return None

    def _scan_torrents_for_pads(self, infohash_hex):
        """Fallback: scan torrents/*.torrent for the given infohash,
        extract pad file sizes, cache them in self._pad_sizes, and
        merge into pad-sizes.json so it persists across restarts.

        Used when a torrent was built on a different machine and its
        pad sizes were never written to the local pad-sizes.json.
        Returns the pad-size dict for this infohash, or None."""
        ih = infohash_hex.lower()
        tdir = Path(self.torrents_dir)
        if not tdir.is_dir():
            return None
        # Try filename-based prefilter: maker-v4 names files with the
        # infohash prefix (first 7 hex chars). Fall back to scanning all.
        candidates = sorted(tdir.glob(f"*{ih[:7]}*.torrent"))
        if not candidates:
            candidates = sorted(tdir.glob("*.torrent"))
        found = None
        for tp in candidates:
            try:
                raw = open(tp, 'rb').read()
                file_ih = _infohash_from_raw(raw)
                if file_ih != ih:
                    continue
                _meta, end = _bdecode(raw)
                _info_val = _meta[b'info']
                # Re-extract pads from the decoded info dict.
                meta, _ = _bdecode(raw)
                info = meta[b'info']
                pads = {}
                for f in info.get(b'files', []):
                    path = f.get(b'path', [])
                    attrs = f.get(b'attr', b'')
                    if attrs == b'p' or (len(path) == 1 and
                                          path[0].startswith(b'.pad')):
                        name = path[0].decode('utf-8', 'replace')
                        pads[name] = f[b'length']
                if pads:
                    found = pads
                    break
            except Exception:
                continue
        if found is None:
            return None
        # Cache in memory.
        with self._pad_sizes_lock:
            self._pad_sizes[ih] = found
        # Persist to pad-sizes.json so subsequent restarts have it.
        try:
            p = Path(self.pad_sizes_db_path)
            data = {}
            if p.exists():
                data = json.load(open(p, 'r'))
                if not isinstance(data, dict):
                    data = {}
            data[ih] = found
            tmp = p.with_suffix('.json.tmp')
            with open(tmp, 'w') as f:
                json.dump(data, f, indent=2)
            tmp.replace(p)
            self._pad_sizes_mtime = p.stat().st_mtime
            print(f"  [pad-fallback] extracted {len(found)} pad(s) for "
                  f"{ih[:12]}… from {Path(tp).name} -> {p.name}")
        except Exception as e:
            print(f"  [pad-fallback] failed to persist: {e}")
        return found

    # ------------------------------------------------------------------
    # Resolve-cache: server-side resolve of HF /resolve/ → final CDN URL
    # ------------------------------------------------------------------

    def _cache_get(self, key):
        """Read resolve-cache entry for key. Returns (url, expires_at) or None."""
        if self._resolve_cache_db is None:
            return None
        try:
            with self._resolve_cache_lock:
                val = self._resolve_cache_db.get(key.encode("utf-8"))
                if val is None:
                    return None
                return val.get("url"), val.get("expires_at", 0)
        except Exception:
            return None

    def _cache_put(self, key, url, expires_at):
        if self._resolve_cache_db is None:
            return
        try:
            with self._resolve_cache_lock:
                self._resolve_cache_db[key.encode("utf-8")] = {
                    "url": url, "expires_at": expires_at}
        except Exception as e:
            print(f"  warning: resolve-cache write failed: {e}")

    def _do_resolve(self, org, repo, rest):
        """Make one server-side request to HF /resolve/ and return the
        final URL after redirects, plus the computed expiry time.

        No Range header is sent — if we send Range, HF locks the signed
        CDN URL's Policy to that specific byte range, making it useless
        for arbitrary piece requests. Without Range, the signed URL
        serves any Range the BT client requests.

        v3 multi-branch: rest may be '<repo>/resolve/<commit>/<file>'
        — strip the '<repo>/resolve/' prefix before building the HF URL.
        v2 torrents: rest is already '<commit>/<file>' (no-op strip).
        """
        rest = _strip_resolve_prefix(repo, rest)
        resolve_url = f"https://huggingface.co/{org}/{repo}/resolve/{rest}"
        req = urllib.request.Request(resolve_url, method="HEAD")
        try:
            resp = urllib.request.urlopen(req, timeout=15)
            final_url = resp.url
            resp.close()
        except urllib.error.HTTPError as e:
            # Some servers don't support HEAD on /resolve/ — fall back
            # to a GET with no Range, closing immediately.
            if e.code in (405, 501):
                try:
                    req2 = urllib.request.Request(resolve_url)
                    resp2 = urllib.request.urlopen(req2, timeout=15)
                    final_url = resp2.url
                    resp2.close()
                except Exception:
                    return None, 0
            else:
                return None, 0
        except Exception:
            return None, 0
        # Compute TTL.
        if _RESOLVECACHE_MARKER in final_url:
            expires_at = int(time.time()) + _RESOLVECACHE_TTL
        else:
            exp = _parse_cdn_expires(final_url)
            if exp:
                expires_at = exp - _CDN_TTL_SAFETY
            else:
                expires_at = int(time.time()) + _CDN_TTL_DEFAULT
        return final_url, expires_at

    def _resolve_hf(self, org, repo, rest):
        """Resolve-cache lookup with in-flight dedup and token-bucket guard.
        Returns a final URL string, or None on failure. On failure the caller
        should fall back to a direct /resolve/ 302."""
        cache_key = f"{org}/{repo}/{rest}"
        now = int(time.time())

        # Track that this key was requested so the refresh loop
        # knows to proactively re-resolve it before expiry.
        with self._request_track_lock:
            self._request_track[cache_key] = now

        # Fast path: check cache.
        cached = self._cache_get(cache_key)
        if cached is not None:
            url, expires_at = cached
            if expires_at > now:
                return url

        # In-flight dedup: if another thread is already resolving this
        # key, wait for it then read the cache. Only one thread per key
        # actually calls HF.
        with self._resolve_cache_lock:
            existing = self._resolve_inflight.get(cache_key)
            if existing is not None:
                # Waiter path: another thread is resolving.
                event = existing
                am_resolver = False
            else:
                # Resolver path: we own this key.
                event = threading.Event()
                self._resolve_inflight[cache_key] = event
                am_resolver = True

        if not am_resolver:
            event.wait(timeout=35)
            fresh = self._cache_get(cache_key)
            if fresh is not None:
                return fresh[0]
            return None

        # We are the resolver thread. Acquire token-bucket slot so
        # pump-api itself never trips HF's 3000/300s rate limit.
        if not self._resolve_bucket.acquire(timeout=30):
            with self._resolve_cache_lock:
                self._resolve_inflight.pop(cache_key, None)
                event.set()
            return None

        try:
            final_url, expires_at = self._do_resolve(org, repo, rest)
        finally:
            with self._resolve_cache_lock:
                self._resolve_inflight.pop(cache_key, None)
                event.set()

        if final_url is None:
            return None
        self._cache_put(cache_key, final_url, expires_at)
        return final_url

    # ------------------------------------------------------------------
    # HTTP handlers
    # ------------------------------------------------------------------

    def handle_head(self, path):
        """Handle HEAD requests for webseed URLs.

        Returns 200 with Content-Length + Accept-Ranges (NOT a 302) so
        libtorrent sees a valid webseed. Range GETs still go through
        handle_webseed_redirect which returns 302→CDN.

        libtorrent 2.0 does NOT follow 302 redirects on HEAD probes for
        file-style webseeds (BEP19 type 2). It treats 302 as invalid and
        drops the webseed. Returning 200 with the correct Content-Length
        (from the resolved CDN URL) makes libtorrent accept the webseed
        and issue Range GETs, which DO follow 302s.
        """
        path_stripped = path
        if path.startswith("/v2/"):
            path_stripped = path[3:]
        parts = path_stripped.lstrip("/").split("/", 3)
        if len(parts) < 4:
            return 404, None
        org, repo, infohash, rest = parts
        if not _INFOHASH_RE.match(infohash):
            return 404, None
        # BEP-5 .pad: return its size as Content-Length (zeros, served by
        # the GET handler). Avoids a pointless HF resolve (404). Matches
        # ".pad" (legacy) and ".pad.N" (new multi-pad layout) by checking
        # the prefix.
        last_seg = rest.rsplit("/", 1)[-1]
        if last_seg == ".pad" or last_seg.startswith(".pad."):
            pad_size = self._pad_size_lookup(infohash, last_seg)
            if pad_size is not None:
                return 200, {"_content_length": pad_size}
        # Resolve the HF URL to get the CDN URL (which has Content-Length)
        final_url = self._resolve_hf(org, repo, rest)
        if not final_url:
            return 404, None
        # Try to get Content-Length from the CDN URL
        content_length = self._head_content_length(final_url)
        if content_length is not None:
            return 200, {"_content_length": content_length,
                         "_redirect": final_url}
        # Fallback: return 302 (old behavior, works for folder-style)
        return 302, {"_redirect": final_url}

    def _head_content_length(self, url):
        """HEAD the CDN URL and return Content-Length, or None on failure."""
        try:
            req = urllib.request.Request(url, method="HEAD")
            resp = urllib.request.urlopen(req, timeout=10)
            cl = resp.headers.get("Content-Length")
            resp.close()
            return int(cl) if cl else None
        except Exception:
            return None

    def handle(self, method, path, body, query):
        if method == "POST" and path == "/pumps":
            return self.handle_pumps(body, query)
        if method == "GET" and path == "/health":
            return 200, {"ok": True}
        if method == "GET":
            return self.handle_webseed_redirect(path)
        return 404, {"error": "not found"}

    def handle_webseed_redirect(self, path):
        """Redirect a webseed piece request to a pump (or cached HF CDN URL).

        Path shape: /<org>/<repo>/<infohash>/<rest...>
        or          /v2/<org>/<repo>/<infohash>/<rest...>
        where <rest> = <info.name>/<file.subpath> (what libtorrent appends
        to a webseed URL ending in '/').

        For the unified CNAME layout, the rest is the same regardless of
        whether the webseed URL was folder-style (trailing /) or file-style
        (no trailing /, full path embedded). Both arrive as:
            /<org>/<repo>/<infohash>/<commit>/<file>
        → split("/",3) gives rest=<commit>/<file>.

        Returns (status, body) where body is either a redirect dict (for
        _respond_json) or a special ("redirect", location) tuple handled
        by the caller.
        """
        is_v2 = False
        path_stripped = path
        if path.startswith("/v2/"):
            is_v2 = True
            path_stripped = path[3:]  # drop "/v2" → keep leading slash

        # Strip leading slash, split into segments.
        parts = path_stripped.lstrip("/").split("/", 3)
        if len(parts) < 4:
            return 404, {"error": "not found"}
        org, repo, infohash, rest = parts
        # Validate infohash. If invalid, 404 (don't redirect garbage).
        if not _INFOHASH_RE.match(infohash):
            return 404, {"error": "not found"}
        try:
            raw_ih = bytes.fromhex(infohash)
        except ValueError:
            return 404, {"error": "not found"}

        # BEP-5 padding file (.pad, .pad.0, .pad.1, …): Transmission does
        # NOT honor the attr=p "skip" flag and tries to download the .pad
        # from webseeds. The .pad doesn't exist on HF → a normal 302→HF
        # resolve would 404 and stall Transmission at ~99.9%. Serve zero
        # bytes directly here so Transmission completes. qBittorrent
        # honors attr=p and never requests the .pad, so it is unaffected.
        # The pad size is looked up by (infohash, filename) in
        # pad-sizes.json (written by the torrent builder).
        last_seg = rest.rsplit("/", 1)[-1]
        if last_seg == ".pad" or last_seg.startswith(".pad."):
            pad_size = self._pad_size_lookup(infohash, last_seg)
            if pad_size is not None:
                return 200, {"_pad": pad_size}

        # Look up pumps that have this infohash.
        pump_host = None
        val = self._pump_lookup(raw_ih)
        if isinstance(val, list) and val:
            # Only redirect to fully-complete pumps (>= 100%). A
            # partially-complete pump 404s on the files it hasn't
            # finished, and BT clients don't re-ask pump-api for
            # another pump after a 302→404, so those pieces stall.
            # Fall back to HF resolve-cache until pumps are complete.
            candidates = [e for e in val
                          if isinstance(e, dict)
                          and e.get("percent_done", 0) >= 100]
            if candidates:
                chosen = secrets.choice(candidates)
                # report.py writes {"ip": "<host>:<port>", ...}.
                # Take the host part; pump webseed runs on port 80.
                name = chosen.get("ip", "")
                if name:
                    pump_host = name.split(":", 1)[0]

        if pump_host:
            # 302 redirect to the pump's HTTP /seed/ location.
            # HTTP not HTTPS — pumps use self-signed certs that BT
            # clients refuse for webseeds. <rest> already URL-encoded
            # by the BT client; quote() defensively around each segment
            # isn't needed since we pass it through verbatim.
            location = f"http://{pump_host}/seed/{rest}"
            return 302, {"_redirect": location}

        # No pump has it — HF fallback via resolve-cache.
        # Both legacy and /v2/ endpoints use the cached resolve so old
        # torrents benefit from the 429 fix too.
        final_url = self._resolve_hf(org, repo, rest)
        if final_url:
            return 302, {"_redirect": final_url}
        # Resolve failed entirely — last-resort direct 302 to /resolve/.
        # The BT client will hit HF directly (may 429, but better than
        # nothing). Strip the v3 '<repo>/resolve/' prefix if present.
        stripped_rest = _strip_resolve_prefix(repo, rest)
        location = f"https://huggingface.co/{org}/{repo}/resolve/{stripped_rest}"
        return 302, {"_redirect": location}

    def handle_pumps(self, body, query):
        if not body:
            return 400, {"error": "empty body"}
        try:
            data = json.loads(body)
        except json.JSONDecodeError:
            return 400, {"error": "invalid json"}
        infohashes = data.get("infohashes")
        if not isinstance(infohashes, list):
            return 400, {"error": "infohashes must be an array"}
        downloading = query.get("downloading", ["0"])[0] == "1"
        results = {}
        downloads = {}
        # First pass: increment counters for downloading torrents (if any).
        if downloading:
            now = int(time.time())
            with self._downloads_lock:
                with IntLmdb.open(self.downloads_db_path, "w") as ddb:
                    for h in infohashes:
                        if not isinstance(h, str) or len(h) != 40:
                            continue
                        try:
                            raw = bytes.fromhex(h)
                        except ValueError:
                            continue
                        try:
                            count = ddb[raw]
                        except KeyError:
                            count = 0
                        count += 1
                        ddb[raw] = count
                # Also record by timestamp so we can estimate demand over
                # hourly / 24h windows. Key = ts||infohash, value = count.
                with IntLmdb.open(self.downloads_ts_db_path, "w") as tdb:
                    for h in infohashes:
                        if not isinstance(h, str) or len(h) != 40:
                            continue
                        try:
                            raw = bytes.fromhex(h)
                        except ValueError:
                            continue
                        key = _ts_key(now, raw)
                        try:
                            n = tdb[key]
                        except KeyError:
                            n = 0
                        tdb[key] = n + 1
        # Second pass: read pump data AND download counts for ALL requested
        # infohashes. Counts are always returned so a fresh page load can
        # display them without sending ?downloading=1.
        for h in infohashes:
            if not isinstance(h, str) or len(h) != 40:
                continue
            try:
                raw = bytes.fromhex(h)
            except ValueError:
                continue
            val = self._pump_lookup(raw)
            if isinstance(val, list):
                results[h] = [{"percent_done": e.get("percent_done")}
                              for e in val if isinstance(e, dict)]
            else:
                results[h] = []
        with self._downloads_lock, IntLmdb.open(self.downloads_db_path, "r") as ddb:
            for h in infohashes:
                if not isinstance(h, str) or len(h) != 40:
                    continue
                try:
                    raw = bytes.fromhex(h)
                except ValueError:
                    continue
                try:
                    downloads[h] = ddb[raw]
                except KeyError:
                    downloads[h] = 0
        return 200, {"results": results, "downloads": downloads}


def serve_forever(host, port, db_path, downloads_db_path, downloads_ts_db_path,
                 resolve_cache_db_path, pad_sizes_db_path="pad-sizes.json"):
    handler = PumpHandler(db_path, downloads_db_path, downloads_ts_db_path,
                          resolve_cache_db_path, pad_sizes_db_path)

    class _Handler:
        def do_GET(self):
            parsed = urlparse(self.path)
            code, data = handler.handle("GET", parsed.path, None, parse_qs(parsed.query))
            self._respond(code, data)

        def do_POST(self):
            parsed = urlparse(self.path)
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length) if length else None
            code, data = handler.handle("POST", parsed.path, body, parse_qs(parsed.query))
            self._respond(code, data)

        def do_HEAD(self):
            # libtorrent (qBittorrent) probes webseeds with HEAD before
            # issuing Range GETs. For file-style webseeds, libtorrent does
            # NOT follow 302 redirects on HEAD — it treats 302 as invalid
            # and drops the webseed. So we return 200 with Content-Length
            # + Accept-Ranges (resolved from the CDN), making libtorrent
            # accept the webseed. Range GETs still get 302→CDN.
            parsed = urlparse(self.path)
            if parsed.path == "/health":
                self._respond_head(200, {"ok": True})
                return
            # Check if it's a webseed path (not /pumps, /health, etc.)
            path_stripped = parsed.path[3:] if parsed.path.startswith("/v2/") else parsed.path
            parts = path_stripped.lstrip("/").split("/", 3)
            if len(parts) >= 4 and _INFOHASH_RE.match(parts[2]):
                code, data = handler.handle_head(parsed.path)
                self._respond_head(code, data)
            else:
                code, data = handler.handle("GET", parsed.path, None, parse_qs(parsed.query))
                self._respond_head(code, data)

        def do_OPTIONS(self):
            self.send_response(204)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type")
            self.send_header("Access-Control-Max-Age", "86400")
            self.end_headers()

        def _respond(self, code, data):
            # Redirector: dict has _redirect key → 302 with Location.
            if isinstance(data, dict) and "_redirect" in data:
                self.send_response(code)
                self.send_header("Location", data["_redirect"])
                self.send_header("Cache-Control", "no-store")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                return
            # BEP-5 .pad: serve zero bytes (with Range support).
            if isinstance(data, dict) and "_pad" in data:
                self._serve_pad(int(data["_pad"]))
                return
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps(data).encode("utf-8"))

        def _serve_pad(self, pad_size):
            """Serve `pad_size` bytes of zeros, honoring a Range header.
            Used for BEP-5 padding files so Transmission (which does not
            honor attr=p) can download them from the webseed."""
            rng = self.headers.get("Range")
            start, end = 0, pad_size - 1
            partial = False
            if rng:
                m = re.match(r"bytes=(\d*)-(\d*)", rng)
                if m:
                    s, e = m.group(1), m.group(2)
                    if s:
                        start = int(s)
                    if e:
                        end = int(e)
                    if start > end or start >= pad_size:
                        self.send_response(416)
                        self.send_header("Content-Range",
                                         f"bytes */{pad_size}")
                        self.send_header("Access-Control-Allow-Origin", "*")
                        self.end_headers()
                        return
                    if end >= pad_size:
                        end = pad_size - 1
                    partial = True
            length = end - start + 1
            if partial:
                self.send_response(206)
                self.send_header("Content-Range",
                                 f"bytes {start}-{end}/{pad_size}")
            else:
                self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(length))
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            # Stream zeros in chunks (pad_size is small, < 1 piece).
            chunk = b"\x00" * min(65536, length)
            remaining = length
            while remaining > 0:
                n = min(len(chunk), remaining)
                self.wfile.write(chunk[:n])
                remaining -= n

        def _respond_head(self, code, data):
            # Same headers as _respond but NEVER write a body (HEAD method).
            if isinstance(data, dict):
                if "_content_length" in data:
                    # HEAD probe returning 200 with Content-Length (not 302)
                    # so libtorrent accepts the webseed. Range GETs will get
                    # 302→CDN separately.
                    self.send_response(200)
                    self.send_header("Content-Length", str(data["_content_length"]))
                    self.send_header("Accept-Ranges", "bytes")
                    self.send_header("Cache-Control", "no-store")
                    self.send_header("Access-Control-Allow-Origin", "*")
                    self.end_headers()
                    return
                if "_redirect" in data:
                    self.send_response(code)
                    self.send_header("Location", data["_redirect"])
                    self.send_header("Cache-Control", "no-store")
                    self.send_header("Access-Control-Allow-Origin", "*")
                    self.end_headers()
                    return
            body = json.dumps(data).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()

    import http.server
    cls = type("Server", (ThreadingMixIn, http.server.HTTPServer), {
        "allow_reuse_address": True,
        "daemon_threads": True,
    })
    server = cls((host, port), type("Handler", (http.server.BaseHTTPRequestHandler,), {
        "do_GET": _Handler.do_GET,
        "do_POST": _Handler.do_POST,
        "do_HEAD": _Handler.do_HEAD,
        "do_OPTIONS": _Handler.do_OPTIONS,
        "_respond": _Handler._respond,
        "_respond_head": _Handler._respond_head,
        "_serve_pad": _Handler._serve_pad,
    }))
    print(f"pump-api-v3 listening on {host}:{port}  db={db_path}  resolve-cache={resolve_cache_db_path}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nshutdown")
        server.server_close()


def main():
    p = argparse.ArgumentParser(description="pump-api-v3 server (multi-branch webseed layout)")
    p.add_argument("--port", type=int, default=8083)
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--db", default="pumps.lmdb")
    p.add_argument("--downloads-db", default="downloads.lmdb")
    p.add_argument("--downloads-ts-db", default="downloads-ts.lmdb")
    p.add_argument("--resolve-cache-db", default="resolve-cache-v2.lmdb")
    p.add_argument("--pad-sizes-db", default="pad-sizes.json")
    args = p.parse_args()

    if not Path(args.db).is_dir():
        print(f"db not found: {args.db} (run report.py first)")
        return 1

    serve_forever(args.host, args.port, args.db, args.downloads_db,
                  args.downloads_ts_db, args.resolve_cache_db,
                  args.pad_sizes_db)


if __name__ == "__main__":
    import sys
    sys.exit(main())

#!/usr/bin/env python3
"""transrpc.py
=============
Minimal Transmission RPC client using only the standard library.

Handles the X-Transmission-Session-Id CSRF handshake automatically.
All methods return parsed JSON (dict/list) and raise TransRpcError on failure.
"""

import base64
import json
import urllib.request
import urllib.error
import ssl

SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE


class TransRpcError(Exception):
    pass


class TransClient:
    def __init__(self, host, user, password, port=443, https=True, timeout=30):
        self.base_url = f"{'https' if https else 'http'}://{host}:{port}/transmission/rpc"
        self.auth = base64.b64encode(f"{user}:{password}".encode()).decode()
        self.timeout = timeout
        self.session_id = None

    def _post(self, payload, timeout=None):
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Basic {self.auth}",
        }
        if self.session_id:
            headers["X-Transmission-Session-Id"] = self.session_id
        data = json.dumps(payload).encode()
        req = urllib.request.Request(self.base_url, data=data, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=timeout or self.timeout,
                                        context=SSL_CTX) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code == 409:
                sid = e.headers.get("X-Transmission-Session-Id")
                if sid:
                    self.session_id = sid
                    return self._post(payload, timeout=timeout)
                raise TransRpcError("409 but no session-id header")
            raise TransRpcError(f"HTTP {e.code}: {e.reason}")

    def _call(self, method, arguments=None, timeout=None):
        payload = {"method": method}
        if arguments:
            payload["arguments"] = arguments
        resp = self._post(payload, timeout=timeout)
        if resp.get("result") != "success":
            raise TransRpcError(f"{method}: {resp.get('result')}")
        return resp.get("arguments", {})

    # --- session ---

    def session_get(self, fields=None):
        args = {"fields": fields} if fields else {}
        return self._call("session-get", args)

    def session_set(self, **kwargs):
        if not kwargs:
            raise TransRpcError("session-set needs at least one argument")
        return self._call("session-set", kwargs)

    def session_stats(self):
        return self._call("session-stats")

    def free_space(self):
        s = self.session_get(fields=["download-dir-free-space"])
        return s.get("download-dir-free-space", 0)

    # --- torrent listing ---

    def torrent_get(self, ids=None, fields=None):
        args = {}
        if ids is not None:
            args["ids"] = ids
        if fields:
            args["fields"] = fields
        else:
            args["fields"] = [
                "id", "name", "hashString", "status", "totalSize",
                "percentDone", "rateDownload", "rateUpload", "files",
                "fileStats", "uploadRatio", "uploadedEver",
            ]
        r = self._call("torrent-get", args)
        return r.get("torrents", [])

    # --- add / remove / control ---

    def torrent_add(self, metainfo_b64=None, filename=None, download_dir=None,
                    paused=False, files_wanted=None, files_unwanted=None):
        args = {"paused": paused}
        if metainfo_b64:
            args["metainfo"] = metainfo_b64
        elif filename:
            args["filename"] = filename
        else:
            raise TransRpcError("need metainfo or filename")
        if download_dir:
            args["download-dir"] = download_dir
        if files_wanted is not None:
            args["files-wanted"] = files_wanted
        if files_unwanted is not None:
            args["files-unwanted"] = files_unwanted
        r = self._call("torrent-add", args)
        if "torrent-added" in r:
            return r["torrent-added"]
        if "torrent-duplicate" in r:
            return r["torrent-duplicate"]
        return r

    def torrent_remove(self, ids, delete_local_data=True, timeout=None):
        return self._call("torrent-remove",
                          {"ids": ids, "delete-local-data": delete_local_data},
                          timeout=timeout)

    def torrent_start(self, ids=None):
        args = {"ids": ids} if ids is not None else {}
        return self._call("torrent-start", args)

    def torrent_stop(self, ids=None):
        args = {"ids": ids} if ids is not None else {}
        return self._call("torrent-stop", args)

    def torrent_set(self, ids, files_wanted=None, files_unwanted=None,
                    location=None, download_limit=None, upload_limit=None):
        args = {"ids": ids}
        if files_wanted is not None:
            args["files-wanted"] = files_wanted
        if files_unwanted is not None:
            args["files-unwanted"] = files_unwanted
        if location:
            args["location"] = location
        if download_limit is not None:
            args["downloadLimit"] = download_limit
        if upload_limit is not None:
            args["uploadLimit"] = upload_limit
        return self._call("torrent-set", args)

    # --- convenience ---

    def find_by_hash(self, infohash):
        torrents = self.torrent_get(
            fields=["id", "hashString", "name", "percentDone", "status"])
        for t in torrents:
            if t.get("hashString", "").lower() == infohash.lower():
                return t
        return None

    def ping(self):
        try:
            self.session_get(fields=["version"])
            return True
        except Exception:
            return False

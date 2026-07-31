// Command hf-piece-hasher streams files from Hugging Face over HTTP and
// computes BitTorrent v1 piece hashes (20-byte SHA1 digests) in memory.
// No disk I/O. Designed to be invoked by maker-v6.py.
//
// Purpose
// -------
// llama.garden (https://llama.garden) is a torrent index of language models
// mirrored from HuggingFace into BitTorrent swarms. To create a .torrent you
// need the `info.pieces` field: the concatenation of one 20-byte SHA1 digest
// per piece, in file order. For multi-gigabyte repos that is a lot of bytes
// to hash, and doing it in pure Python while also downloading the files is
// slow. This Go binary does both steps at once — it streams each file over
// HTTP with N parallel workers and feeds the bytes straight into a rolling
// SHA1 piece buffer, never touching disk. The parent Python process
// (maker-v6.py) reads the raw digests from stdout and bencodes them into the
// final .torrent.
//
// I/O protocol
// -------------
//   Input  (stdin):  one JSON object conforming to the Spec struct below.
//                    See the Spec/FileSpec fields for the exact schema.
//   Output (stdout): raw concatenated 20-byte SHA1 piece digests, in file
//                    order (matching the `index` field). The byte stream is
//                    exactly 20 * (number of pieces) bytes long and is ready
//                    to drop straight into the bencoded `info.pieces` field
//                    with no further processing.
//   Stderr:          newline-delimited JSON progress lines (see ProgressLine)
//                    for the parent to parse and render a progress bar. The
//                    final line has type "done" with totals; "error" means a
//                    fatal failure and the process exits with code 1.
//
// Exit codes
// ----------
//   0  all files streamed and hashed successfully; stdout is complete.
//   1  at least one file failed after all retries; stdout is to be discarded.
//
// Build
// -----
//   cd hf-piece-hasher && go build .
//   (Go 1.22+; go.mod says 1.23 but `sed -i 's/go 1.23/go 1.22/'` works.)
//
// Standalone usage (without maker-v6.py)
// --------------------------------------
//   echo '{"piece_length":16777216,"files":[{"index":0,"url":"https://...","size":12345}]}' \
//     | ./hf-piece-hasher > pieces.bin
//
// See also: maker-v6.py (the Python orchestrator), post-torr-nostr-j.py
// (publishes the resulting .torrent to Nostr as a kind 30099 listing).
package main

import (
	"bufio"
	"bytes"
	"crypto/sha1"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// FileSpec describes one file to stream+hash. The parent (maker-v6.py)
// builds one of these per file in the HuggingFace repo, in the order they
// should appear in the torrent's info dict. The `index` field fixes the
// output ordering so results stay deterministic regardless of which
// worker finishes first.
type FileSpec struct {
	Index   int    `json:"index"`
	URL     string `json:"url"`
	Size    int64  `json:"size"`
	PadSize int64  `json:"pad_size"`  // BEP-5 virtual pad bytes (zero-filled)
	OutPath string `json:"out_path"`  // if non-empty, write file here as it streams
}

// Spec is the top-level JSON input read from stdin. PieceLength is the
// BitTorrent piece size in bytes (must be a power of two, typically
// 16 MiB for large repos). Files is the ordered list of files to hash.
// HFToken is optional but required for gated repos and recommended for
// public repos to avoid anonymous rate limits; it is sent as a Bearer
// Authorization header and never logged. NumWorkers, ChunkSize and
// MaxRetries fall back to sane defaults (8, 1 MiB, 10) when zero or absent.
type Spec struct {
	PieceLength int        `json:"piece_length"`
	Files       []FileSpec `json:"files"`
	HFToken     string     `json:"hf_token,omitempty"`
	NumWorkers  int        `json:"num_workers,omitempty"`  // default 8
	ChunkSize   int        `json:"chunk_size,omitempty"`    // default 1 MiB
	MaxRetries  int        `json:"max_retries,omitempty"`   // default 10
}

// Result is what a worker goroutine returns. Pieces holds the raw
// 20-byte SHA1 digests concatenated in piece order for that one file;
// the parent concatenates all results in `index` order to form the full
// `info.pieces` blob. Hashed is the number of real (non-pad) bytes
// streamed, used for progress reporting.
type Result struct {
	Index  int
	Pieces []byte
	Hashed int64
	Err    error
}

// ProgressLine is written to stderr as JSON for the parent to parse and
// render a progress bar. Type is one of:
//   "progress"  — periodic aggregate tick (every 500ms) with total bytes
//                 hashed so far and a rate in MB/s.
//   "file_done" — one file finished successfully; Index identifies it.
//   "file_err"  — one file failed; Index identifies it, Msg has the error.
//   "done"      — final line, all files done; carries total bytes + elapsed.
//   "error"     — fatal, process exits with code 1 right after.
type ProgressLine struct {
	Type      string `json:"type"` // "progress" | "file_done" | "file_err" | "done" | "error"
	Index     int    `json:"index,omitempty"`
	Bytes     int64  `json:"bytes,omitempty"`
	Hashed    int64  `json:"hashed,omitempty"`
	Elapsed   string `json:"elapsed,omitempty"`
	Rate      string `json:"rate,omitempty"`
	Msg       string `json:"msg,omitempty"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, `{"type":"error","msg":%q}`+"\n", err.Error())
		os.Exit(1)
	}
}

// run is the real entry point; main wraps it to convert errors into a
// stderr JSON "error" line and a non-zero exit. It reads the JSON spec
// from stdin, validates the fields (applying defaults for NumWorkers,
// ChunkSize and MaxRetries), spins up a shared http.Client with a pooled
// transport sized to the worker count, fans out one goroutine per file
// (bounded by a semaphore channel), waits for all of them, verifies that
// every file produced at least one piece, then writes the concatenated
// digests to stdout in file-index order and a final "done" line to stderr.
// On any per-file failure it returns an error before touching stdout so
// the parent never sees a partial pieces blob.
func run() error {
	raw, err := io.ReadAll(os.Stdin)
	if err != nil {
		return fmt.Errorf("read stdin: %w", err)
	}
	var spec Spec
	if err := json.Unmarshal(raw, &spec); err != nil {
		return fmt.Errorf("parse spec: %w", err)
	}
	if spec.PieceLength <= 0 {
		return errors.New("piece_length must be > 0")
	}
	if len(spec.Files) == 0 {
		return errors.New("no files")
	}
	if spec.NumWorkers <= 0 {
		spec.NumWorkers = 8
	}
	if spec.ChunkSize <= 0 {
		spec.ChunkSize = 1 << 20 // 1 MiB
	}
	if spec.MaxRetries <= 0 {
		spec.MaxRetries = 10
	}

	// One shared http.Client with a generous connection pool.
	transport := &http.Transport{
		MaxIdleConns:        spec.NumWorkers * 2,
		MaxIdleConnsPerHost: spec.NumWorkers * 2,
		MaxConnsPerHost:     spec.NumWorkers,
		IdleConnTimeout:     90 * time.Second,
		ForceAttemptHTTP2:   true,
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   0, // no overall timeout; we manage per-read via retries
	}

	results := make([]Result, len(spec.Files))
	progress := make(chan ProgressLine, spec.NumWorkers*4)
	progressWg := &sync.WaitGroup{}
	progressWg.Add(1)
	go func() {
		defer progressWg.Done()
		w := bufio.NewWriter(os.Stderr)
		defer w.Flush()
		for p := range progress {
			b, _ := json.Marshal(p)
			w.Write(b)
			w.WriteByte('\n')
		}
	}()

	// Aggregate bytes-hashed counter for top-level progress display.
	var totalHashed atomic.Int64
	totalSize := int64(0)
	for _, f := range spec.Files {
		totalSize += f.Size + f.PadSize
	}

	// Periodic top-level progress ticker.
	stopTick := make(chan struct{})
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		start := time.Now()
		last := int64(0)
		lastT := start
		for {
			select {
			case <-ticker.C:
				cur := totalHashed.Load()
				now := time.Now()
				dt := now.Sub(lastT).Seconds()
				rate := float64(0)
				if dt > 0 {
					rate = float64(cur-last) / dt
				}
				progress <- ProgressLine{
					Type:    "progress",
					Bytes:   cur,
					Hashed:  cur,
					Elapsed: now.Sub(start).Truncate(time.Second).String(),
					Rate:    fmt.Sprintf("%.1f MB/s", rate/1e6),
				}
				last = cur
				lastT = now
			case <-stopTick:
				return
			}
		}
	}()

	start := time.Now()

	// Worker pool: one goroutine per file, bounded by a semaphore.
	sem := make(chan struct{}, spec.NumWorkers)
	var wg sync.WaitGroup
	for i, f := range spec.Files {
		wg.Add(1)
		go func(idx int, file FileSpec) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			pieces, hashed, err := streamAndHash(file, spec, client, &totalHashed)
			if err != nil {
				progress <- ProgressLine{Type: "file_err", Index: idx, Msg: err.Error()}
			} else {
				progress <- ProgressLine{Type: "file_done", Index: idx, Bytes: hashed}
			}
			results[idx] = Result{Index: idx, Pieces: pieces, Hashed: hashed, Err: err}
		}(i, f)
	}
	wg.Wait()
	close(stopTick)
	close(progress)
	progressWg.Wait()

	// Sanity check: every file produced pieces.
	for i, r := range results {
		if r.Err != nil {
			return fmt.Errorf("file index %d failed: %w", i, r.Err)
		}
		if len(r.Pieces) == 0 {
			return fmt.Errorf("file index %d produced no pieces", i)
		}
	}

	// Write all pieces to stdout, in file order.
	w := bufio.NewWriterSize(os.Stdout, 1<<20)
	for _, r := range results {
		if _, err := w.Write(r.Pieces); err != nil {
			return fmt.Errorf("write stdout: %w", err)
		}
	}
	if err := w.Flush(); err != nil {
		return fmt.Errorf("flush stdout: %w", err)
	}

	elapsed := time.Since(start).Truncate(time.Millisecond)
	final := totalHashed.Load()
	rate := float64(0)
	if elapsed.Seconds() > 0 {
		rate = float64(final) / elapsed.Seconds()
	}
	fmt.Fprintf(os.Stderr, `{"type":"done","bytes":%d,"hashed":%d,"elapsed":%q,"rate":%q}`+"\n",
		final, final, elapsed.String(), fmt.Sprintf("%.1f MB/s", rate/1e6))
	return nil
}

// streamAndHash streams one file over HTTP, feeding bytes into a rolling
// piece-buffer. On connection drop, retries with a Range header from the
// current offset. After the stream ends, appends virtual pad zeros and
// flushes the final (partial) piece.
//
// Concurrency: one goroutine per file, bounded by the semaphore in run().
// Each goroutine shares the single http.Client (which pools connections
// per host up to NumWorkers) but maintains its own offset, buffer and
// pieces slice, so no mutex is needed on the per-file state.
//
// Resume: if the response body read fails mid-stream (network drop, RST,
// server-side abort), the loop re-issues a GET with a "Range: bytes=N-"
// header from the current offset, up to MaxRetries times, with an
// exponential backoff capped at 30s. The HF token is re-sent on every
// attempt. If an OutPath is set, the output file is seeked to the resume
// offset before continuing the write, so the on-disk copy stays consistent
// with what was hashed.
//
// Padding: BitTorrent BEP-5 allows the last piece of the last file (or the
// last piece overall for multi-file torrents built with name=<repo>) to be
// padded with zeros to a full piece. The parent computes PadSize and we
// append that many zero bytes to the buffer before flushing the final
// partial piece, so the digest matches what a standard BT client would
// expect for the padded torrent.
//
// Returns the concatenated 20-byte digests, the count of real (non-pad)
// bytes hashed, and any error. On error the pieces slice is nil and must
// not be used by the caller.
func streamAndHash(file FileSpec, spec Spec, client *http.Client, totalHashed *atomic.Int64) ([]byte, int64, error) {
	var pieces []byte
	var buf bytes.Buffer
	var hashed int64
	var offset int64

	chunk := make([]byte, spec.ChunkSize)

	// Optional disk writer: opened once, seeks on resume.
	var outFile *os.File
	if file.OutPath != "" {
		if err := os.MkdirAll(filepath.Dir(file.OutPath), 0o755); err != nil {
			return nil, hashed, fmt.Errorf("mkdir for %s: %w", file.OutPath, err)
		}
		f, err := os.OpenFile(file.OutPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
		if err != nil {
			return nil, hashed, fmt.Errorf("create %s: %w", file.OutPath, err)
		}
		// Preallocate for contiguous layout (sparse on SSD, contiguity hint on HDD).
		if file.Size > 0 {
			_ = f.Truncate(file.Size)
		}
		outFile = f
		defer outFile.Close()
	}

	flushPieces := func() {
		for buf.Len() >= spec.PieceLength {
			piece := buf.Next(spec.PieceLength)
			h := sha1.Sum(piece)
			pieces = append(pieces, h[:]...)
		}
	}

	for attempt := 0; attempt <= spec.MaxRetries; attempt++ {
		req, err := http.NewRequest("GET", file.URL, nil)
		if err != nil {
			return nil, hashed, fmt.Errorf("build request: %w", err)
		}
		req.Header.Set("User-Agent", "hf-piece-hasher/1.0")
		if spec.HFToken != "" {
			req.Header.Set("Authorization", "Bearer "+spec.HFToken)
		}
		if offset > 0 {
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
		}

		resp, err := client.Do(req)
		if err != nil {
			if attempt == spec.MaxRetries {
				return nil, hashed, fmt.Errorf("http (attempt %d): %w", attempt, err)
			}
			backoff := time.Duration(1<<uint(attempt)) * time.Second
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
			time.Sleep(backoff)
			continue
		}
		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
			resp.Body.Close()
			if attempt == spec.MaxRetries {
				return nil, hashed, fmt.Errorf("http status %d for %s", resp.StatusCode, file.URL)
			}
			time.Sleep(time.Duration(1<<uint(attempt)) * time.Second)
			continue
		}

		// On resume, seek the output file to the current offset.
		if outFile != nil && offset > 0 {
			if _, err := outFile.Seek(offset, 0); err != nil {
				resp.Body.Close()
				return nil, hashed, fmt.Errorf("seek output to %d: %w", offset, err)
			}
		}

		// Read loop.
		streamErr := error(nil)
		for {
			n, err := resp.Body.Read(chunk)
			if n > 0 {
				buf.Write(chunk[:n])
				if outFile != nil {
					if _, werr := outFile.Write(chunk[:n]); werr != nil {
						resp.Body.Close()
						return nil, hashed, fmt.Errorf("write %s: %w", file.OutPath, werr)
					}
				}
				hashed += int64(n)
				offset += int64(n)
				totalHashed.Add(int64(n))
				flushPieces()
			}
			if err != nil {
				if !errors.Is(err, io.EOF) {
					streamErr = err
				}
				break
			}
		}
		resp.Body.Close()

		// If we've read the whole file, we're done.
		if offset >= file.Size && streamErr == nil {
			break
		}
		// If stream broke before EOF, retry from current offset.
		if streamErr != nil && attempt == spec.MaxRetries {
			return nil, hashed, fmt.Errorf("stream after %d bytes: %w", offset, streamErr)
		}
		// Brief backoff before retry.
		if streamErr != nil {
			time.Sleep(time.Duration(1<<uint(attempt)) * time.Second)
		}
	}

	// Append virtual BEP-5 pad (zero bytes) and flush remaining pieces.
	if file.PadSize > 0 {
		buf.Write(bytes.Repeat([]byte{0}, int(file.PadSize)))
		flushPieces()
	}
	if buf.Len() > 0 {
		h := sha1.Sum(buf.Bytes())
		pieces = append(pieces, h[:]...)
	}

	return pieces, hashed, nil
}

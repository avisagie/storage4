package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"storage4/catalogue"
	"storage4/format"
	"storage4/names"
)

// maxRequestBytes bounds one buffered request (file, metadata, multipart
// framing).
const maxRequestBytes = MaxFileSize + MaxMetadataSize + 1<<20

var (
	errBadRequest  = errors.New("bad request")
	errTooLarge    = errors.New("too large")
	errUnsupported = errors.New("unsupported media type")
	errBudget      = errors.New("memory budget full")
)

// Handler returns the node's HTTP API (docs/api.md). The host may mount it,
// or Config.ListenAddr makes the node serve it.
func (n *Node) Handler() http.Handler { return http.HandlerFunc(n.serve) }

func (n *Node) serve(w http.ResponseWriter, r *http.Request) {
	n.reqMu.RLock()
	if n.stopping {
		n.reqMu.RUnlock()
		retryLater(w, http.StatusServiceUnavailable, "shutting down")
		return
	}
	n.inflight.Add(1)
	n.reqMu.RUnlock()
	defer n.inflight.Done()

	if n.cfg.Token != "" && r.Header.Get("Authorization") != "Bearer "+n.cfg.Token {
		plain(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if r.URL.RawQuery != "" {
		plain(w, http.StatusBadRequest, "query strings are not accepted")
		return
	}
	kind, bucket, name, hasName, ok := parsePath(r.URL.EscapedPath())
	if !ok {
		plain(w, http.StatusBadRequest, "invalid path")
		return
	}
	if err := names.ValidateBucket(bucket); err != nil {
		plain(w, http.StatusBadRequest, "invalid bucket name")
		return
	}
	if hasName {
		if err := names.ValidateName(name); err != nil {
			plain(w, http.StatusBadRequest, "invalid name")
			return
		}
	}
	ctx := r.Context()
	switch {
	case kind == 'b' && !hasName:
		switch r.Method {
		case http.MethodPut:
			n.createBucket(ctx, w, r, bucket)
		case http.MethodDelete:
			n.deleteBucket(ctx, w, bucket)
		default:
			plain(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	case kind == 'b':
		switch r.Method {
		case http.MethodPut:
			n.handlePut(ctx, w, r, bucket, name)
		case http.MethodGet, http.MethodHead:
			n.handleGet(ctx, w, r, bucket, name)
		case http.MethodDelete:
			// Name erasure arrives with milestone 8.
			plain(w, http.StatusNotImplemented, "erasure is not implemented yet")
		default:
			plain(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	default: // 'm'
		if !hasName {
			plain(w, http.StatusBadRequest, "invalid path")
			return
		}
		if r.Method != http.MethodGet {
			plain(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		n.handleMeta(ctx, w, bucket, name)
	}
}

// parsePath splits /b/{bucket}[/{name...}] and /m/{bucket}/{name...}.
// Percent-escapes are decoded; an escaped "/" (%2F) is refused so a name's
// segments are unambiguous.
func parsePath(esc string) (kind byte, bucket, name string, hasName, ok bool) {
	if len(esc) < 4 || esc[0] != '/' || (esc[1] != 'b' && esc[1] != 'm') || esc[2] != '/' {
		return 0, "", "", false, false
	}
	kind = esc[1]
	rest := esc[3:]
	if strings.Contains(strings.ToLower(rest), "%2f") {
		return 0, "", "", false, false
	}
	b, nm, has := strings.Cut(rest, "/")
	bu, err1 := url.PathUnescape(b)
	na, err2 := url.PathUnescape(nm)
	if err1 != nil || err2 != nil {
		return 0, "", "", false, false
	}
	if has && nm == "" {
		return 0, "", "", false, false // trailing slash
	}
	return kind, bu, na, has, true
}

func plain(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(code)
	io.WriteString(w, msg+"\n")
}

func retryLater(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Retry-After", "1")
	plain(w, code, msg)
}

// fail maps an error to a status. Messages never carry names, metadata or
// internal identifiers.
func (n *Node) fail(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, errBadRequest):
		plain(w, http.StatusBadRequest, "bad request")
	case errors.Is(err, errTooLarge):
		plain(w, http.StatusRequestEntityTooLarge, "file or metadata too large")
	case errors.Is(err, errUnsupported):
		plain(w, http.StatusUnsupportedMediaType, "unsupported content type")
	case errors.Is(err, errPrecondition):
		plain(w, http.StatusPreconditionFailed, "precondition failed")
	case errors.Is(err, catalogue.ErrBucketDeleted), errors.Is(err, errNameErased):
		plain(w, http.StatusGone, "gone")
	case errors.Is(err, catalogue.ErrBucketNotFound), errors.Is(err, errNoSuchName):
		plain(w, http.StatusNotFound, "not found")
	case errors.Is(err, catalogue.ErrBucketConflict):
		plain(w, http.StatusConflict, "bucket exists with different options")
	case errors.Is(err, errBudget):
		n.d.Alarms.Raise(AlarmMemoryBudget, Warning, "request buffer budget exhausted")
		retryLater(w, http.StatusServiceUnavailable, "busy")
	case errors.Is(err, catalogue.ErrUnavailable), errors.Is(err, catalogue.ErrLeaseHeld),
		errors.Is(err, catalogue.ErrLeaseLost), errors.Is(err, errNoReplicaSet),
		errors.Is(err, errLeaseUnavailable), errors.Is(err, errTooManyOpen),
		errors.Is(err, errRetriesExhausted), errors.Is(err, errLookupUncertain),
		errors.Is(err, errCounterUnavailable), errors.Is(err, context.Canceled):
		retryLater(w, http.StatusServiceUnavailable, "unavailable")
	default:
		n.d.Logger.Error("request failed", "err", err.Error())
		plain(w, http.StatusInternalServerError, "internal error")
	}
}

func etagOf(sha [32]byte) string { return `"` + hex.EncodeToString(sha[:]) + `"` }

func setDigestHeaders(h http.Header, sha [32]byte) {
	h.Set("ETag", etagOf(sha))
	h.Set("Repr-Digest", "sha-256=:"+base64.StdEncoding.EncodeToString(sha[:])+":")
}

// ---- buckets ----

func (n *Node) createBucket(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket string) {
	shards := n.cfg.DefaultShards
	body, err := io.ReadAll(io.LimitReader(r.Body, 4097))
	if err != nil || len(body) > 4096 {
		n.fail(w, errBadRequest)
		return
	}
	if len(bytes.TrimSpace(body)) > 0 {
		var opt struct {
			Shards *int `json:"shards"`
		}
		dec := json.NewDecoder(bytes.NewReader(body))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&opt); err != nil || dec.More() {
			n.fail(w, errBadRequest)
			return
		}
		if opt.Shards != nil {
			shards = *opt.Shards
		}
	}
	if err := names.ValidateShardCount(uint32(max(shards, 0))); err != nil {
		n.fail(w, errBadRequest)
		return
	}
	_, created, err := n.d.Catalogue.CreateBucket(ctx, bucket, shards)
	if err != nil {
		n.fail(w, err)
		return
	}
	if created {
		w.WriteHeader(http.StatusCreated)
	} else {
		w.WriteHeader(http.StatusOK)
	}
}

func (n *Node) deleteBucket(ctx context.Context, w http.ResponseWriter, bucket string) {
	err := n.d.Catalogue.DeleteBucket(ctx, bucket)
	n.forgetBucket(bucket)
	n.abandonBucket(bucket)
	if err != nil {
		n.fail(w, err)
		return
	}
	n.cnt.bucketDeletes.Add(1)
	w.WriteHeader(http.StatusAccepted)
}

// ---- PUT ----

func (n *Node) handlePut(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, name string) {
	var req putRequest
	if err := parseConditions(r, &req); err != nil {
		n.cnt.putsRejected.Add(1)
		n.fail(w, err)
		return
	}
	wantDigest, err := parseContentDigest(r.Header.Get("Content-Digest"))
	if err != nil {
		n.cnt.putsRejected.Add(1)
		n.fail(w, err)
		return
	}
	mt, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	multipartBody := false
	switch {
	case r.Header.Get("Content-Type") == "" || (err == nil && mt == "application/octet-stream"):
	case err == nil && mt == "multipart/form-data" && params["boundary"] != "":
		multipartBody = true
	default:
		n.cnt.putsRejected.Add(1)
		n.fail(w, errUnsupported)
		return
	}
	// Reserve buffer space before reading the body; never queue.
	limit := int64(MaxFileSize)
	if multipartBody {
		limit = maxRequestBytes
	}
	reserve := limit
	if cl := r.ContentLength; cl > limit {
		n.cnt.putsRejected.Add(1)
		n.fail(w, errTooLarge)
		return
	} else if cl >= 0 {
		reserve = cl
	}
	if !n.budget.tryAcquire(reserve) {
		n.cnt.putsRejected.Add(1)
		n.fail(w, errBudget)
		return
	}
	defer n.budget.release(reserve)
	n.d.Alarms.Clear(AlarmMemoryBudget)

	if multipartBody {
		req.meta, req.payload, err = readMultipart(r.Body, params["boundary"])
	} else {
		req.payload, err = readLimited(r.Body, MaxFileSize, reserve)
	}
	if err != nil {
		n.cnt.putsRejected.Add(1)
		n.fail(w, err)
		return
	}
	req.sha = sha256.Sum256(req.payload)
	if wantDigest != nil && *wantDigest != req.sha {
		n.cnt.putsRejected.Add(1)
		n.fail(w, errBadRequest)
		return
	}
	b, err := n.bucket(ctx, bucket, true)
	if err == nil && b.State == catalogue.BucketDeleted {
		err = catalogue.ErrBucketDeleted
	}
	if err != nil {
		n.cnt.putsRejected.Add(1)
		n.fail(w, err)
		return
	}
	n.cnt.bytesIn.Add(uint64(len(req.payload)))
	stored, err := n.put(ctx, b, name, req)
	if err != nil {
		n.cnt.putsRejected.Add(1)
		n.fail(w, err)
		return
	}
	setDigestHeaders(w.Header(), req.sha)
	if stored {
		n.cnt.putsStored.Add(1)
		w.WriteHeader(http.StatusCreated)
	} else {
		n.cnt.putsDeduplicated.Add(1)
		w.WriteHeader(http.StatusOK)
	}
}

// readLimited reads a body of at most max bytes, sized up front when the
// length is known.
func readLimited(body io.Reader, max int64, sizeHint int64) ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0, min(sizeHint, max)+1))
	n, err := io.Copy(buf, io.LimitReader(body, max+1))
	if err != nil {
		return nil, errBadRequest
	}
	if n > max {
		return nil, errTooLarge
	}
	return buf.Bytes(), nil
}

// readMultipart parses a multipart PUT: an optional "metadata" part first,
// then exactly one "file" part, nothing else.
func readMultipart(body io.Reader, boundary string) (meta, file []byte, err error) {
	mr := multipart.NewReader(body, boundary)
	haveFile := false
	for first := true; ; first = false {
		p, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, errBadRequest
		}
		switch p.FormName() {
		case "metadata":
			if !first {
				return nil, nil, errBadRequest // after the file, or repeated
			}
			if meta, err = readLimited(p, MaxMetadataSize, 0); err != nil {
				return nil, nil, err
			}
		case "file":
			if haveFile {
				return nil, nil, errBadRequest
			}
			haveFile = true
			if file, err = readLimited(p, MaxFileSize, 0); err != nil {
				return nil, nil, err
			}
		default:
			return nil, nil, errBadRequest
		}
	}
	if !haveFile {
		return nil, nil, errBadRequest
	}
	if len(meta) == 0 {
		meta = nil
	}
	return meta, file, nil
}

// parseConditions reads If-None-Match: * and If-Match.
func parseConditions(r *http.Request, req *putRequest) error {
	inm, im := r.Header.Get("If-None-Match"), r.Header.Get("If-Match")
	if inm != "" && im != "" {
		return errBadRequest
	}
	if inm != "" {
		if strings.TrimSpace(inm) != "*" {
			return errBadRequest
		}
		req.ifNoneMatchA = true
	}
	if im != "" {
		if strings.TrimSpace(im) == "*" {
			req.ifMatchAny = true
			return nil
		}
		hexs, ok := parseETag(im)
		if !ok {
			return errBadRequest
		}
		req.ifMatch = hexs
	}
	return nil
}

// parseETag accepts one strong ETag "<64 hex>".
func parseETag(s string) (string, bool) {
	s = strings.TrimSpace(s)
	if len(s) != 66 || s[0] != '"' || s[65] != '"' {
		return "", false
	}
	h := s[1:65]
	if b, err := hex.DecodeString(h); err != nil || len(b) != 32 || strings.ToLower(h) != h {
		return "", false
	}
	return h, true
}

// parseContentDigest reads the sha-256 member of Content-Digest (RFC 9530);
// other algorithms are ignored. nil means none was sent.
func parseContentDigest(v string) (*[32]byte, error) {
	for _, item := range strings.Split(v, ",") {
		item = strings.TrimSpace(item)
		val, ok := strings.CutPrefix(item, "sha-256=")
		if !ok {
			continue
		}
		if len(val) < 2 || val[0] != ':' || val[len(val)-1] != ':' {
			return nil, errBadRequest
		}
		raw, err := base64.StdEncoding.DecodeString(val[1 : len(val)-1])
		if err != nil || len(raw) != 32 {
			return nil, errBadRequest
		}
		return (*[32]byte)(raw), nil
	}
	return nil, nil
}

// ---- GET, HEAD, metadata ----

// lookupName finds the latest version of a name on its shard.
func (n *Node) lookupName(ctx context.Context, bucket, name string, excl exclusions) (*found, error) {
	b, err := n.bucket(ctx, bucket, false)
	if err != nil {
		return nil, err
	}
	if b.State == catalogue.BucketDeleted {
		return nil, catalogue.ErrBucketDeleted
	}
	idx, err := names.Shard(name, uint32(b.Shards))
	if err != nil {
		return nil, err
	}
	f, uncertain, err := n.shardOf(b, int(idx)).lookupExcluding(ctx, name, excl)
	if uncertain {
		return nil, errLookupUncertain
	}
	return f, err
}

func (n *Node) handleGet(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, name string) {
	excl := exclusions{}
	for {
		f, err := n.lookupName(ctx, bucket, name, excl)
		if err != nil {
			n.fail(w, err)
			return
		}
		done, bad := n.serveGetReleasing(w, r, f)
		if done {
			return
		}
		// The copy failed verification before any byte was sent: try another
		// replica (docs/api.md §2.2).
		n.d.Alarms.Raise(AlarmReplicaUnavailable, Warning, "a replica returned a corrupt record")
		excl[exclKey{bad, f.vol}] = true
	}
}

// serveGetReleasing is serveGet that releases the record even when the
// transfer aborts by panicking with http.ErrAbortHandler.
func (n *Node) serveGetReleasing(w http.ResponseWriter, r *http.Request, f *found) (bool, *localDisk) {
	defer f.release()
	return n.serveGet(w, r, f)
}

// serveGet sends the record. done is false if the payload failed verification
// before anything was sent; bad is then the disk to avoid.
func (n *Node) serveGet(w http.ResponseWriter, r *http.Request, f *found) (done bool, bad *localDisk) {
	h := f.head
	setDigestHeaders(w.Header(), h.SHA256)
	if inm := r.Header.Get("If-None-Match"); inm != "" {
		for _, c := range strings.Split(inm, ",") {
			if c = strings.TrimSpace(c); c == "*" || c == etagOf(h.SHA256) {
				w.WriteHeader(http.StatusNotModified)
				return true, nil
			}
		}
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(h.PayloadLen))
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return true, nil
	}
	body := h.Payload()
	// The payload CRC is only known after the last byte is read. Every chunk
	// is therefore sent only after the read that follows it has succeeded, so
	// the final chunk leaves after the CRC verified; aborting before it is
	// sent leaves the client with a short body it can detect. A small file
	// (one chunk) is verified completely before the first byte leaves.
	cur, nxt := make([]byte, 64<<10), make([]byte, 64<<10)
	k, err := io.ReadFull(body, cur)
	if !readOK(err) {
		if errors.Is(err, format.ErrCorrupt) {
			return false, f.disk
		}
		n.d.Logger.Error("reading a payload failed before sending")
		plain(w, http.StatusInternalServerError, "internal error")
		return true, nil
	}
	n.cnt.gets.Add(1)
	w.WriteHeader(http.StatusOK)
	for err == nil { // cur is a full chunk; more may follow
		k2, err2 := io.ReadFull(body, nxt)
		if !readOK(err2) {
			panic(http.ErrAbortHandler) // cur is unsent: the body stays short
		}
		if _, werr := w.Write(cur[:k]); werr != nil {
			return true, nil
		}
		n.cnt.bytesOut.Add(uint64(k))
		cur, nxt, k, err = nxt, cur, k2, err2
	}
	if _, werr := w.Write(cur[:k]); werr == nil {
		n.cnt.bytesOut.Add(uint64(k))
	}
	return true, nil
}

// readOK reports whether an io.ReadFull result is a clean read: a full
// buffer, or the verified end of the payload.
func readOK(err error) bool {
	return err == nil || err == io.ErrUnexpectedEOF || err == io.EOF
}

func (n *Node) handleMeta(ctx context.Context, w http.ResponseWriter, bucket, name string) {
	f, err := n.lookupName(ctx, bucket, name, nil)
	if err != nil {
		n.fail(w, err)
		return
	}
	defer f.release()
	if len(f.head.Metadata) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(f.head.Metadata)))
	w.WriteHeader(http.StatusOK)
	w.Write(f.head.Metadata)
}

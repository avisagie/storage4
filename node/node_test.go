package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand/v2"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"testing"
	"time"

	"storage4/catalogue"
	"storage4/catalogue/mem"
	"storage4/clock"
	"storage4/db"
	"storage4/db/dbtest"
	"storage4/disk"
	"storage4/format"
	"storage4/volume"
)

// rig is one node on in-memory disks with the in-memory catalogue.
type rig struct {
	t     *testing.T
	clk   *clock.Fake
	cat   catalogue.Catalogue
	fs    []*disk.Mem
	alarm *RecordingAlarms
	n     *Node
	srv   *httptest.Server
	gen   int
	cfg   Config
	fss   []disk.FS // what the node sees; the Mem itself unless wrapped
}

// backend selects the catalogue implementation under test; runBackends runs
// a test against all of them.
var backend = "mem"

func newCatalogue(t *testing.T, clk clock.Clock) catalogue.Catalogue {
	t.Helper()
	o := catalogue.Options{NodeTTL: 30 * time.Second}
	switch backend {
	case "sqlite":
		return dbtest.Catalogue(t, db.SQLite, clk, testMeta, o)
	case "postgres":
		return dbtest.Catalogue(t, db.Postgres, clk, testMeta, o)
	}
	return mem.New(clk, testMeta, o)
}

var testMeta = catalogue.Meta{ClusterID: [16]byte{9, 9}, ClusterName: "t", SchemaVersion: 1, MinCompat: 1, Incarnation: 1}

func newRig(t *testing.T, disks int) *rig {
	t.Helper()
	return newRigWith(t, disks, Config{}, nil)
}

// newRigWith starts a node; wrap may replace a disk's FS (fault injection).
func newRigWith(t *testing.T, disks int, cfg Config, wrap func(i int, m *disk.Mem) disk.FS) *rig {
	t.Helper()
	cfg.NodeName = "n1"
	clk := clock.NewFake(time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC))
	r := &rig{t: t, clk: clk, alarm: &RecordingAlarms{}, cfg: cfg,
		cat: newCatalogue(t, clk)}
	ctx := context.Background()
	nid, err := r.cat.AssignNodeID(ctx, "n1")
	if err != nil {
		t.Fatal(err)
	}
	for i := range disks {
		m := disk.NewMem(disk.MemOptions{})
		r.fs = append(r.fs, m)
		if wrap != nil {
			r.fss = append(r.fss, wrap(i, m))
		} else {
			r.fss = append(r.fss, m)
		}
		id := disk.Identity{ClusterID: testMeta.ClusterID, NodeID: uint16(nid), DiskID: [16]byte{byte(i + 1)}}
		if err := disk.WriteIdentity(m, id); err != nil {
			t.Fatal(err)
		}
	}
	r.start()
	t.Cleanup(r.stop)
	return r
}

func (r *rig) deps() Deps {
	r.gen++
	seed := [32]byte{byte(r.gen)}
	return Deps{Clock: r.clk, Rand: rand.NewChaCha8(seed), Disks: r.fss, Catalogue: r.cat, Alarms: r.alarm}
}

func (r *rig) start() {
	r.t.Helper()
	n, err := Start(context.Background(), r.cfg, r.deps())
	if err != nil {
		r.t.Fatal(err)
	}
	r.n = n
	r.srv = httptest.NewServer(n.Handler())
}

func (r *rig) stop() {
	if r.n == nil {
		return
	}
	r.srv.Close()
	if err := r.n.Stop(context.Background()); err != nil {
		r.t.Errorf("stop: %v", err)
	}
	r.n = nil
}

func (r *rig) restart() {
	r.stop()
	r.start()
}

type resp struct {
	code int
	h    http.Header
	body []byte
}

func (r *rig) do(method, path string, body []byte, hdr ...string) resp {
	r.t.Helper()
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, _ := http.NewRequest(method, r.srv.URL+path, rd)
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		r.t.Fatal(err)
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	return resp{res.StatusCode, res.Header, b}
}

func (r *rig) put(path string, body []byte, hdr ...string) resp {
	return r.do(http.MethodPut, path, body, hdr...)
}

func (r *rig) putMeta(path string, meta, file []byte, hdr ...string) resp {
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	if meta != nil {
		fw, _ := mw.CreateFormField("metadata")
		fw.Write(meta)
	}
	fw, _ := mw.CreateFormFile("file", "f")
	fw.Write(file)
	mw.Close()
	return r.do(http.MethodPut, path, buf.Bytes(), append(hdr, "Content-Type", mw.FormDataContentType())...)
}

func etag(b []byte) string { s := sha256.Sum256(b); return `"` + hex.EncodeToString(s[:]) + `"` }

func (r *rig) want(res resp, code int) {
	r.t.Helper()
	if res.code != code {
		r.t.Fatalf("status %d (%q), want %d", res.code, res.body, code)
	}
}

func testPutGetHead(t *testing.T) {
	r := newRig(t, 3)
	data := []byte("hello storage")
	res := r.put("/b/docs/a/b:c", data)
	r.want(res, 201)
	if res.h.Get("ETag") != etag(data) {
		t.Fatalf("etag %q", res.h.Get("ETag"))
	}
	sum := sha256.Sum256(data)
	if res.h.Get("Repr-Digest") != "sha-256=:"+base64.StdEncoding.EncodeToString(sum[:])+":" {
		t.Fatalf("repr-digest %q", res.h.Get("Repr-Digest"))
	}
	r.want(r.put("/b/docs/a/b:c", data), 200) // identical to latest: nothing stored
	g := r.do("GET", "/b/docs/a/b:c", nil)
	r.want(g, 200)
	if !bytes.Equal(g.body, data) || g.h.Get("ETag") != etag(data) {
		t.Fatalf("get: %q %v", g.body, g.h)
	}
	h := r.do("HEAD", "/b/docs/a/b:c", nil)
	r.want(h, 200)
	if h.h.Get("Content-Length") != fmt.Sprint(len(data)) || len(h.body) != 0 {
		t.Fatalf("head: %v %q", h.h, h.body)
	}
	r.want(r.do("GET", "/b/docs/a/b:c", nil, "If-None-Match", etag(data)), 304)
	r.want(r.do("GET", "/b/docs/missing", nil), 404)
	r.want(r.do("GET", "/b/nobucket/x", nil), 404)
	r.want(r.do("GET", "/m/docs/a/b:c", nil), 204)
	snap := r.n.Metrics().Snapshot()
	if snap.Counters["puts_stored"] != 1 || snap.Counters["puts_deduplicated"] != 1 {
		t.Fatalf("metrics %v", snap.Counters)
	}
}

func testMetadataIsPartOfDedup(t *testing.T) {
	r := newRig(t, 3)
	file := []byte("same bytes")
	r.want(r.putMeta("/b/docs/n", []byte(`{"a":1}`), file), 201)
	r.want(r.putMeta("/b/docs/n", []byte(`{"a":1}`), file), 200) // identical content and metadata
	r.want(r.putMeta("/b/docs/n", []byte(`{"a":2}`), file), 201) // metadata change = new version
	m := r.do("GET", "/m/docs/n", nil)
	r.want(m, 200)
	if string(m.body) != `{"a":2}` {
		t.Fatalf("metadata %q", m.body)
	}
	r.want(r.put("/b/docs/n", file), 201) // dropping the metadata is a change too
	r.want(r.do("GET", "/m/docs/n", nil), 204)
	r.want(r.put("/b/docs/n", file), 200)
	// A → B → A stores A again.
	r.want(r.put("/b/docs/n", []byte("other")), 201)
	r.want(r.put("/b/docs/n", file), 201)
}

func testConditionsAndDigests(t *testing.T) {
	r := newRig(t, 3)
	a, b := []byte("aaa"), []byte("bbb")
	r.want(r.put("/b/c/x", a, "If-None-Match", "*"), 201)
	r.want(r.put("/b/c/x", a, "If-None-Match", "*"), 412)
	r.want(r.put("/b/c/x", b, "If-Match", etag(b)), 412)
	r.want(r.put("/b/c/x", b, "If-Match", etag(a)), 201)
	r.want(r.put("/b/c/new", b, "If-Match", etag(b)), 412) // no version
	r.want(r.put("/b/c/x", b, "If-None-Match", "*", "If-Match", etag(b)), 400)
	sum := sha256.Sum256(b)
	good := "sha-256=:" + base64.StdEncoding.EncodeToString(sum[:]) + ":"
	r.want(r.put("/b/c/y", b, "Content-Digest", good), 201)
	other := sha256.Sum256([]byte("zzz"))
	r.want(r.put("/b/c/z", b, "Content-Digest", "sha-256=:"+base64.StdEncoding.EncodeToString(other[:])+":"), 400)
	r.want(r.do("GET", "/b/c/z", nil), 404) // nothing stored
}

func testValidation(t *testing.T) {
	r := newRig(t, 3)
	r.want(r.put("/b/.bad/x", []byte("x")), 400)
	r.want(r.put("/b/ok/a//b", []byte("x")), 400)
	r.want(r.put("/b/ok/a%2Fb", []byte("x")), 400)
	r.want(r.put("/b/ok/a?x=1", []byte("x")), 400)
	r.want(r.put("/b/ok/x", []byte("x"), "Content-Type", "text/plain"), 415)
	r.want(r.do("PATCH", "/b/ok/x", nil), 405)
	r.want(r.do("DELETE", "/b/ok/x", nil), 501)
	r.want(r.put("/elsewhere", nil), 400)
}

func testBucketLifecycle(t *testing.T) {
	r := newRig(t, 3)
	r.want(r.put("/b/bk", nil), 201)
	r.want(r.put("/b/bk", nil), 200)
	r.want(r.put("/b/bk", []byte(`{"shards":8}`)), 409)
	r.want(r.put("/b/bk2", []byte(`{"shards":8,"x":1}`)), 400)
	r.want(r.put("/b/bk/f", []byte("data")), 201)
	r.want(r.do("DELETE", "/b/bk", nil), 202)
	r.want(r.do("DELETE", "/b/bk", nil), 202)
	r.want(r.put("/b/bk/f", []byte("data2")), 410)
	r.want(r.do("GET", "/b/bk/f", nil), 410)
	r.want(r.put("/b/bk", nil), 410)
}

func testConcurrentIdenticalPutsStoreOneVersion(t *testing.T) {
	r := newRig(t, 3)
	var wg sync.WaitGroup
	codes := make(chan int, 16)
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			codes <- r.put("/b/c/race", []byte("identical")).code
		}()
	}
	wg.Wait()
	close(codes)
	stored := 0
	for c := range codes {
		if c == 201 {
			stored++
		} else if c != 200 {
			t.Fatalf("status %d", c)
		}
	}
	if stored != 1 {
		t.Fatalf("%d PUTs stored a version, want exactly 1", stored)
	}
}

func testRestartSealsAndKeepsData(t *testing.T) {
	r := newRig(t, 3)
	for i := range 20 {
		r.want(r.put(fmt.Sprintf("/b/c/n%d", i), []byte(fmt.Sprint("v", i))), 201)
	}
	r.want(r.put("/b/c/n3", []byte("v3 again")), 201)
	r.restart()
	for i := range 20 {
		g := r.do("GET", fmt.Sprintf("/b/c/n%d", i), nil)
		r.want(g, 200)
		want := fmt.Sprint("v", i)
		if i == 3 {
			want = "v3 again"
		}
		if string(g.body) != want {
			t.Fatalf("n%d = %q, want %q", i, g.body, want)
		}
	}
	vols, err := r.cat.ListShardVolumes(context.Background(), "c", 0)
	_ = err
	for s := range 4 {
		vs, _ := r.cat.ListShardVolumes(context.Background(), "c", s)
		for _, v := range vs {
			if v.State != catalogue.VolumeSealed {
				t.Fatalf("volume %v is %s after restart", v.ID, v.State)
			}
		}
	}
	_ = vols
	// New writes go to a new volume with a higher volume number and win.
	r.want(r.put("/b/c/n3", []byte("v3 third")), 201)
	if g := r.do("GET", "/b/c/n3", nil); string(g.body) != "v3 third" {
		t.Fatalf("latest = %q", g.body)
	}
}

func testStopSealsAndReleasesLeases(t *testing.T) {
	r := newRig(t, 3)
	r.want(r.put("/b/c/a", []byte("x")), 201)
	idx, _ := shardIndexOf(r, "c", "a")
	r.stop()
	if _, ok, _ := r.cat.ShardOwner(context.Background(), "c", idx); ok {
		t.Fatal("lease still held after Stop")
	}
	vs, _ := r.cat.ListShardVolumes(context.Background(), "c", idx)
	if len(vs) != 1 || vs[0].State != catalogue.VolumeSealed {
		t.Fatalf("volumes after Stop: %+v", vs)
	}
}

func shardIndexOf(r *rig, bucket, name string) (int, error) {
	b, err := r.cat.GetBucket(context.Background(), bucket)
	if err != nil {
		return 0, err
	}
	for s := range b.Shards {
		vs, _ := r.cat.ListShardVolumes(context.Background(), bucket, s)
		if len(vs) > 0 {
			return s, nil
		}
	}
	return 0, fmt.Errorf("no volumes")
}

// kill simulates a crash of the process: nothing is sealed or released.
func (r *rig) kill() {
	r.srv.Close()
	r.n.cancel()
	r.n.wg.Wait()
	for _, ov := range r.n.openVolumes() {
		ov.close()
	}
	r.n = nil
}

func testVolumeRotatesWhenFull(t *testing.T) {
	r := newRigWith(t, 3, Config{VolumeMaxSize: 6 << 10, AllowSmallVolumes: true, DefaultShards: 1}, nil)
	payload := bytes.Repeat([]byte("x"), 1500)
	for i := range 12 {
		r.want(r.put(fmt.Sprintf("/b/c/f%d", i), append([]byte{byte(i)}, payload...)), 201)
	}
	vs, _ := r.cat.ListShardVolumes(context.Background(), "c", 0)
	if len(vs) < 3 {
		t.Fatalf("%d volumes, expected rotation", len(vs))
	}
	sealed := 0
	for _, v := range vs {
		if v.State == catalogue.VolumeSealed {
			sealed++
		}
	}
	if sealed != len(vs)-1 {
		t.Fatalf("%d of %d sealed, want all but the open one", sealed, len(vs))
	}
	// Every file is found across sealed volumes (via .nidx) and the open one.
	for i := range 12 {
		g := r.do("GET", fmt.Sprintf("/b/c/f%d", i), nil)
		r.want(g, 200)
		if g.body[0] != byte(i) || len(g.body) != 1501 {
			t.Fatalf("f%d wrong body", i)
		}
	}
	// A newer version in the open volume beats older sealed ones.
	r.want(r.put("/b/c/f0", []byte("newest")), 201)
	if g := r.do("GET", "/b/c/f0", nil); string(g.body) != "newest" {
		t.Fatalf("latest = %q", g.body)
	}
}

func testReplicaFailureSealsAndRetriesOnNewVolume(t *testing.T) {
	var failing func() bool
	wrap := func(i int, m *disk.Mem) disk.FS {
		if i != 0 {
			return m
		}
		return disk.NewFaulty(m, func() {}, func(op disk.Op) disk.Decision {
			if op.Kind == disk.OpSync && failing != nil && failing() {
				return disk.Decision{Err: disk.ErrIO}
			}
			return disk.Decision{}
		})
	}
	r := newRigWith(t, 4, Config{DefaultShards: 1}, wrap)
	r.want(r.put("/b/c/a", []byte("before")), 201)
	on := true
	failing = func() bool { return on }
	r.want(r.put("/b/c/b", []byte("during")), 201) // retried on a volume without disk 0
	on = false
	r.want(r.put("/b/c/c", []byte("after")), 201)
	for name, want := range map[string]string{"a": "before", "b": "during", "c": "after"} {
		if g := r.do("GET", "/b/c/"+name, nil); g.code != 200 || string(g.body) != want {
			t.Fatalf("%s = %d %q", name, g.code, g.body)
		}
	}
	if r.n.Metrics().Snapshot().Counters["append_retries"] == 0 {
		t.Fatal("no retry recorded")
	}
	if len(r.alarm.Active()) == 0 {
		t.Fatal("no alarm for the failed replica")
	}
	vs, _ := r.cat.ListShardVolumes(context.Background(), "c", 0)
	failed := 0
	for _, v := range vs {
		for _, rep := range v.Replicas {
			if rep.State == catalogue.ReplicaFailed {
				failed++
			}
		}
	}
	if failed == 0 {
		t.Fatal("failed replica not recorded in the catalogue")
	}
}

func TestRequestLimitsAndMultipart(t *testing.T) { eachBackend(t, testRequestLimitsAndMultipart) }

func testNoReplicaSetIs503(t *testing.T) {
	r := newRig(t, 2) // fewer than three disks
	res := r.put("/b/c/a", []byte("x"))
	r.want(res, 503)
	if res.h.Get("Retry-After") == "" {
		t.Fatal("503 without Retry-After")
	}
	r.want(r.do("GET", "/b/c/a", nil), 404)
}

func testMemoryBudget503(t *testing.T) {
	r := newRig(t, 3)
	if !r.n.budget.tryAcquire(r.n.cfg.MemoryBudget) {
		t.Fatal("setup")
	}
	res := r.put("/b/c/a", []byte("x"))
	r.want(res, 503)
	if res.h.Get("Retry-After") == "" {
		t.Fatal("503 without Retry-After")
	}
	r.n.budget.release(r.n.cfg.MemoryBudget)
	r.want(r.put("/b/c/a", []byte("x")), 201)
	if r.n.budget.inUse() != 0 {
		t.Fatalf("budget leaked %d bytes", r.n.budget.inUse())
	}
}

func testCrashRecovery(t *testing.T) {
	for seed := uint64(1); seed <= 8; seed++ {
		t.Run(fmt.Sprint("seed", seed), func(t *testing.T) {
			r := newRigWith(t, 3, Config{DefaultShards: 2}, nil)
			want := map[string]string{}
			for i := range 25 {
				name, val := fmt.Sprintf("n%d", i%9), fmt.Sprintf("value %d", i)
				r.want(r.put("/b/c/"+name, []byte(val)), 201)
				want[name] = val
			}
			r.kill()
			for _, m := range r.fs {
				m.Crash(rand.New(rand.NewPCG(seed, 1)), disk.CrashAny)
			}
			r.clk.Advance(time.Minute)
			r.start()
			for name, val := range want {
				if g := r.do("GET", "/b/c/"+name, nil); g.code != 200 || string(g.body) != val {
					t.Fatalf("%s = %d %q, want %q", name, g.code, g.body, val)
				}
			}
			for s := range 2 {
				vs, _ := r.cat.ListShardVolumes(context.Background(), "c", s)
				for _, v := range vs {
					if v.State != catalogue.VolumeSealed {
						t.Fatalf("volume %v is %s after restart", v.ID, v.State)
					}
				}
			}
			r.want(r.put("/b/c/n0", []byte("post-crash")), 201)
			if g := r.do("GET", "/b/c/n0", nil); string(g.body) != "post-crash" {
				t.Fatalf("latest = %q", g.body)
			}
		})
	}
}

// eventually polls cond, yielding to other goroutines; the test deadline
// bounds it.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for range 2_000_000 {
		if cond() {
			return
		}
		runtime.Gosched()
	}
	t.Fatalf("timed out waiting for %s", what)
}

func testIdleVolumeSealedAndLeaseReleased(t *testing.T) {
	r := newRigWith(t, 3, Config{DefaultShards: 1, VolumeIdle: time.Hour}, nil)
	r.want(r.put("/b/c/a", []byte("x")), 201)
	ctx := context.Background()
	// Let the maintenance loop run through just over an hour, one tick at a
	// time, so the lease is renewed along the way as it would be in life.
	for range 400 {
		eventually(t, "maintenance loop waiting", func() bool { return r.clk.Waiters() >= 1 })
		r.clk.Advance(10 * time.Second)
	}
	eventually(t, "idle seal and lease release", func() bool {
		_, held, _ := r.cat.ShardOwner(ctx, "c", 0)
		vs, _ := r.cat.ListShardVolumes(ctx, "c", 0)
		return !held && len(vs) == 1 && vs[0].State == catalogue.VolumeSealed
	})
	// The shard still serves reads and takes the next write on a new volume.
	r.want(r.do("GET", "/b/c/a", nil), 200)
	r.want(r.put("/b/c/a", []byte("y")), 201)
	if g := r.do("GET", "/b/c/a", nil); string(g.body) != "y" {
		t.Fatalf("latest = %q", g.body)
	}
}

func testStopJoinsGoroutines(t *testing.T) {
	r := newRig(t, 3) // the catalogue's own goroutines (database/sql) live until the test ends
	put := func() {
		rec := httptest.NewRecorder()
		r.n.Handler().ServeHTTP(rec, httptest.NewRequest("PUT", "/b/c/a", bytes.NewReader([]byte("x"))))
		if rec.Code != 201 && rec.Code != 200 {
			t.Fatalf("status %d", rec.Code)
		}
	}
	put()
	r.stop()
	before := runtime.NumGoroutine()
	for range 3 {
		r.start()
		put()
		r.stop()
	}
	eventually(t, "goroutines to exit", func() bool { return runtime.NumGoroutine() <= before })
}

func testTwoNodesInOneProcess(t *testing.T) {
	a, b := newRig(t, 3), newRig(t, 3)
	a.want(a.put("/b/c/x", []byte("from a")), 201)
	b.want(b.put("/b/c/x", []byte("from b")), 201)
	if g := a.do("GET", "/b/c/x", nil); string(g.body) != "from a" {
		t.Fatalf("a = %q", g.body)
	}
	if g := b.do("GET", "/b/c/x", nil); string(g.body) != "from b" {
		t.Fatalf("b = %q", g.body)
	}
}

func TestCorruptReplicaFallsBack(t *testing.T) { eachBackend(t, testCorruptReplicaFallsBack) }

func testRequestLimitsAndMultipart(t *testing.T) {
	r := newRigWith(t, 3, Config{Token: "secret"}, nil)
	auth := []string{"Authorization", "Bearer secret"}
	r.want(r.put("/b/c/a", []byte("x")), 401)
	r.want(r.put("/b/c/a", []byte("x"), "Authorization", "Bearer nope"), 401)
	r.want(r.put("/b/c/a", []byte("x"), auth...), 201)

	// A declared length over the cap is refused before the body is read.
	req := httptest.NewRequest("PUT", "/b/c/big", nil)
	req.Header.Set("Authorization", "Bearer secret")
	req.ContentLength = MaxFileSize + 1
	rec := httptest.NewRecorder()
	r.n.Handler().ServeHTTP(rec, req)
	if rec.Code != 413 {
		t.Fatalf("oversized Content-Length: %d", rec.Code)
	}

	multi := func(parts ...[2]string) resp {
		var buf bytes.Buffer
		mw := multipart.NewWriter(&buf)
		for _, p := range parts {
			fw, _ := mw.CreateFormField(p[0])
			fw.Write([]byte(p[1]))
		}
		mw.Close()
		return r.do("PUT", "/b/c/m", buf.Bytes(), append(auth, "Content-Type", mw.FormDataContentType())...)
	}
	r.want(multi([2]string{"metadata", string(make([]byte, MaxMetadataSize+1))}, [2]string{"file", "x"}), 413)
	r.want(multi([2]string{"file", "x"}, [2]string{"metadata", "{}"}), 400) // metadata after file
	r.want(multi([2]string{"file", "x"}, [2]string{"file", "y"}), 400)      // two files
	r.want(multi([2]string{"extra", "x"}, [2]string{"file", "y"}), 400)     // unknown part
	r.want(multi([2]string{"metadata", "{}"}), 400)                         // no file
	r.want(multi([2]string{"metadata", string(make([]byte, MaxMetadataSize))}, [2]string{"file", "x"}), 201)
	r.want(multi([2]string{"metadata", ""}, [2]string{"file", "x"}), 201) // empty metadata = none
	r.want(r.do("GET", "/m/c/m", nil, auth...), 204)
}

func testCorruptReplicaFallsBack(t *testing.T) {
	for _, sealed := range []bool{false, true} {
		t.Run(fmt.Sprint("sealed=", sealed), func(t *testing.T) {
			r := newRigWith(t, 3, Config{DefaultShards: 1}, nil)
			data := bytes.Repeat([]byte("payload "), 20)
			r.want(r.put("/b/c/a", data), 201)
			if sealed {
				r.restart() // seals the volume; reads now go through .nidx
			}
			corrupt := func(diskIdx int) {
				vs, _ := r.cat.ListShardVolumes(context.Background(), "c", 0)
				f, err := r.fs[diskIdx].OpenFile(volume.Path("c", uint64(vs[0].ID)), disk.Write)
				if err != nil {
					t.Fatal(err)
				}
				defer f.Close()
				var off, ln int64
				if _, err := volume.Scan(f, func(rec volume.Rec) error {
					if rec.Type == format.TypeFile {
						off, ln = rec.Off, int64(rec.Len)
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				if _, err := f.WriteAt([]byte{0xFF, 0xFF}, off+ln-24); err != nil { // inside the payload
					t.Fatal(err)
				}
				if err := f.Sync(); err != nil {
					t.Fatal(err)
				}
			}
			get := func() resp { return r.do("GET", "/b/c/a", nil) }
			if g := get(); g.code != 200 || !bytes.Equal(g.body, data) {
				t.Fatalf("intact: %d", g.code)
			}
			corrupt(0)
			corrupt(1)
			if g := get(); g.code != 200 || !bytes.Equal(g.body, data) {
				t.Fatalf("one intact replica left: %d", g.code)
			}
			corrupt(2)
			r.want(get(), 500) // no intact copy, and never wrong bytes
		})
	}
}

// eachBackend runs a test against the in-memory, SQLite and PostgreSQL
// catalogues. Tests in this file are not parallel, so the selector is safe.
func eachBackend(t *testing.T, f func(*testing.T)) {
	for _, b := range []string{"mem", "sqlite", "postgres"} {
		t.Run(b, func(t *testing.T) {
			backend = b
			defer func() { backend = "mem" }()
			f(t)
		})
	}
}

func TestPutGetHead(t *testing.T)            { eachBackend(t, testPutGetHead) }
func TestMetadataIsPartOfDedup(t *testing.T) { eachBackend(t, testMetadataIsPartOfDedup) }
func TestConditionsAndDigests(t *testing.T)  { eachBackend(t, testConditionsAndDigests) }
func TestValidation(t *testing.T)            { eachBackend(t, testValidation) }
func TestBucketLifecycle(t *testing.T)       { eachBackend(t, testBucketLifecycle) }
func TestConcurrentIdenticalPutsStoreOneVersion(t *testing.T) {
	eachBackend(t, testConcurrentIdenticalPutsStoreOneVersion)
}
func TestRestartSealsAndKeepsData(t *testing.T)   { eachBackend(t, testRestartSealsAndKeepsData) }
func TestStopSealsAndReleasesLeases(t *testing.T) { eachBackend(t, testStopSealsAndReleasesLeases) }
func TestVolumeRotatesWhenFull(t *testing.T)      { eachBackend(t, testVolumeRotatesWhenFull) }
func TestReplicaFailureSealsAndRetriesOnNewVolume(t *testing.T) {
	eachBackend(t, testReplicaFailureSealsAndRetriesOnNewVolume)
}
func TestNoReplicaSetIs503(t *testing.T) { eachBackend(t, testNoReplicaSetIs503) }
func TestMemoryBudget503(t *testing.T)   { eachBackend(t, testMemoryBudget503) }
func TestCrashRecovery(t *testing.T)     { eachBackend(t, testCrashRecovery) }
func TestIdleVolumeSealedAndLeaseReleased(t *testing.T) {
	eachBackend(t, testIdleVolumeSealedAndLeaseReleased)
}
func TestStopJoinsGoroutines(t *testing.T)  { eachBackend(t, testStopJoinsGoroutines) }
func TestTwoNodesInOneProcess(t *testing.T) { eachBackend(t, testTwoNodesInOneProcess) }

package node

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestConfigDefaultsAndValidation(t *testing.T) {
	c, err := Config{NodeName: "n1"}.withDefaults()
	if err != nil {
		t.Fatal(err)
	}
	if c.DefaultShards != 4 || c.MemoryBudget != 256<<20 || c.LeaseTTL != 30*time.Second ||
		c.VolumeMaxSize != 1<<30 || c.VolumeIdle != time.Hour || c.MaxOpenVolumes != 64 {
		t.Fatalf("defaults: %+v", c)
	}
	for name, bad := range map[string]Config{
		"no name":        {},
		"shards":         {NodeName: "n", DefaultShards: 2000},
		"tiny budget":    {NodeName: "n", MemoryBudget: 1 << 20},
		"tiny volume":    {NodeName: "n", VolumeMaxSize: 1 << 20},
		"negative ttl":   {NodeName: "n", LeaseTTL: -1},
		"negative limit": {NodeName: "n", MaxOpenVolumes: -1},
	} {
		if _, err := bad.withDefaults(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestBudget(t *testing.T) {
	b := newBudget(100)
	if !b.tryAcquire(60) || !b.tryAcquire(40) {
		t.Fatal("fits exactly")
	}
	if b.tryAcquire(1) {
		t.Fatal("over budget accepted")
	}
	if b.tryAcquire(-1) {
		t.Fatal("negative accepted")
	}
	b.release(60)
	if b.inUse() != 40 || !b.tryAcquire(60) {
		t.Fatal("release did not free bytes")
	}
	defer func() {
		if recover() == nil {
			t.Fatal("over-release must panic")
		}
	}()
	b.release(1000)
}

func TestBudgetConcurrent(t *testing.T) {
	b := newBudget(1000)
	var wg sync.WaitGroup
	var granted atomic.Int64
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if b.tryAcquire(100) {
				granted.Add(100)
			}
		}()
	}
	wg.Wait()
	if granted.Load() != 1000 || b.inUse() != 1000 {
		t.Fatalf("granted %d, in use %d", granted.Load(), b.inUse())
	}
}

func TestNameLockSerialisesSameName(t *testing.T) {
	var l nameLocks
	var inside, maxInside atomic.Int32
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			unlock := l.lock("b", "same/name")
			n := inside.Add(1)
			for {
				m := maxInside.Load()
				if n <= m || maxInside.CompareAndSwap(m, n) {
					break
				}
			}
			for range 100 { // widen the race window without real time
				runtime.Gosched()
			}
			inside.Add(-1)
			unlock()
		}()
	}
	wg.Wait()
	if maxInside.Load() != 1 {
		t.Fatalf("%d goroutines held the same name's lock at once", maxInside.Load())
	}
}

func TestNameLockDistinctNamesCanOverlap(t *testing.T) {
	var l nameLocks
	// Find two names on different stripes, then hold one and take the other.
	a := l.lock("b", "a")
	defer a()
	done := make(chan struct{})
	go func() {
		for i := 0; ; i++ {
			name := "x" + string(rune('a'+i%26)) + string(rune('a'+i/26%26))
			if stripeOf("b", name) != stripeOf("b", "a") {
				u := l.lock("b", name)
				u()
				close(done)
				return
			}
		}
	}()
	<-done // a hang here is caught by the test timeout
}

func TestAlarmsRecording(t *testing.T) {
	var a RecordingAlarms
	a.Raise(AlarmDiskExcluded, Warning, "x")
	a.Raise(AlarmDiskExcluded, Warning, "x")
	a.Raise(AlarmReplicaUnavailable, Critical, "y")
	if got := a.Active(); len(got) != 2 {
		t.Fatalf("active = %v", got)
	}
	a.Clear(AlarmDiskExcluded)
	if got := a.Active(); len(got) != 1 || got[0] != AlarmReplicaUnavailable {
		t.Fatalf("active = %v", got)
	}
}

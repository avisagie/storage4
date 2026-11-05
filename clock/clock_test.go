package clock

import (
	"testing"
	"time"
)

func TestFakeAfter(t *testing.T) {
	f := NewFake(time.Unix(100, 0))
	a := f.After(10 * time.Second)
	b := f.After(20 * time.Second)
	if f.Waiters() != 2 {
		t.Fatalf("waiters = %d", f.Waiters())
	}
	f.Advance(9 * time.Second)
	select {
	case <-a:
		t.Fatal("fired early")
	default:
	}
	f.Advance(time.Second)
	select {
	case got := <-a:
		if !got.Equal(time.Unix(110, 0)) {
			t.Fatalf("fired with %v", got)
		}
	default:
		t.Fatal("did not fire at the deadline")
	}
	if f.Waiters() != 1 {
		t.Fatalf("waiters = %d", f.Waiters())
	}
	f.Set(time.Unix(500, 0))
	select {
	case <-b:
	default:
		t.Fatal("Set did not fire")
	}
	if f.Waiters() != 0 {
		t.Fatalf("waiters = %d", f.Waiters())
	}
}

func TestFakeAfterNonPositive(t *testing.T) {
	f := NewFake(time.Unix(1, 0))
	select {
	case <-f.After(0):
	default:
		t.Fatal("After(0) must fire immediately")
	}
	select {
	case <-f.After(-time.Second):
	default:
		t.Fatal("After(<0) must fire immediately")
	}
	if f.Waiters() != 0 {
		t.Fatal("immediate After must not register a waiter")
	}
}

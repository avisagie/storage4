// Package clock is the injected time source. Production code never calls
// time.Now; cmd/ supplies the real clock, tests and sim/ supply Fake.
package clock

import (
	"sync"
	"time"
)

// Clock reports the current time.
type Clock interface{ Now() time.Time }

// Fake is a manually advanced Clock, safe for concurrent use.
type Fake struct {
	mu sync.Mutex
	t  time.Time
}

// NewFake returns a Fake starting at t.
func NewFake(t time.Time) *Fake { return &Fake{t: t} }

// Now returns the fake time.
func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.t
}

// Advance moves the fake time forward by d.
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.t = f.t.Add(d)
}

// Set sets the fake time.
func (f *Fake) Set(t time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.t = t
}

// Package clock is the injected time source. Production code never calls
// time.Now; cmd/ supplies the real clock, tests and sim/ supply Fake.
package clock

import (
	"sync"
	"time"
)

// Clock reports the current time and lets code wait without time.Sleep.
type Clock interface {
	Now() time.Time
	// After returns a channel that receives the time once d has passed on
	// this clock. Like time.After, the channel is buffered: a send never
	// blocks, and an abandoned channel leaks nothing but itself.
	After(d time.Duration) <-chan time.Time
}

// Fake is a manually advanced Clock, safe for concurrent use. Time moves
// only through Advance and Set; After channels fire when the fake time
// reaches their deadline.
type Fake struct {
	mu      sync.Mutex
	t       time.Time
	waiters []waiter
}

type waiter struct {
	at time.Time
	ch chan time.Time
}

// NewFake returns a Fake starting at t.
func NewFake(t time.Time) *Fake { return &Fake{t: t} }

// Now returns the fake time.
func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.t
}

// After returns a channel that fires when the fake time reaches Now()+d. A
// d <= 0 fires immediately.
func (f *Fake) After(d time.Duration) <-chan time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	ch := make(chan time.Time, 1)
	if d <= 0 {
		ch <- f.t
		return ch
	}
	f.waiters = append(f.waiters, waiter{at: f.t.Add(d), ch: ch})
	return ch
}

// Waiters returns the number of After channels that have not fired yet. Tests
// use it to wait until a goroutine is blocked on the clock before advancing.
func (f *Fake) Waiters() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.waiters)
}

// Advance moves the fake time forward by d and fires due After channels.
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.t = f.t.Add(d)
	f.fire()
}

// Set sets the fake time and fires due After channels.
func (f *Fake) Set(t time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.t = t
	f.fire()
}

// fire sends to every waiter whose deadline has been reached. f.mu is held.
func (f *Fake) fire() {
	keep := f.waiters[:0]
	for _, w := range f.waiters {
		if w.at.After(f.t) {
			keep = append(keep, w)
			continue
		}
		w.ch <- f.t
	}
	f.waiters = keep
}

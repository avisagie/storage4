package node

import (
	"sort"
	"sync"
)

// AlarmID names a condition. Details never carry names, metadata or keys.
type AlarmID string

// Alarm IDs raised by the node.
const (
	AlarmCatalogueUnavailable AlarmID = "catalogue-unavailable"
	AlarmReplicaUnavailable   AlarmID = "replica-unavailable"
	AlarmDiskExcluded         AlarmID = "disk-excluded"
	AlarmShardLeaseLost       AlarmID = "shard-lease-lost"
	AlarmIndexRebuilt         AlarmID = "index-rebuilt"
	AlarmSealPending          AlarmID = "seal-pending"
	AlarmMemoryBudget         AlarmID = "memory-budget-exhausted"
)

// Severity of an alarm.
type Severity int

const (
	Info Severity = iota
	Warning
	Critical
)

// Alarms is implemented by the host. Raise is idempotent per ID (a persistent
// condition does not flood the host); calls must not block and must be safe
// for concurrent use.
type Alarms interface {
	Raise(id AlarmID, sev Severity, detail string)
	Clear(id AlarmID)
}

// NopAlarms discards everything; it is the default.
type NopAlarms struct{}

func (NopAlarms) Raise(AlarmID, Severity, string) {}
func (NopAlarms) Clear(AlarmID)                   {}

// RecordingAlarms remembers the active alarms. For tests and simple hosts.
type RecordingAlarms struct {
	mu     sync.Mutex
	active map[AlarmID]string
}

func (r *RecordingAlarms) Raise(id AlarmID, _ Severity, detail string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.active == nil {
		r.active = map[AlarmID]string{}
	}
	r.active[id] = detail
}

func (r *RecordingAlarms) Clear(id AlarmID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.active, id)
}

// Active returns the currently raised alarm IDs, sorted.
func (r *RecordingAlarms) Active() []AlarmID {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []AlarmID
	for id := range r.active {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

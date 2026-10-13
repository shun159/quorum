package quorum

import (
	"slices"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/gen"
)

// memStorage is a Storage in memory, for simulations: it outlives the
// member, restarted, and a crash loses the writes the member had not
// learned were durable, but for the first ones, as many as the test keeps.
// A write is durable once the member learns it is, fsyncTime after it, as
// a disk takes time to sync.
type memStorage struct {
	saved  Saved
	writes []memWrite // performed, in order, not known durable yet
}

// memWrite is a write: of the state if state is set, of a snapshot if
// snapshot is, else of the log.
type memWrite struct {
	tag      any
	state    *State
	snapshot *Snapshot
	index    uint64
	entries  []Entry
}

// fsyncTime is how long a write of a memStorage takes to be durable.
const fsyncTime = 5 * time.Millisecond

// memDurable is the message telling the member a write is durable.
type memDurable struct{ tag any }

// memEffect is the effect of a write, performed by the member's runtime.
type memEffect struct {
	actor.Extension
	m *memStorage
	w memWrite
}

func (e memEffect) Perform(env gen.Env) {
	e.m.writes = append(e.m.writes, e.w)
	env.SendAfter(fsyncTime, memDurable{e.w.tag})
}

func (m *memStorage) Load() (Saved, error) {
	return Saved{State: m.saved.State, Snapshot: m.saved.Snapshot, Log: slices.Clone(m.saved.Log)}, nil
}

func (m *memStorage) SaveSnapshot(snap Snapshot, tag any) actor.Effect {
	return memEffect{m: m, w: memWrite{tag: tag, snapshot: &snap}}
}

func (m *memStorage) SaveState(st State, tag any) actor.Effect {
	return memEffect{m: m, w: memWrite{tag: tag, state: &st}}
}

func (m *memStorage) Append(index uint64, entries []Entry, tag any) actor.Effect {
	return memEffect{m: m, w: memWrite{tag: tag, index: index, entries: slices.Clone(entries)}}
}

func (m *memStorage) Durable(msg any) (any, error, bool) {
	d, ok := msg.(memDurable)
	if !ok {
		return nil, nil, false
	}
	i := slices.IndexFunc(m.writes, func(w memWrite) bool { return w.tag == d.tag })
	m.keep(i + 1)
	return d.tag, nil, true
}

// crash loses the writes not known durable, but the first keep of them.
func (m *memStorage) crash(keep int) {
	m.keep(min(keep, len(m.writes)))
	m.writes = nil
}

// keep makes the first n writes durable.
func (m *memStorage) keep(n int) {
	for _, w := range m.writes[:n] {
		switch {
		case w.state != nil:
			m.saved.State = *w.state
		case w.snapshot != nil:
			m.saveSnapshot(*w.snapshot)
		default:
			kept := min(w.index-1-m.saved.Snapshot.Index, uint64(len(m.saved.Log)))
			m.saved.Log = append(slices.Clip(m.saved.Log[:kept]), w.entries...)
		}
	}
	m.writes = m.writes[n:]
}

// saveSnapshot puts snap in place of the entries up to its index.
func (m *memStorage) saveSnapshot(snap Snapshot) {
	gone := min(snap.Index-m.saved.Snapshot.Index, uint64(len(m.saved.Log)))
	m.saved.Log = slices.Clone(m.saved.Log[gone:])
	m.saved.Snapshot = snap
}

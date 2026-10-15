package raft

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
	saved      Saved
	writes     []memWrite // performed, in order, not known durable yet
	background []memWrite // snapshots saved apart, not durable yet
}

// memWrite is a write: of the state if state is set, of a snapshot if
// snapshot is, else of the log.
type memWrite struct {
	tag        any
	state      *State
	snapshot   *SavedSnapshot
	encode     func() (Encoded, error) // the snapshot, as performed
	background bool
	index      uint64
	entries    []Entry
}

// fsyncTime is how long a write of a memStorage takes to be durable.
const fsyncTime = 5 * time.Millisecond

// memDurable is the message telling the member a write is durable, or
// failed.
type memDurable struct {
	tag any
	err error
}

// memEffect is the effect of a write, performed by the member's runtime.
type memEffect struct {
	actor.Extension
	m *memStorage
	w memWrite
}

func (e memEffect) Perform(env gen.Env) {
	if e.w.encode != nil {
		enc, err := e.w.encode()
		if err != nil {
			env.Send(env.Self(), memDurable{e.w.tag, err})
			return
		}
		e.w.snapshot.Encoded = enc
	}
	if e.w.background {
		e.m.background = append(e.m.background, e.w)
	} else {
		e.m.writes = append(e.m.writes, e.w)
	}
	env.SendAfter(fsyncTime, memDurable{tag: e.w.tag})
}

func (m *memStorage) Load() (Saved, error) {
	return Saved{State: m.saved.State, Snapshot: m.saved.Snapshot, Log: slices.Clone(m.saved.Log)}, nil
}

func (m *memStorage) SaveSnapshot(index uint64, encode func() (Encoded, error), background bool, tag any) actor.Effect {
	return memEffect{m: m, w: memWrite{tag: tag, snapshot: &SavedSnapshot{Index: index}, encode: encode, background: background}}
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
	if d.err != nil {
		return d.tag, d.err, true
	}
	if i := slices.IndexFunc(m.background, func(w memWrite) bool { return w.tag == d.tag }); i >= 0 {
		m.saveSnapshot(*m.background[i].snapshot)
		m.background = slices.Delete(m.background, i, i+1)
		return d.tag, nil, true
	}
	i := slices.IndexFunc(m.writes, func(w memWrite) bool { return w.tag == d.tag })
	m.keep(i + 1)
	return d.tag, nil, true
}

// crash loses the writes not known durable, but the first keep of them.
func (m *memStorage) crash(keep int) {
	m.keep(min(keep, len(m.writes)))
	m.writes, m.background = nil, nil
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
			index, entries := w.index, w.entries
			if s := m.saved.Snapshot.Index; index <= s {
				// Entries up to a snapshot saved in the background, durable
				// first: as Load skips them.
				skip := min(s+1-index, uint64(len(entries)))
				index, entries = s+1, entries[skip:]
			}
			kept := min(index-1-m.saved.Snapshot.Index, uint64(len(m.saved.Log)))
			m.saved.Log = append(slices.Clip(m.saved.Log[:kept]), entries...)
		}
	}
	m.writes = m.writes[n:]
}

// saveSnapshot puts snap in place of the entries up to its index, unless
// older than the one saved.
func (m *memStorage) saveSnapshot(snap SavedSnapshot) {
	if snap.Index <= m.saved.Snapshot.Index {
		return
	}
	gone := min(snap.Index-m.saved.Snapshot.Index, uint64(len(m.saved.Log)))
	m.saved.Log = slices.Clone(m.saved.Log[gone:])
	m.saved.Snapshot = snap
}

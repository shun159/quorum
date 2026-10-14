// Package kvsstorage keeps the members of quorum groups in a kvs table:
// who they are, their terms, their votes, their snapshots and their logs.
// One table serves all the groups of a node, each member its own part of
// it, so that the writes of all go to disk together.
//
// The table is a child of the supervision tree, which opens it as it
// starts and closes it as it stops, ahead of the members, which find it
// by its directory:
//
//	Strategy: supervisor.OneForAll,
//	Children: []supervisor.ChildSpec{
//		{ID: "storage", Start: kvsstorage.Child(dir)},
//		{ID: "member", Start: quorum.Child(spec)}, // spec.Storage = kvsstorage.In(dir, spec.Group)
//	},
//
// Under OneForAll, the table and the members restart together: a member
// stops on a write failed, after which the table must be opened again. A
// table opened by hand is For.
//
// The commands and the states of the machines are encoded by gob: their
// types must be registered, as dist.Register does.
package kvsstorage

import (
	"cmp"
	"fmt"
	"slices"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/gen"
	"github.com/shun159/molecule/kvs"
	"github.com/shun159/quorum"
)

// Kind is what a record of a member is.
type Kind uint8

const (
	KindState Kind = iota
	KindSnapshot
	KindEntry
)

// Key orders the records of a table: by group, then by kind, the entries
// of the log by index.
type Key struct {
	Group string
	Kind  Kind
	Index uint64
}

func (k Key) Compare(o Key) int {
	return cmp.Or(cmp.Compare(k.Group, o.Group), cmp.Compare(k.Kind, o.Kind), cmp.Compare(k.Index, o.Index))
}

// Record is a record of a member, the field of its kind set.
type Record struct {
	State    quorum.State
	Snapshot quorum.Snapshot
	Entry    quorum.Entry
}

// Table is a table of members.
type Table = kvs.Table[Key, Record]

// Open opens the table kept in dir, made if missing.
func Open(dir string) (*Table, error) {
	return kvs.Open(dir, kvs.Options[Key, Record]{Type: kvs.OrderedSet})
}

// For returns the storage of the member of group in t.
func For(t *Table, group string) quorum.Storage {
	return storage{t: t, group: group}
}

// In returns the storage of the member of group in the table kept in dir,
// open by Child.
func In(dir, group string) quorum.Storage {
	return storage{dir: clean(dir), group: group}
}

// storage is the part of a member in its table: t, or else the table open
// in dir.
type storage struct {
	t     *Table
	dir   string
	group string
}

func (s storage) table() (*Table, error) {
	if s.t != nil {
		return s.t, nil
	}
	return opened(s.dir)
}

func (s storage) key(kind Kind, index uint64) Key {
	return Key{Group: s.group, Kind: kind, Index: index}
}

// end is the first key past those of the member.
func (s storage) end() Key { return Key{Group: s.group + "\x00"} }

// Load reads the records of the member back. Entries up to the snapshot,
// left by a crash amid deleting them, are skipped.
func (s storage) Load() (quorum.Saved, error) {
	t, err := s.table()
	if err != nil {
		return quorum.Saved{}, err
	}
	var saved quorum.Saved
	for k, r := range t.Range(s.key(KindState, 0), s.end()) {
		switch {
		case k.Kind == KindState:
			saved.State = r.State
		case k.Kind == KindSnapshot:
			saved.Snapshot = r.Snapshot
		case k.Index <= saved.Snapshot.Index:
		case k.Index == saved.Snapshot.Index+uint64(len(saved.Log))+1:
			saved.Log = append(saved.Log, r.Entry)
		default:
			return quorum.Saved{}, fmt.Errorf("kvsstorage: group %s has entry %d after %d",
				s.group, k.Index, saved.Snapshot.Index+uint64(len(saved.Log)))
		}
	}
	return saved, nil
}

func (s storage) SaveState(st quorum.State, tag any) actor.Effect {
	return writesEffect{s: s, tag: tag, writes: func(t *Table) []write {
		return []write{put(t, s.key(KindState, 0), Record{State: st})}
	}}
}

func (s storage) Durable(msg any) (any, error, bool) {
	d, ok := msg.(kvs.Durable)
	return d.Tag, d.Err, ok
}

// Append returns the effect writing entries from index on, in place of
// those saved from there. It deletes the entries saved from index on, the
// last first, then puts the new ones, in order: should the node crash amid
// them, the log left on disk is the start of the old one, or of the new
// one, never old entries after new ones.
func (s storage) Append(index uint64, entries []quorum.Entry, tag any) actor.Effect {
	return writesEffect{s: s, tag: tag, writes: func(t *Table) []write {
		var writes []write
		for _, k := range slices.Backward(s.keys(t, index)) {
			writes = append(writes, del(t, k))
		}
		for i, e := range entries {
			writes = append(writes, put(t, s.key(KindEntry, index+uint64(i)), Record{Entry: e}))
		}
		return writes
	}}
}

// SaveSnapshot returns the effect putting snap, then deleting the entries
// up to its index, which Load skips meanwhile.
func (s storage) SaveSnapshot(snap quorum.Snapshot, tag any) actor.Effect {
	return writesEffect{s: s, tag: tag, writes: func(t *Table) []write {
		writes := []write{put(t, s.key(KindSnapshot, 0), Record{Snapshot: snap})}
		for _, k := range s.keys(t, 1) {
			if k.Index > snap.Index {
				break
			}
			writes = append(writes, del(t, k))
		}
		return writes
	}}
}

// keys returns the keys of the entries saved in t from index on, in order.
func (s storage) keys(t *Table, index uint64) []Key {
	var keys []Key
	for k := range t.Range(s.key(KindEntry, index), s.end()) {
		keys = append(keys, k)
	}
	return keys
}

// write is a write of the table, to make with its tag.
type write func(tag any) actor.Effect

func put(t *Table, k Key, r Record) write {
	return func(tag any) actor.Effect { return t.PutEffect(k, r, tag) }
}

func del(t *Table, k Key) write {
	return func(tag any) actor.Effect { return t.DeleteEffect(k, tag) }
}

// writesEffect performs the writes it makes, as it is performed: what
// they are depends on what is saved then. Only its last write tells when
// it is durable, which those before it are then. Without a table open,
// it fails.
type writesEffect struct {
	actor.Extension
	s      storage
	tag    any
	writes func(*Table) []write
}

func (e writesEffect) Perform(env gen.Env) {
	t, err := e.s.table()
	if err != nil {
		env.Send(env.Self(), kvs.Durable{Tag: e.tag, Err: err})
		return
	}
	writes := e.writes(t)
	if len(writes) == 0 {
		env.Send(env.Self(), kvs.Durable{Tag: e.tag})
		return
	}
	for i, w := range writes {
		var tag any
		if i == len(writes)-1 {
			tag = e.tag
		}
		w(tag).(gen.Performer).Perform(env)
	}
}

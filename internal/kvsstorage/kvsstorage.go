// Package kvsstorage keeps the members of quorum groups in a kvs table:
// who they are, their terms, their votes, their snapshots and their logs.
// One table serves all the groups of a node, each member its own part of
// it, so that the writes of all go to disk together.
//
// The table is a child of the supervision tree, quorum.Storage, which
// opens it as it starts and closes it as it stops; the members, In it,
// find it by its directory. A table opened by hand is For.
//
// The commands and the states of the machines are encoded by gob: their
// types must be registered, as dist.Register does.
package kvsstorage

import (
	"cmp"
	"fmt"
	"path/filepath"
	"slices"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/gen"
	"github.com/shun159/molecule/kvs"
	"github.com/shun159/quorum/internal/raft"
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

// Record is a record of a member, the field of its kind set; or of a group
// started on the node, being deleted from it if Deleting.
type Record struct {
	State    raft.State
	Snapshot uint64 // the index of the snapshot, kept in its file
	Entry    raft.Entry
	Deleting bool
}

// Table is a table of members.
type Table = kvs.Table[Key, Record]

var options = kvs.Options[Key, Record]{Type: kvs.OrderedSet}

// Open opens the table kept in dir, made if missing.
func Open(dir string) (*Table, error) {
	return kvs.Open(dir, options)
}

// Child returns the owner of the table kept in dir, made if missing, to
// start under a supervisor ahead of the members In it: see kvs.Child.
func Child(dir string) gen.Child {
	return kvs.Child(dir, options)
}

// For returns the storage of the member of group in t.
func For(t *Table, group string) raft.Storage {
	return storage{t: t, group: group}
}

// In returns the storage of the member of group in the table kept in dir,
// open by Child.
func In(dir, group string) raft.Storage {
	if abs, err := filepath.Abs(dir); err == nil {
		dir = abs // for kvs.Opened to find it without the working directory
	}
	return storage{dir: dir, group: group}
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
	return kvs.Opened[Key, Record](s.dir)
}

func (s storage) key(kind Kind, index uint64) Key {
	return Key{Group: s.group, Kind: kind, Index: index}
}

// end is the first key past those of the member.
func (s storage) end() Key { return Key{Group: s.group + "\x00"} }

// Load reads the records of the member back. Entries up to the snapshot,
// left by a crash amid deleting them, are skipped.
func (s storage) Load() (raft.Saved, error) {
	t, err := s.table()
	if err != nil {
		return raft.Saved{}, err
	}
	var saved raft.Saved
	for k, r := range t.Range(s.key(KindState, 0), s.end()) {
		switch {
		case k.Kind == KindState:
			saved.State = r.State
		case k.Kind == KindSnapshot:
			removeTemps(s.snapshotDir(t))
			enc, err := readSnapshot(s.snapshotDir(t), r.Snapshot)
			if err != nil {
				return raft.Saved{}, fmt.Errorf("kvsstorage: group %s cannot read its snapshot: %w", s.group, err)
			}
			saved.Snapshot = raft.SavedSnapshot{Index: r.Snapshot, Encoded: enc}
		case k.Index <= saved.Snapshot.Index:
		case k.Index == saved.Snapshot.Index+uint64(len(saved.Log))+1:
			saved.Log = append(saved.Log, r.Entry)
		default:
			return raft.Saved{}, fmt.Errorf("kvsstorage: group %s has entry %d after %d",
				s.group, k.Index, saved.Snapshot.Index+uint64(len(saved.Log)))
		}
	}
	return saved, nil
}

func (s storage) SaveState(st raft.State, tag any) actor.Effect {
	return writesEffect{s: s, tag: tag, writes: func(t *Table) ([]write, error) {
		return []write{put(s.key(KindState, 0), Record{State: st})}, nil
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
func (s storage) Append(index uint64, entries []raft.Entry, tag any) actor.Effect {
	return writesEffect{s: s, tag: tag, writes: func(t *Table) ([]write, error) {
		var writes []write
		for _, k := range slices.Backward(s.keys(t, index)) {
			writes = append(writes, del(k))
		}
		for i, e := range entries {
			writes = append(writes, put(s.key(KindEntry, index+uint64(i)), Record{Entry: e}))
		}
		return writes, nil
	}}
}

// SaveSnapshot returns the effect writing the file of the snapshot encode
// encodes, then putting its record, and deleting the entries up to index,
// which Load skips meanwhile: in a goroutine of its own if background.
func (s storage) SaveSnapshot(index uint64, encode func() (raft.Encoded, error), background bool, tag any) actor.Effect {
	return snapshotEffect{s: s, index: index, encode: encode, background: background, tag: tag}
}

// keys returns the keys of the entries saved in t from index on, in order.
func (s storage) keys(t *Table, index uint64) []Key {
	var keys []Key
	for k := range t.Range(s.key(KindEntry, index), s.end()) {
		keys = append(keys, k)
	}
	return keys
}

// write is a write of the table.
type write = kvs.Write[Key, Record]

func put(k Key, r Record) write { return write{Key: k, Val: r} }

func del(k Key) write { return write{Key: k, Delete: true} }

// writesEffect performs the writes it makes, as it is performed: what
// they are depends on what is saved then. They are made at once, all or
// none, and one Durable tells of them. Without a table open, it fails.
type writesEffect struct {
	actor.Extension
	s      storage
	tag    any
	writes func(*Table) ([]write, error)
}

func (e writesEffect) Perform(env gen.Env) {
	t, err := e.s.table()
	if err != nil {
		env.Send(env.Self(), kvs.Durable{Tag: e.tag, Err: err})
		return
	}
	writes, err := e.writes(t)
	if err != nil {
		env.Send(env.Self(), kvs.Durable{Tag: e.tag, Err: err})
		return
	}
	t.WritesEffect(e.tag, writes...).(gen.Performer).Perform(env)
}

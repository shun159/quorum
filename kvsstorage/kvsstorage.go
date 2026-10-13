// Package kvsstorage keeps the members of quorum groups in a kvs table:
// their terms, their votes and their logs. One table serves all the groups
// of a node, each member its own part of it, so that the writes of all go
// to disk together.
//
//	table, err := kvsstorage.Open(dir)
//	spec.Storage = kvsstorage.For(table, spec.Group)
//
// The commands are encoded by gob: their types must be registered, as
// dist.Register does.
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

// Key orders the records of a table: by group, then the state of the
// group's member at index 0, then the entries of its log at their index.
type Key struct {
	Group string
	Index uint64
}

func (k Key) Compare(o Key) int {
	return cmp.Or(cmp.Compare(k.Group, o.Group), cmp.Compare(k.Index, o.Index))
}

// Record is the state of a member, at index 0, or an entry of its log.
type Record struct {
	State quorum.State
	Entry quorum.Entry
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

type storage struct {
	t     *Table
	group string
}

// state is the key of the state of the member; the entries follow it.
func (s storage) state() Key { return Key{Group: s.group} }

// end is the first key past those of the member.
func (s storage) end() Key { return Key{Group: s.group + "\x00"} }

func (s storage) Load() (quorum.Saved, error) {
	var saved quorum.Saved
	for k, r := range s.t.Range(s.state(), s.end()) {
		switch k.Index {
		case 0:
			saved.State = r.State
		case uint64(len(saved.Log)) + 1:
			saved.Log = append(saved.Log, r.Entry)
		default:
			return quorum.Saved{}, fmt.Errorf("kvsstorage: group %s has entry %d after %d", s.group, k.Index, len(saved.Log))
		}
	}
	return saved, nil
}

func (s storage) SaveState(st quorum.State, tag any) actor.Effect {
	return s.t.PutEffect(s.state(), Record{State: st}, tag)
}

func (s storage) Append(index uint64, entries []quorum.Entry, tag any) actor.Effect {
	return appendEffect{s: s, index: index, entries: entries, tag: tag}
}

func (s storage) Durable(msg any) (any, error, bool) {
	d, ok := msg.(kvs.Durable)
	return d.Tag, d.Err, ok
}

// appendEffect writes entries from index on, in place of those saved from
// there. Only its last write tells when it is durable, which those before
// it are then.
//
// It deletes the entries saved from index on, the last first, then puts
// the new ones, in order: should the node crash amid them, the log left on
// disk is the start of the old one, or of the new one, never old entries
// after new ones.
type appendEffect struct {
	actor.Extension
	s       storage
	index   uint64
	entries []quorum.Entry
	tag     any
}

func (e appendEffect) Perform(env gen.Env) {
	writes := e.writes()
	if len(writes) == 0 {
		env.Send(env.Self(), kvs.Durable{Tag: e.tag})
		return
	}
	for i, write := range writes {
		var tag any
		if i == len(writes)-1 {
			tag = e.tag
		}
		write(tag).(gen.Performer).Perform(env)
	}
}

// writes returns the writes of the effect, in order, each to make with
// its tag.
func (e appendEffect) writes() []func(tag any) actor.Effect {
	t := e.s.t
	var writes []func(any) actor.Effect
	var old []Key
	for k := range t.Range(Key{Group: e.s.group, Index: e.index}, e.s.end()) {
		old = append(old, k)
	}
	for _, k := range slices.Backward(old) {
		writes = append(writes, func(tag any) actor.Effect { return t.DeleteEffect(k, tag) })
	}
	for i, entry := range e.entries {
		k := Key{Group: e.s.group, Index: e.index + uint64(i)}
		writes = append(writes, func(tag any) actor.Effect { return t.PutEffect(k, Record{Entry: entry}, tag) })
	}
	return writes
}

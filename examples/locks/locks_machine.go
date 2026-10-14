package main

import (
	"context"
	"maps"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/dist"
	"github.com/shun159/quorum"
)

// group is the name of the group of the locks.
const group = "locks"

// Locks is the state the group replicates: who holds each lock. It is a
// value: lock and unlock change a copy of Held.
type Locks struct {
	Held map[string]Holder
	Last uint64 // the last token given
}

// Holder is who holds a lock, and the token it was given with it. Tokens
// only grow: a store the holder writes to can refuse a token older than
// one it has seen, as from a holder that lost the lock while it paused.
type Holder struct {
	Owner string
	Token uint64
}

// Op is a command to the group: lock, unlock, or ask who holds a lock.
type Op struct {
	Kind  Kind
	Name  string
	Owner string
}

type Kind uint8

const (
	Acquire Kind = iota + 1
	Release
	Query // a read, see machine.Read
)

// Reply is what an Op comes to: the holder of the lock, and whether the
// Op did as asked.
type Reply struct {
	Holder Holder
	OK     bool
}

func init() {
	dist.Register(Op{}, Reply{}, Locks{})
}

// machine is the quorum.Machine of the locks, and a quorum.Reader.
type machine struct{}

func (machine) Init() Locks { return Locks{} }

func (machine) Apply(l Locks, op Op) (Locks, Reply, []actor.Effect) {
	switch op.Kind {
	case Acquire:
		l, rep := l.lock(op.Name, op.Owner)
		return l, rep, nil
	case Release:
		l, rep := l.unlock(op.Name, op.Owner)
		return l, rep, nil
	}
	return l, Reply{}, nil
}

func (machine) Read(l Locks, op Op) Reply {
	h, ok := l.Held[op.Name]
	return Reply{Holder: h, OK: ok}
}

// lock gives the lock name to owner with a new token, if no one holds it.
// Its holder asking again keeps it, with the same token.
func (l Locks) lock(name, owner string) (Locks, Reply) {
	if h, ok := l.Held[name]; ok {
		return l, Reply{Holder: h, OK: h.Owner == owner}
	}
	l.Last++
	h := Holder{Owner: owner, Token: l.Last}
	l.Held = maps.Clone(l.Held)
	if l.Held == nil {
		l.Held = map[string]Holder{}
	}
	l.Held[name] = h
	return l, Reply{Holder: h, OK: true}
}

// unlock frees the lock name, if owner holds it.
func (l Locks) unlock(name, owner string) (Locks, Reply) {
	h, ok := l.Held[name]
	if !ok || h.Owner != owner {
		return l, Reply{Holder: h}
	}
	l.Held = maps.Clone(l.Held)
	delete(l.Held, name)
	return l, Reply{OK: true}
}

// spec is the group of the locks, its member keeping its log in storage.
func spec(storage quorum.Storage) quorum.Spec[Locks, Op, Reply] {
	return quorum.Spec[Locks, Op, Reply]{Group: group, Machine: machine{}, Storage: storage}
}

// Lock has owner take the lock name, asking the group on nodes.
func Lock(ctx context.Context, caller actor.Caller, nodes []string, name, owner string) (Reply, error) {
	return quorum.Command[Reply](ctx, caller, group, nodes, Op{Kind: Acquire, Name: name, Owner: owner})
}

// Unlock has owner free the lock name.
func Unlock(ctx context.Context, caller actor.Caller, nodes []string, name, owner string) (Reply, error) {
	return quorum.Command[Reply](ctx, caller, group, nodes, Op{Kind: Release, Name: name, Owner: owner})
}

// Who returns who holds the lock name: a read, which goes through no log.
func Who(ctx context.Context, caller actor.Caller, nodes []string, name string) (Reply, error) {
	return quorum.Read[Reply](ctx, caller, group, nodes, Op{Kind: Query, Name: name})
}

package main

import (
	"context"
	"fmt"
	"hash/fnv"
	"maps"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/dist"
	"github.com/shun159/quorum"
)

// Locks is the state a group replicates: who holds each of its locks. It
// is a value: lock and unlock change a copy of Held.
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

// Op is a command to a group: lock, unlock, ask who holds a lock, or how
// many are held.
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
	Count // a read
)

// Reply is what an Op comes to: the holder of the lock, and whether the
// Op did as asked; or, counting, how many locks are held.
type Reply struct {
	Holder Holder
	OK     bool
	Count  int
}

func init() {
	dist.Register(Op{}, Reply{}, Locks{}, Granted{}, Released{}, Leading{}, Following{}, leadsReq{}, Leads{})
}

// machine is the quorum.Machine of the locks of group, a quorum.Reader,
// and a quorum.Leader: it tells the holder of its node when the node
// leads the group, and when no more.
type machine struct{ group string }

func (machine) Init() Locks { return Locks{} }

func (m machine) Elected(_ Locks, term uint64) []actor.Effect {
	return actor.Do(actor.Send{To: holderName, Msg: Leading{m.group, term}})
}

func (m machine) Surrendered(Locks) []actor.Effect {
	return actor.Do(actor.Send{To: holderName, Msg: Following{m.group}})
}

func (machine) Apply(l Locks, op Op) (Locks, Reply, []actor.Effect) {
	switch op.Kind {
	case Acquire:
		return l.lock(op.Name, op.Owner)
	case Release:
		return l.unlock(op.Name, op.Owner)
	}
	return l, Reply{}, nil
}

func (machine) Read(l Locks, op Op) Reply {
	if op.Kind == Count {
		return Reply{Count: len(l.Held)}
	}
	h, ok := l.Held[op.Name]
	return Reply{Holder: h, OK: ok}
}

// lock gives the lock name to owner with a new token, if no one holds it,
// and tells the holder of the node of owner. Its holder asking again keeps
// it, with the same token.
func (l Locks) lock(name, owner string) (Locks, Reply, []actor.Effect) {
	if h, ok := l.Held[name]; ok {
		return l, Reply{Holder: h, OK: h.Owner == owner}, nil
	}
	l.Last++
	h := Holder{Owner: owner, Token: l.Last}
	l.Held = maps.Clone(l.Held)
	if l.Held == nil {
		l.Held = map[string]Holder{}
	}
	l.Held[name] = h
	return l, Reply{Holder: h, OK: true}, actor.Do(actor.Send{To: holderAt(owner), Msg: Granted{name, h.Token}})
}

// unlock frees the lock name, if owner holds it, and tells the holder of
// the node of owner.
func (l Locks) unlock(name, owner string) (Locks, Reply, []actor.Effect) {
	h, ok := l.Held[name]
	if !ok || h.Owner != owner {
		return l, Reply{Holder: h}, nil
	}
	l.Held = maps.Clone(l.Held)
	delete(l.Held, name)
	return l, Reply{OK: true}, actor.Do(actor.Send{To: holderAt(owner), Msg: Released{name, h.Token}})
}

// Service is the lock service on Nodes, its locks spread over Groups
// groups by their names: each group decides on its own, its leader on any
// of the nodes, so that the work spreads over them.
type Service struct {
	Nodes  []string
	Groups int
}

// group returns the name of the group i.
func group(i int) string { return fmt.Sprintf("locks/%d", i) }

// groupOf returns the group of the lock name.
func (s Service) groupOf(name string) string {
	h := fnv.New32a()
	h.Write([]byte(name))
	return group(int(h.Sum32() % uint32(s.Groups)))
}

// specs are the members of the groups on a node, keeping their logs in
// dir.
func (s Service) specs(dir string) []quorum.Spec[Locks, Op, Reply] {
	var specs []quorum.Spec[Locks, Op, Reply]
	for i := range s.Groups {
		specs = append(specs, quorum.Spec[Locks, Op, Reply]{Group: group(i), Machine: machine{group(i)}, Dir: dir})
	}
	return specs
}

// Found founds the groups, on all the nodes.
func (s Service) Found(ctx context.Context, caller actor.Caller) error {
	for i := range s.Groups {
		if err := quorum.Found(ctx, caller, group(i), s.Nodes); err != nil {
			return err
		}
	}
	return nil
}

// Lock has owner take the lock name.
func (s Service) Lock(ctx context.Context, caller actor.Caller, name, owner string) (Reply, error) {
	return quorum.Command[Reply](ctx, caller, s.groupOf(name), s.Nodes, Op{Kind: Acquire, Name: name, Owner: owner})
}

// Unlock has owner free the lock name.
func (s Service) Unlock(ctx context.Context, caller actor.Caller, name, owner string) (Reply, error) {
	return quorum.Command[Reply](ctx, caller, s.groupOf(name), s.Nodes, Op{Kind: Release, Name: name, Owner: owner})
}

// Who returns who holds the lock name: a read, which goes through no log.
func (s Service) Who(ctx context.Context, caller actor.Caller, name string) (Reply, error) {
	return quorum.Read[Reply](ctx, caller, s.groupOf(name), s.Nodes, Op{Kind: Query, Name: name})
}

// Count returns how many locks of group are held.
func (s Service) Count(ctx context.Context, caller actor.Caller, group string) (int, error) {
	r, err := quorum.Read[Reply](ctx, caller, group, s.Nodes, Op{Kind: Count})
	return r.Count, err
}

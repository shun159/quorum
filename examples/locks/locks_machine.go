package main

import (
	"context"
	"fmt"
	"hash/fnv"
	"time"

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

// Holder is who holds a lock, the token it was given with it, and when
// its lease expires, in UnixNano, unless renewed. Tokens only grow: a
// store the holder writes to can refuse a token older than one it has
// seen, as from a holder that lost the lock while it paused.
type Holder struct {
	Owner   string
	Token   uint64
	Expires int64
}

// Op is a command to a group: lock, for Lease, or renew; unlock; ask who
// holds a lock, or how many are held.
type Op struct {
	Kind  Kind
	Name  string
	Owner string
	Lease time.Duration
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
	dist.Register(Op{}, Reply{}, Locks{}, Granted{}, Released{}, Lost{}, Leading{}, Following{}, leadsReq{}, Leads{})
}

// machine is the quorum.Machine of the locks of group: Timed and a
// TimedReader, for leases, and a quorum.Leader, telling the holder of its
// node when the node leads the group, and when no more.
type machine struct{ group string }

func (machine) Init() Locks { return Locks{} }

func (m machine) Elected(_ Locks, term uint64) []actor.Effect {
	return actor.Do(actor.Send{To: holderName, Msg: Leading{m.group, term}})
}

func (m machine) Surrendered(Locks) []actor.Effect {
	return actor.Do(actor.Send{To: holderName, Msg: Following{m.group}})
}

func (m machine) Apply(l Locks, op Op) (Locks, Reply, []actor.Effect) {
	return m.ApplyAt(l, op, time.Time{}) // ApplyAt is called instead
}

func (m machine) Read(l Locks, op Op) Reply {
	return m.ReadAt(l, op, time.Time{}) // ReadAt is called instead
}

func (machine) ApplyAt(l Locks, op Op, at time.Time) (Locks, Reply, []actor.Effect) {
	switch op.Kind {
	case Acquire:
		return l.lock(op.Name, op.Owner, at.Add(op.Lease).UnixNano(), at.UnixNano())
	case Release:
		return l.unlock(op.Name, op.Owner)
	}
	return l, Reply{}, nil
}

// ReadAt answers who holds a lock, or how many are held, at: a lease
// expired is held no more.
func (machine) ReadAt(l Locks, op Op, at time.Time) Reply {
	now := at.UnixNano()
	if op.Kind == Count {
		n := 0
		for _, h := range l.Held {
			if h.Expires > now {
				n++
			}
		}
		return Reply{Count: n}
	}
	h, ok := l.Held[op.Name]
	if !ok || h.Expires <= now {
		return Reply{}
	}
	return Reply{Holder: h, OK: true}
}

// lock gives the lock name to owner with a new token, until expires, if
// no one holds it, or its lease expired at now, and tells the holder of
// the node of owner, and of the one that lost it. Its holder asking again
// renews it, with the same token.
func (l Locks) lock(name, owner string, expires, now int64) (Locks, Reply, []actor.Effect) {
	h, held := l.Held[name]
	switch {
	case held && h.Owner == owner:
		h.Expires = max(h.Expires, expires)
		l.Held = with(l.Held, name, h)
		return l, Reply{Holder: h, OK: true}, nil
	case held && h.Expires > now:
		return l, Reply{Holder: h}, nil
	}
	var effs []actor.Effect
	if held {
		effs = append(effs, actor.Send{To: holderAt(h.Owner), Msg: Lost{name, h.Token}})
	}
	l.Last++
	h = Holder{Owner: owner, Token: l.Last, Expires: expires}
	l.Held = with(l.Held, name, h)
	return l, Reply{Holder: h, OK: true}, append(effs, actor.Send{To: holderAt(owner), Msg: Granted{name, h.Token}})
}

// unlock frees the lock name, if owner holds it, and tells the holder of
// the node of owner.
func (l Locks) unlock(name, owner string) (Locks, Reply, []actor.Effect) {
	h, ok := l.Held[name]
	if !ok || h.Owner != owner {
		return l, Reply{Holder: h}, nil
	}
	l.Held = without(l.Held, name)
	return l, Reply{OK: true}, actor.Do(actor.Send{To: holderAt(owner), Msg: Released{name, h.Token}})
}

// Service is the lock service on Nodes, its locks spread over Groups
// groups by their names: each group decides on its own, its leader on any
// of the nodes, so that the work spreads over them.
type Service struct {
	Nodes  []string
	Groups int
	Lease  time.Duration // of a lock, renewed by the holder of its node
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

// Lock has owner take the lock name, for a lease; owner holding it, it
// renews the lease.
func (s Service) Lock(ctx context.Context, caller actor.Caller, name, owner string) (Reply, error) {
	return quorum.Command[Reply](ctx, caller, s.groupOf(name), s.Nodes, Op{Kind: Acquire, Name: name, Owner: owner, Lease: s.Lease})
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

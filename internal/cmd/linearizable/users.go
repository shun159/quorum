package main

import (
	"context"
	"slices"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/proc"
	"github.com/shun159/quorum"
	"github.com/shun159/quorum/internal/register"
)

// user is a client of the register, with a session of its own, writing or
// reading in turn, one at a time. It keeps the history of what it asked
// and what came of it. Each user writes numbers of its own: user i of
// count, i+1, then i+1+count, and so on.
type user struct {
	genserver.Default[history]
	nodes   []string
	i       int
	count   int
	timeout time.Duration
	clock   func() int64
}

// history is what a user asked, and what came of it, with its client.
type history struct {
	client quorum.Client[int, int]
	ops    []register.Op
}

type (
	opTag      struct{ i int }
	nextOp     struct{}
	historyReq struct{}
)

func (u user) Init(proc.PID) (history, []actor.Effect, error) {
	return history{client: quorum.NewClient[int, int](group, u.nodes, u.timeout)}, actor.Do(u.later()), nil
}

// later has the user do its next operation in a while.
func (u user) later() actor.Effect {
	return actor.StartTimer{Key: nextOp{}, After: 10 * time.Millisecond, Msg: nextOp{}}
}

func (u user) HandleInfo(h history, msg any) (history, []actor.Effect) {
	if _, ok := msg.(nextOp); ok {
		return u.call(h)
	}
	var effs []actor.Effect
	var res *quorum.Result[int]
	h.client, effs, res = h.client.Handle(msg)
	if res != nil {
		return u.returned(h, *res)
	}
	return h, effs
}

func (u user) HandleCall(h history, _ historyReq, from genserver.From[[]register.Op]) (history, []actor.Effect) {
	return h, actor.Do(from.Reply(h.ops))
}

// call has the user write a new number, or read, in turn.
func (u user) call(h history) (history, []actor.Effect) {
	o := register.Op{User: u.i, Call: u.clock(), Read: len(h.ops)%2 == 1}
	tag := opTag{len(h.ops)}
	var effs []actor.Effect
	if o.Read {
		h.client, effs = h.client.Read(tag, 0)
	} else {
		o.Value = u.i + 1 + len(h.ops)/2*u.count
		h.client, effs = h.client.Command(tag, o.Value)
	}
	h.ops = append(h.ops, o)
	return h, effs
}

// returned keeps what came of an operation, and has the user do the next.
func (u user) returned(h history, res quorum.Result[int]) (history, []actor.Effect) {
	h.ops = slices.Clone(h.ops)
	o := &h.ops[res.Tag.(opTag).i]
	o.Return, o.Out, o.Done = u.clock(), res.Rep, res.Err == nil
	return h, actor.Do(u.later())
}

// users are the users running.
type users []genserver.Ref[historyReq, []register.Op, genserver.None]

// startUsers starts count users of the group on nodes, each waiting on a
// member for timeout before it asks another, all timing their operations
// by one clock.
func startUsers(n *proc.Node, nodes []string, count int, timeout time.Duration) (users, error) {
	start := time.Now()
	clock := func() int64 { return int64(time.Since(start)) }
	var us users
	for i := range count {
		ref, err := genserver.Start(context.Background(), n, user{nodes: nodes, i: i, count: count, timeout: timeout, clock: clock})
		if err != nil {
			return nil, err
		}
		us = append(us, ref)
	}
	return us, nil
}

// stop returns the history of all the users, and stops them.
func (us users) stop(n *proc.Node) ([]register.Op, error) {
	var all []register.Op
	for _, ref := range us {
		ops, err := ref.Call(context.Background(), n, historyReq{})
		if err != nil {
			return nil, err
		}
		all = append(all, ops...)
		ref.Stop(context.Background(), n)
	}
	return all, nil
}

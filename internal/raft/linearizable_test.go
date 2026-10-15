package raft

import (
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/gensim"
	"github.com/shun159/molecule/proc"
	"github.com/shun159/quorum/internal/register"
)

// user is a client of a register, with a session of its own, writing or
// reading in turn, one at a time. It keeps the history of what it asked
// and what came of it, for the history of all to be checked linearizable.
// Each user writes numbers of its own: user i of count, i+1, then
// i+1+count, and so on.
type user struct {
	genserver.Default[history]
	group   string
	members []string
	i       int
	count   int
	now     func() time.Time
}

// history is what a user asked, and what came of it, with its client.
type history struct {
	client Client[int, int]
	ops    []register.Op
}

// opTag tags an operation, as the index of its op in the history.
type opTag struct{ i int }

type nextOp struct{}

func (u user) WithClock(now func() time.Time) any { u.now = now; return u }

// userTimeout is short, for a user to ask another member, and another,
// while the leader is paused: a member it asks again may be the leader,
// which answers once it resumes, before it learns that another leads.
const userTimeout = 100 * time.Millisecond

func (u user) Init(proc.PID) (history, []actor.Effect, error) {
	return history{client: NewClient[int, int](u.group, u.members, userTimeout)}, actor.Do(u.later()), nil
}

// later has the user do its next operation in a while.
func (u user) later() actor.Effect {
	return actor.StartTimer{Key: nextOp{}, After: tickEvery, Msg: nextOp{}}
}

func (u user) HandleInfo(h history, msg any) (history, []actor.Effect) {
	if _, ok := msg.(nextOp); ok {
		return u.call(h)
	}
	var effs []actor.Effect
	var res *Result[int]
	h.client, effs, res = h.client.Handle(msg)
	if res != nil {
		return u.returned(h, *res)
	}
	return h, effs
}

// call has the user write a new number, or read, in turn.
func (u user) call(h history) (history, []actor.Effect) {
	o := register.Op{User: u.i, Call: u.now().UnixNano(), Read: len(h.ops)%2 == 1}
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
func (u user) returned(h history, res Result[int]) (history, []actor.Effect) {
	h.ops = slices.Clone(h.ops)
	o := &h.ops[res.Tag.(opTag).i]
	o.Return, o.Out, o.Done = u.now().UnixNano(), res.Rep, res.Err == nil
	return h, actor.Do(u.later())
}

// TestLinearizable has users write and read a register, while members are
// cut off or crashed at random, and messages lost, as in TestChaos, the
// leader paused as often, and checks that the history is linearizable.
func TestLinearizable(t *testing.T) {
	for seed := range uint64(30) {
		linearizable(t, seed)
	}
}

func linearizable(t *testing.T, seed uint64, opts ...gensim.Option) {
	t.Helper()
	members := []string{"a", "b", "c", "d", "e"}[:[]int{3, 5}[seed%2]]
	g := newGroupOf(t, seed, members, func(string) Machine[int, int, int] { return register.Machine{} }, opts...)
	g.s.Loss = 0.05
	const count = 5
	var users []proc.PID
	for i := range count {
		users = append(users, spawn(g, genserver.Gen(user{group: g.name, members: members, i: i, count: count}), fmt.Sprint("user", i)))
	}
	g.op = spawn(g, genserver.Gen(operator{group: g.name, members: members}), "operator")

	r := rand.New(rand.NewPCG(seed, 7))
	for range 20 {
		g.recover()
		if l, ok := g.anyLeader(); ok && r.IntN(2) == 0 { // its reads tested
			g.s.Pause(l)
		} else {
			g.disturb(r)
		}
		g.run(time.Duration(200+r.IntN(800)) * time.Millisecond)
	}
	var all []register.Op
	for _, u := range users {
		h, _ := gensim.State[history](g.s, u)
		all = append(all, h.ops...)
	}
	checkLinearizable(t, seed, all)
}

func checkLinearizable(t *testing.T, seed uint64, history []register.Op) {
	t.Helper()
	done, unknown := register.Count(history)
	res, info := register.Check(history, 10*time.Second)
	t.Logf("seed %d: %d operations, %d done, %d unknown: %s", seed, len(history), done, unknown, res)
	if res == porcupine.Ok {
		return
	}
	path := fmt.Sprintf("%s/linearizable-%d.html", os.TempDir(), seed)
	if err := register.Visualize(info, path); err == nil {
		t.Logf("seed %d: see %s", seed, path)
	}
	t.Errorf("seed %d: history %s", seed, res)
}

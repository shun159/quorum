package raft

import (
	"fmt"
	"math"
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
)

// swapper is a register: a command writes its number, replying with the
// number it held before; a read replies with the number it holds.
type swapper struct{}

func (swapper) Init() int { return 0 }

func (swapper) Apply(old, n int) (int, int, []actor.Effect) { return n, old, nil }

func (swapper) Read(n, _ int) int { return n }

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
	ops    []op
}

// op is an operation of a user: a write of value, or a read, called at
// call, returned at ret with out; done false, it ended with an error, or
// did not end, and may have been applied or not.
type op struct {
	user      int
	read      bool
	value     int
	call, ret int64
	out       int
	done      bool
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
	o := op{user: u.i, call: u.now().UnixNano(), read: len(h.ops)%2 == 1}
	tag := opTag{len(h.ops)}
	var effs []actor.Effect
	if o.read {
		h.client, effs = h.client.Read(tag, 0)
	} else {
		o.value = u.i + 1 + len(h.ops)/2*u.count
		h.client, effs = h.client.Command(tag, o.value)
	}
	h.ops = append(h.ops, o)
	return h, effs
}

// returned keeps what came of an operation, and has the user do the next.
func (u user) returned(h history, res Result[int]) (history, []actor.Effect) {
	h.ops = slices.Clone(h.ops)
	o := &h.ops[res.Tag.(opTag).i]
	o.ret, o.out, o.done = u.now().UnixNano(), res.Rep, res.Err == nil
	return h, actor.Do(u.later())
}

// register is the model of the register for porcupine: a write not done
// may have been applied or not, a read not done tells nothing.
var register = porcupine.Model{
	Init: func() any { return 0 },
	Step: func(state, input, output any) (bool, any) {
		o := input.(op)
		done := output.(bool)
		switch {
		case o.read:
			return !done || o.out == state.(int), state
		case !done:
			return true, o.value
		}
		return o.out == state.(int), o.value
	},
	DescribeOperation: func(input, output any) string {
		o := input.(op)
		res := fmt.Sprint(o.out)
		if !output.(bool) {
			res = "?"
		}
		if o.read {
			return "read → " + res
		}
		return fmt.Sprintf("write %d → %s", o.value, res)
	},
}

// operations returns the history for porcupine: an operation not done
// returns after all the others, if ever; a read not done is left out.
func operations(history []op) []porcupine.Operation {
	var ops []porcupine.Operation
	for _, o := range history {
		if o.read && !o.done {
			continue
		}
		ret := o.ret
		if !o.done {
			ret = math.MaxInt64
		}
		ops = append(ops, porcupine.Operation{ClientId: o.user, Input: o, Call: o.call, Output: o.done, Return: ret})
	}
	return ops
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
	g := newGroupOf(t, seed, members, func(string) Machine[int, int, int] { return swapper{} }, opts...)
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
	var all []op
	for _, u := range users {
		h, _ := gensim.State[history](g.s, u)
		all = append(all, h.ops...)
	}
	checkLinearizable(t, seed, all)
}

func checkLinearizable(t *testing.T, seed uint64, history []op) {
	t.Helper()
	var done, unknown int
	for _, o := range history {
		if o.done {
			done++
		} else if o.ret != 0 {
			unknown++
		}
	}
	res, info := porcupine.CheckOperationsVerbose(register, operations(history), 10*time.Second)
	t.Logf("seed %d: %d operations, %d done, %d unknown: %s", seed, len(history), done, unknown, res)
	if res == porcupine.Ok {
		return
	}
	path := fmt.Sprintf("%s/linearizable-%d.html", os.TempDir(), seed)
	if err := porcupine.VisualizePath(register, info, path); err == nil {
		t.Logf("seed %d: see %s", seed, path)
	}
	t.Errorf("seed %d: history %s", seed, res)
}

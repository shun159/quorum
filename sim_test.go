package quorum

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genstatem"
	"github.com/shun159/molecule/gensim"
	"github.com/shun159/molecule/proc"
)

// counter adds the numbers it is given, and replies with the sum.
type counter struct{}

func (counter) Init() int { return 0 }

func (counter) Apply(sum, n int) (int, int, []actor.Effect) { return sum + n, sum + n, nil }

// group is a group of counters simulated, and what the test has seen of
// it, to check that Raft keeps its promises at every step.
type group struct {
	t       *testing.T
	s       *gensim.Sim
	name    string
	members []string
	pids    map[string]proc.PID

	leaders   map[uint64]string // the leader seen in each term
	committed raftLog           // the longest log seen committed
}

func newGroup(t *testing.T, seed uint64, members []string, opts ...gensim.Option) *group {
	t.Helper()
	return newGroupOf(t, seed, members, func(string) Machine[int, int, int] { return counter{} }, opts...)
}

// newGroupOf is newGroup with the machine of each member made by machine,
// given its node.
func newGroupOf(t *testing.T, seed uint64, members []string, machine func(node string) Machine[int, int, int], opts ...gensim.Option) *group {
	t.Helper()
	g := &group{
		t: t, s: gensim.New(seed, opts...), name: "g", members: members,
		pids: map[string]proc.PID{}, leaders: map[uint64]string{},
	}
	for _, m := range members {
		spec := Spec[int, int, int]{Group: g.name, Members: members, Machine: machine(m)}
		pid, err := gensim.Start(g.s, Child(spec), gensim.On(m))
		if err != nil {
			t.Fatal(err)
		}
		g.pids[m] = pid
	}
	return g
}

// member returns the role and data of the member on node m, if it runs.
func (g *group) member(m string) (role, replica[int], bool) {
	mach, ok := gensim.State[genstatem.Machine[role, replica[int]]](g.s, g.pids[m])
	return mach.State(), mach.Data(), ok
}

// run runs the simulation for d, checking the group at every step.
func (g *group) run(d time.Duration) {
	g.t.Helper()
	if err := g.s.Run(d, g.check); err != nil {
		g.t.Fatalf("%v\n%s", err, g.s.TraceString())
	}
}

// leader returns the member leading the latest term, if any.
func (g *group) leader() (string, bool) {
	var found string
	var term uint64
	for _, m := range g.members {
		if r, d, ok := g.member(m); ok && r == leader && d.term > term {
			found, term = m, d.term
		}
	}
	return found, found != ""
}

// command has the group add n, as Command would, from outside the
// simulation: it asks the members in turn until the leader applies it.
func (g *group) command(n int) (int, error) {
	g.t.Helper()
	next := g.members[0]
	for i := 1; i <= 100; i++ { // 100 tries outlast elections
		v, err := g.s.Call(actor.Remote{Node: next, Name: replicaName(g.name)}, command{n})
		if err != nil {
			next = g.members[i%len(g.members)]
			continue
		}
		switch r := v.(type) {
		case applied:
			return r.Rep.(int), nil
		case redirect:
			next = r.Leader
			if next == "" {
				next = g.members[i%len(g.members)]
				g.s.Advance(retryAfter)
			}
		default:
			return 0, fmt.Errorf("got %#v", v)
		}
	}
	return 0, fmt.Errorf("no leader took %d", n)
}

// check checks what Raft promises, over the members running.
func (g *group) check() error {
	for _, m := range g.members {
		r, d, ok := g.member(m)
		if !ok {
			continue
		}
		for _, err := range []error{g.checkLeader(m, r, d), g.checkCommitted(m, d), checkApplied(m, d)} {
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// checkLeader checks that a term has one leader at most.
func (g *group) checkLeader(m string, r role, d replica[int]) error {
	if r != leader {
		return nil
	}
	if other, ok := g.leaders[d.term]; ok && other != m {
		return fmt.Errorf("term %d has two leaders: %s and %s", d.term, other, m)
	}
	g.leaders[d.term] = m
	return nil
}

// checkCommitted checks that what is committed never changes: the log a
// member has committed is the start of the longest seen committed, or
// starts with it.
func (g *group) checkCommitted(m string, d replica[int]) error {
	mine := d.log[:d.commit]
	for i := range min(len(mine), len(g.committed)) {
		if mine[i] != g.committed[i] {
			return fmt.Errorf("%s committed %+v at index %d, where %+v was", m, mine[i], i+1, g.committed[i])
		}
	}
	if len(mine) > len(g.committed) {
		g.committed = slices.Clone(mine)
	}
	return nil
}

// checkApplied checks that the state of a member is what it applied.
func checkApplied(m string, d replica[int]) error {
	sum := 0
	for _, e := range d.log[:d.applied] {
		if n, ok := e.Cmd.(int); ok {
			sum += n
		}
	}
	if sum != d.state {
		return fmt.Errorf("%s holds %d, having applied commands adding up to %d", m, d.state, sum)
	}
	return nil
}

var three = []string{"a", "b", "c"}

func TestElection(t *testing.T) {
	g := newGroup(t, 1, three)
	g.run(2 * time.Second)
	l, ok := g.leader()
	if !ok {
		t.Fatal("no leader")
	}
	_, ld, _ := g.member(l)
	for _, m := range three {
		if _, d, _ := g.member(m); d.term != ld.term || d.leader != l {
			t.Errorf("%s: term %d, leader %q; want %d, %q", m, d.term, d.leader, ld.term, l)
		}
	}
}

func TestCommand(t *testing.T) {
	g := newGroup(t, 1, three)
	for i := 1; i <= 5; i++ {
		if sum, err := g.command(1); err != nil || sum != i {
			t.Fatalf("command %d: %d, %v", i, sum, err)
		}
	}
	g.run(time.Second)
	for _, m := range three {
		if _, d, _ := g.member(m); d.state != 5 {
			t.Errorf("%s holds %d, want 5", m, d.state)
		}
	}
}

// The leader crashed, another takes over, with what was committed.
func TestLeaderCrash(t *testing.T) {
	g := newGroup(t, 1, three)
	for range 3 {
		if _, err := g.command(1); err != nil {
			t.Fatal(err)
		}
	}
	old, _ := g.leader()
	g.s.Crash(old)
	g.run(2 * time.Second)
	l, ok := g.leader()
	if !ok || l == old {
		t.Fatalf("leader %q after %s crashed", l, old)
	}
	if sum, err := g.command(1); err != nil || sum != 4 {
		t.Errorf("command after the crash: %d, %v", sum, err)
	}
}

func TestGroupOfOne(t *testing.T) {
	g := newGroup(t, 1, []string{"a"})
	if sum, err := g.command(7); err != nil || sum != 7 {
		t.Fatalf("command: %d, %v", sum, err)
	}
}

// A command of another type is refused, and stops nothing.
func TestWrongCommand(t *testing.T) {
	g := newGroup(t, 1, three)
	g.run(time.Second)
	l, _ := g.leader()
	v, err := g.s.Call(actor.Remote{Node: l, Name: replicaName(g.name)}, command{"one"})
	if r, ok := v.(rejected); err != nil || !ok {
		t.Fatalf("got %#v, %v", r, err)
	}
	if sum, err := g.command(1); err != nil || sum != 1 {
		t.Errorf("command after: %d, %v", sum, err)
	}
}

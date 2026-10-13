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
	specs   map[string]Spec[int, int, int]
	storage map[string]*memStorage

	leaders   map[uint64]string // the leader seen in each term
	committed raftLog           // the longest log seen committed

	due *crashLater // a crash to come, amid the steps of a run
}

// crashLater is a crash of member after so many steps, its storage
// keeping keep of the writes it did not know were durable.
type crashLater struct {
	member      string
	steps, keep int
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
		pids: map[string]proc.PID{}, specs: map[string]Spec[int, int, int]{},
		storage: map[string]*memStorage{}, leaders: map[uint64]string{},
	}
	for _, m := range members {
		g.storage[m] = &memStorage{}
		g.specs[m] = Spec[int, int, int]{Group: g.name, Members: members, Machine: machine(m), Storage: g.storage[m]}
		g.start(m)
	}
	return g
}

// start starts the member on node m.
func (g *group) start(m string) {
	g.t.Helper()
	pid, err := gensim.Start(g.s, Child(g.specs[m]), gensim.On(m))
	if err != nil {
		g.t.Fatal(err)
	}
	g.pids[m] = pid
}

// crash crashes the node m, its storage keeping the first keep writes it
// had not learned were durable.
func (g *group) crash(m string, keep int) {
	g.s.Crash(m)
	g.storage[m].crash(keep)
}

// crashIfDue counts a step down to the crash due, and crashes then.
func (g *group) crashIfDue() {
	if g.due == nil {
		return
	}
	if g.due.steps--; g.due.steps <= 0 {
		g.crash(g.due.member, g.due.keep)
		g.due = nil
	}
}

// restart restarts the node m, crashed, and its member, with what it saved.
func (g *group) restart(m string) {
	g.t.Helper()
	g.s.Restart(m)
	g.start(m)
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

// check checks what Raft promises, over the members running. It is
// called at every step, where a crash due comes, writes in flight.
func (g *group) check() error {
	g.crashIfDue()
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

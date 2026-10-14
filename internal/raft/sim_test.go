package raft

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

// Read returns the sum, whatever it is given.
func (counter) Read(sum, _ int) int { return sum }

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
	committed []Entry           // what was seen committed, from index 1
	states    map[uint64]int    // the state of the machine seen at each index applied

	due *crashLater // a crash to come, amid the steps of a run
	op  proc.PID    // the operator swapping members, if any
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
	g := unfounded(t, seed, members, machine, opts...)
	g.found()
	return g
}

// unfounded starts the members of a group, on blank storages, without
// founding it.
func unfounded(t *testing.T, seed uint64, members []string, machine func(node string) Machine[int, int, int], opts ...gensim.Option) *group {
	t.Helper()
	g := &group{
		t: t, s: gensim.New(seed, opts...), name: "g", members: members,
		pids: map[string]proc.PID{}, specs: map[string]Spec[int, int, int]{},
		storage: map[string]*memStorage{}, leaders: map[uint64]string{}, states: map[uint64]int{},
	}
	for _, m := range members {
		g.storage[m] = &memStorage{}
		g.specs[m] = Spec[int, int, int]{Group: g.name, Machine: machine(m), Storage: g.storage[m], SnapshotEvery: snapshotEvery}
		g.start(m)
	}
	return g
}

// found founds the group with its members, as Found does, from outside
// the simulation.
func (g *group) found() {
	g.t.Helper()
	var config Config
	for _, m := range g.members {
		config.Voters = append(config.Voters, g.identify(m))
	}
	for _, m := range g.members {
		if v, err := g.s.Call(g.at(m), found{config}); err != nil || v != (applied{}) {
			g.t.Fatalf("founding on %s: %v, %v", m, v, err)
		}
	}
}

// at is where the member on node m is.
func (g *group) at(m string) actor.Remote {
	return actor.Remote{Node: m, Name: replicaName(g.name)}
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

// loseDisk crashes the node m, its storage lost: restarted, it starts on
// a blank one, as on a new disk.
func (g *group) loseDisk(m string) {
	g.s.Crash(m)
	g.storage[m] = &memStorage{}
	spec := g.specs[m]
	spec.Storage = g.storage[m]
	g.specs[m] = spec
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

// leader returns the member leading the latest term: the test fails if
// none does.
func (g *group) leader() string {
	g.t.Helper()
	l, ok := g.anyLeader()
	if !ok {
		g.t.Fatal("no leader")
	}
	return l
}

// anyLeader returns the member leading the latest term, if any.
func (g *group) anyLeader() (string, bool) {
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
// simulation.
func (g *group) command(n int) (int, error) {
	g.t.Helper()
	v, err := g.ask(command{Cmd: n})
	if err != nil {
		return 0, err
	}
	return v.(applied).Rep.(int), nil
}

// ask sends req to the leader, as ask does, from outside the simulation:
// it asks the members in turn until the leader replies, done.
func (g *group) ask(req any) (any, error) {
	g.t.Helper()
	next := g.members[0]
	for i := 1; i <= 100; i++ { // 100 tries outlast elections
		v, err := g.s.Call(g.at(next), req)
		if err != nil {
			next = g.members[i%len(g.members)]
			continue
		}
		switch r := v.(type) {
		case applied:
			return r, nil
		case redirect:
			if next = r.Leader; next != "" {
				continue
			}
			next = g.members[i%len(g.members)]
		case busy:
		default:
			return nil, fmt.Errorf("got %#v", v)
		}
		g.s.Advance(retryAfter)
	}
	return nil, fmt.Errorf("no leader took %#v", req)
}

// identify returns who the member on node m is.
func (g *group) identify(m string) proc.PID {
	g.t.Helper()
	v, err := g.s.Call(g.at(m), identify{})
	if err != nil {
		g.t.Fatal(err)
	}
	return v.(identity).ID
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
		for _, err := range []error{g.checkLeader(m, r, d), g.checkCommitted(m, d), g.checkApplied(m, d)} {
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

// checkCommitted checks that what is committed never changes: each entry
// a member has committed is the one seen committed at its index before,
// if any. A member's entries in its snapshot it does not have.
func (g *group) checkCommitted(m string, d replica[int]) error {
	for k, e := range d.log.upTo(d.commit) {
		i := d.log.start + uint64(k) // the index of e, less one
		switch {
		case i < uint64(len(g.committed)):
			if !sameEntry(e, g.committed[i]) {
				return fmt.Errorf("%s committed %+v at index %d, where %+v was", m, e, i+1, g.committed[i])
			}
		case i == uint64(len(g.committed)):
			g.committed = append(g.committed, e)
		}
	}
	return nil
}

// sameEntry reports whether a and b are the same entry, their
// configurations compared by what they hold, as each member has its own.
func sameEntry(a, b Entry) bool {
	if a.Term != b.Term || a.Cmd != b.Cmd || (a.Config == nil) != (b.Config == nil) {
		return false
	}
	return a.Config == nil ||
		slices.Equal(a.Config.Voters, b.Config.Voters) && slices.Equal(a.Config.Learners, b.Config.Learners)
}

// checkApplied checks that the members agree on the machine: one that
// applied the entries up to an index holds what every other did there.
func (g *group) checkApplied(m string, d replica[int]) error {
	if state, ok := g.states[d.applied]; ok && state != d.state {
		return fmt.Errorf("%s holds %d, having applied up to %d, where another held %d", m, d.state, d.applied, state)
	}
	g.states[d.applied] = d.state
	return nil
}

var three = []string{"a", "b", "c"}

// snapshotEvery is small in the simulations, for the members to take
// snapshots, and send them, all the time.
const snapshotEvery = 4

func TestElection(t *testing.T) {
	g := newGroup(t, 1, three)
	g.run(2 * time.Second)
	l := g.leader()
	_, ld, _ := g.member(l)
	for _, m := range three {
		if _, d, _ := g.member(m); d.term != ld.term || d.leader.Node() != l {
			t.Errorf("%s: term %d, leader %v; want %d, %s", m, d.term, d.leader, ld.term, l)
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
	old := g.leader()
	g.s.Crash(old)
	g.run(2 * time.Second)
	l := g.leader()
	if l == old {
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
	l := g.leader()
	v, err := g.s.Call(actor.Remote{Node: l, Name: replicaName(g.name)}, command{Cmd: "one"})
	if r, ok := v.(rejected); err != nil || !ok {
		t.Fatalf("got %#v, %v", r, err)
	}
	if sum, err := g.command(1); err != nil || sum != 1 {
		t.Errorf("command after: %d, %v", sum, err)
	}
}

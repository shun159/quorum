package raft

import (
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/gen"
	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/gensim"
	"github.com/shun159/molecule/proc"
)

// client adds 1 to a group of counters, or reads the count, every tick,
// in turn, one at a time, and counts what comes of them. It works for a
// while, then rests for as long, for the group to go quiet amid faults.
type client struct {
	genserver.Default[outcomes]
	group   string
	members []string
}

// outcomes are what a client knows of its commands.
type outcomes struct {
	quorum  Client[int, int]
	acked   int // applied, the leader said
	unknown int // ended with an error: applied or not
	asked   int // commands and reads sent
	ticks   int

	reads int   // answered
	floor int   // the adds acknowledged when the read out was sent
	wrong []int // reads answered not between floor and the adds sent
}

type tick struct{}

const (
	tickEvery = 50 * time.Millisecond
	workTicks = 20 // then rests as many
)

func (c client) Init(proc.PID) (outcomes, []actor.Effect, error) {
	o := outcomes{quorum: NewClient[int, int](c.group, c.members, time.Second)}
	return o, actor.Do(nextTick()), nil
}

func nextTick() actor.Effect {
	return actor.StartTimer{Key: tick{}, After: tickEvery, Msg: tick{}}
}

func (c client) HandleInfo(o outcomes, msg any) (outcomes, []actor.Effect) {
	if _, ok := msg.(tick); ok {
		return c.tick(o)
	}
	var effs []actor.Effect
	var res *Result[int]
	o.quorum, effs, res = o.quorum.Handle(msg)
	if res != nil {
		o = c.count(o, *res)
	}
	return o, effs
}

// tick sends the next command or read, unless one is out.
func (c client) tick(o outcomes) (outcomes, []actor.Effect) {
	effs := actor.Do(nextTick())
	if o.ticks++; o.quorum.Out() > 0 || (o.ticks/workTicks)%2 == 1 {
		return o, effs
	}
	var ask []actor.Effect
	if o.asked++; o.asked%2 == 0 {
		o.floor = o.acked
		o.quorum, ask = o.quorum.Read("read", 0)
	} else {
		o.quorum, ask = o.quorum.Command("add", 1)
	}
	return o, append(effs, ask...)
}

// count counts what came of a command or a read. A read must have every
// add acknowledged before it, and no more than were sent.
func (c client) count(o outcomes, res Result[int]) outcomes {
	if res.Tag == "read" {
		if res.Err == nil {
			o.reads++
			if res.Rep < o.floor || res.Rep > o.acked+o.unknown {
				o.wrong = append(o.wrong, res.Rep)
			}
		}
		return o
	}
	if res.Err == nil {
		o.acked++
	} else {
		o.unknown++
	}
	return o
}

// TestChaos has a client add to a group of three or five, while members
// are cut off or crashed at random, and messages lost, for many seeds.
func TestChaos(t *testing.T) {
	for seed := range uint64(30) {
		chaos(t, seed)
	}
}

// FuzzChaos is TestChaos with the order of events searched by the fuzzer,
// the faults still those of the seed.
func FuzzChaos(f *testing.F) {
	f.Add(uint64(0), []byte{})
	f.Add(uint64(1), []byte{3, 1, 4, 1, 5, 9, 2, 6})
	f.Fuzz(func(t *testing.T, seed uint64, schedule []byte) {
		chaos(t, seed, gensim.WithChoices(schedule))
	})
}

// chaos runs a group under partitions, crashes, disks replaced and lost
// messages, of three members or five as the seed has it, checking it at
// every step. Once healed and idle, the members all hold the same count,
// which has every command acknowledged, and no more than were sent;
// every node's member is a voter; and the group is quiet.
func chaos(t *testing.T, seed uint64, opts ...gensim.Option) {
	t.Helper()
	// Three are the most fragile: one member losing a write may lose what
	// was committed. Five see two members cut off at once.
	members := []string{"a", "b", "c", "d", "e"}[:[]int{3, 5}[seed%2]]
	g := newGroup(t, seed, members, opts...)
	g.s.Loss = 0.05
	cl := spawn(g, genserver.Gen(client{group: g.name, members: members}), "client")
	g.op = spawn(g, genserver.Gen(operator{group: g.name, members: members}), "operator")

	r := rand.New(rand.NewPCG(seed, 5))
	for range 20 {
		g.recover()
		g.disturb(r)
		g.run(time.Duration(200+r.IntN(800)) * time.Millisecond)
	}
	// The client stops, its count kept, for the group to settle.
	o, _ := gensim.State[outcomes](g.s, cl)
	g.s.Exit(cl, proc.Kill)
	g.recover()
	g.s.Loss = 0
	g.run(5 * time.Second)

	g.checkCount(seed, o)
	g.checkSwapped(seed)
	if !g.quietAll() {
		g.dump()
		g.t.Fatalf("seed %d: settled, not quiet", seed)
	}
}

// dump logs what each member is at.
func (g *group) dump() {
	for _, m := range g.members {
		r, d, ok := g.member(m)
		g.t.Logf("%s: %v, running %v, id %v, term %d, leader %v, quiet %v, commit %d applied %d of %d, peers %+v, config %+v",
			m, r, ok, d.id, d.term, d.leader, d.quiet, d.commit, d.applied, d.log.lastIndex(), d.peers, d.config)
	}
}

// sameMembers reports whether a and b hold the same members, in any order.
func sameMembers(a, b []proc.PID) bool {
	byID := func(x, y proc.PID) int { return strings.Compare(x.String(), y.String()) }
	return slices.Equal(slices.SortedFunc(slices.Values(a), byID), slices.SortedFunc(slices.Values(b), byID))
}

// spawn spawns b on node, in the simulation of g.
func spawn[S any](g *group, b gen.Behaviour[S], node string) proc.PID {
	g.t.Helper()
	pid, err := gensim.Spawn(g.s, b, nil, gensim.On(node))
	if err != nil {
		g.t.Fatal(err)
	}
	return pid
}

// checkCount checks that the members all hold the same count: every add
// applied once, those acknowledged and, maybe, the one still out. In its
// session, no add ends unknown.
func (g *group) checkCount(seed uint64, o outcomes) {
	g.t.Helper()
	_, d, _ := g.member(g.members[0])
	for _, m := range g.members {
		if _, dm, _ := g.member(m); dm.state != d.state {
			g.dump()
			g.t.Fatalf("seed %d: %s holds %d, %s %d", seed, m, dm.state, g.members[0], d.state)
		}
	}
	if o.unknown > 0 {
		g.t.Fatalf("seed %d: %d adds ended unknown", seed, o.unknown)
	}
	if out := o.quorum.Out(); d.state < o.acked || d.state > o.acked+out {
		g.t.Fatalf("seed %d: count %d, with %d acknowledged and %d out", seed, d.state, o.acked, out)
	}
	if o.acked == 0 || o.reads == 0 {
		g.t.Errorf("seed %d: %d acknowledged, %d read", seed, o.acked, o.reads)
	}
	if len(o.wrong) > 0 {
		g.t.Errorf("seed %d: read %v, of %d acknowledged and %d unknown", seed, o.wrong, o.acked, o.unknown)
	}
}

// checkSwapped checks that the operator is done swapping, and that the
// members of all nodes, the new ones included, are the voters.
func (g *group) checkSwapped(seed uint64) {
	g.t.Helper()
	s, _ := gensim.State[swapping](g.s, g.op)
	if s.step != idle {
		g.t.Fatalf("seed %d: swapping %s still, at step %d", seed, s.node, s.step)
	}
	var ids []proc.PID
	for _, m := range g.members {
		_, d, _ := g.member(m)
		ids = append(ids, d.id)
	}
	_, d, _ := g.member(g.leader())
	if len(d.config.Learners) > 0 || !sameMembers(d.config.Voters, ids) {
		g.t.Fatalf("seed %d: members %v, after %d swaps, but configuration %+v", seed, ids, s.swaps, d.config)
	}
}

// disturb may cut a member or two off the others, the connections lost
// or silent; or crash one amid the steps to come, its storage keeping
// some of the writes it did not know were durable; or replace the disk of
// one, for the operator to swap its member; or have the leader hand over
// to one; or pause one, the leader more often than not.
func (g *group) disturb(r *rand.Rand) {
	members := g.members
	switch r.IntN(8) {
	case 0: // all well
	case 1:
		i := r.IntN(len(members))
		g.s.Partition(members[i:i+1], without(members, i))
	case 2:
		perm := r.Perm(len(members))
		minority := []string{members[perm[0]], members[perm[1]]}
		var majority []string
		for _, i := range perm[2:] {
			majority = append(majority, members[i])
		}
		g.s.Partition(minority, majority)
	case 3:
		g.due = &crashLater{member: members[r.IntN(len(members))], steps: r.IntN(300), keep: r.IntN(3)}
	case 4:
		g.replaceDisk(members[r.IntN(len(members))])
	case 5:
		if l, ok := g.anyLeader(); ok {
			g.s.Call(g.at(l), transferLeader{To: members[r.IntN(len(members))]})
		}
	case 6:
		i := r.IntN(len(members))
		g.s.Silence(members[i:i+1], without(members, i))
	case 7:
		m := members[r.IntN(len(members))]
		if l, ok := g.anyLeader(); ok && r.IntN(2) == 0 {
			m = l
		}
		g.s.Pause(m)
	}
}

// replaceDisk restarts node m on a new disk, and has the operator swap its
// member, unless it is swapping another.
func (g *group) replaceDisk(m string) {
	if s, _ := gensim.State[swapping](g.s, g.op); s.step != idle {
		return
	}
	g.loseDisk(m)
	g.restart(m)
	g.s.Cast(g.op, swap{m})
}

// recover heals the partitions and silences, resumes the members paused,
// and restarts those crashed.
func (g *group) recover() {
	g.due = nil
	g.s.Heal()
	for _, m := range g.members {
		g.s.Resume(m)
	}
	for _, m := range g.members {
		if _, _, ok := g.member(m); !ok {
			g.restart(m)
		}
	}
}

func without(members []string, i int) []string {
	return append(append([]string{}, members[:i]...), members[i+1:]...)
}

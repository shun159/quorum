package raft

import (
	"slices"
	"testing"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genstatem"
	"github.com/shun159/molecule/gensim"
	"github.com/shun159/molecule/proc"
)

// busySince counts what the members did after the step from of the
// trace: the messages they sent each other, and their timers fired.
func (g *group) busySince(from int) int {
	count := 0
	for _, e := range g.s.Trace()[from:] {
		if (e.Kind == gensim.Sent && !e.From.IsZero()) || e.Kind == gensim.Fired {
			count++
		}
	}
	return count
}

// quietAll reports whether every member running is quiet.
func (g *group) quietAll() bool {
	for _, m := range g.members {
		if _, d, ok := g.member(m); ok && !d.quiet {
			return false
		}
	}
	return true
}

// An idle group goes quiet: its members send nothing, nor wake, however
// long. A command wakes it, and it goes quiet again.
func TestQuiet(t *testing.T) {
	g := newGroup(t, 1, three)
	g.run(time.Second)
	if !g.quietAll() {
		t.Fatal("idle, not quiet")
	}
	from := len(g.s.Trace())
	g.run(time.Minute)
	if n := g.busySince(from); n != 0 {
		t.Fatalf("quiet, %d messages and timers in a minute", n)
	}

	if sum, err := g.command(5); err != nil || sum != 5 {
		t.Fatalf("command: %d, %v", sum, err)
	}
	if v, err := g.ask(read{0}); err != nil || v.(applied).Rep != 5 {
		t.Fatalf("read: %#v, %v", v, err)
	}
	g.run(time.Second)
	if !g.quietAll() {
		t.Error("not quiet again")
	}
	for _, m := range g.members {
		if _, d, _ := g.member(m); d.state != 5 {
			t.Errorf("%s holds %d", m, d.state)
		}
	}
}

// A group whose Quiet is negative never goes quiet.
func TestQuietNever(t *testing.T) {
	g := unfounded(t, 1, three, func(string) Machine[int, int, int] { return counter{} })
	for _, m := range three {
		spec := g.specs[m]
		spec.Quiet = -1
		g.specs[m] = spec
		g.s.Exit(g.pids[m], proc.Kill)
		g.start(m)
	}
	g.found()
	g.run(5 * time.Second)
	for _, m := range g.members {
		if _, d, _ := g.member(m); d.quiet {
			t.Errorf("%s quiet", m)
		}
	}
}

// The node of a quiet leader lost, or the leader itself, the others elect
// another, which takes commands.
func TestQuietLeaderLost(t *testing.T) {
	for _, how := range []string{"node", "process"} {
		t.Run(how, func(t *testing.T) {
			g := newGroup(t, 1, three)
			g.run(time.Second)
			old := g.leader()
			if how == "node" {
				g.crash(old, 0)
			} else {
				g.s.Exit(g.pids[old], proc.Kill)
			}
			g.run(2 * time.Second)
			if l := g.leader(); l == old {
				t.Fatalf("%s still leads", old)
			}
			if sum, err := g.command(1); err != nil || sum != 1 {
				t.Fatalf("command: %d, %v", sum, err)
			}
		})
	}
}

// A quiet leader cut off from the others steps down, and they elect
// another; healed, it follows that one, and the group goes quiet again.
func TestQuietPartition(t *testing.T) {
	g := newGroup(t, 1, three)
	g.run(time.Second)
	old := g.leader()
	g.s.Partition([]string{old}, without(three, slices.Index(three, old)))
	g.run(2 * time.Second)
	if r, _, _ := g.member(old); r == leader {
		t.Fatal("cut off, still leads")
	}
	l := g.leader()
	if l == old {
		t.Fatalf("%s leads again", old)
	}
	g.s.Heal()
	if _, err := g.command(1); err != nil {
		t.Fatal(err)
	}
	g.run(2 * time.Second)
	if _, d, _ := g.member(old); d.leader != g.pids[l] && d.leader.Node() != l {
		t.Errorf("%s follows %v, not %s", old, d.leader, l)
	}
	if !g.quietAll() {
		t.Error("not quiet again")
	}
}

// A member restarted, which missed the group going quiet, wakes the
// leader asking for votes, and catches up.
func TestQuietRestart(t *testing.T) {
	g := newGroup(t, 1, three)
	g.run(time.Second)
	l := g.leader()
	f := without(three, slices.Index(three, l))[0]
	g.crash(f, 0)
	g.restart(f)
	g.run(3 * time.Second)
	if g.leader() != l {
		t.Errorf("the leader changed to %s", g.leader())
	}
	if _, d, _ := g.member(f); d.leader.IsZero() || !d.quiet {
		t.Errorf("%s: leader %v, quiet %v", f, d.leader, d.quiet)
	}
}

// A quiet follower refuses others a vote, as it hears its leader, but its
// leader itself: asking, it leads no more.
func TestQuietPreVote(t *testing.T) {
	b, d := member(t)
	d.term, d.leader, d.quiet = 1, ida, true
	ask := func(from proc.PID) preVoteReply {
		t.Helper()
		_, _, effs := b.HandleEvent(follower, d, genstatem.Info{Msg: preVote{Term: 2, Candidate: from, LastIndex: d.log.lastIndex(), LastTerm: d.log.lastTerm()}})
		for _, m := range sent(effs) {
			if r, ok := m.(preVoteReply); ok {
				return r
			}
		}
		t.Fatalf("no reply: %v", effs)
		return preVoteReply{}
	}
	if r := ask(idc); r.Granted {
		t.Error("granted c, hearing a")
	}
	if r := ask(ida); !r.Granted {
		t.Error("refused its leader")
	}
}

// A quiet follower stops following its leader once its node falls
// silent, before any monitor of it fires; another node silent changes
// nothing.
func TestQuietLeaderSilent(t *testing.T) {
	b, d := member(t)
	d.term, d.leader, d.quiet = 1, ida, true
	r, after, _ := b.HandleEvent(follower, d, genstatem.Info{Msg: proc.NodeSilent{Node: idc.Node()}})
	if r != follower || !after.quiet || after.leader != ida {
		t.Errorf("c silent: %v, quiet %v, leader %v", r, after.quiet, after.leader)
	}
	_, after, effs := b.HandleEvent(follower, d, genstatem.Info{Msg: proc.NodeSilent{Node: ida.Node()}})
	if after.quiet || !after.leader.IsZero() || !slices.ContainsFunc(effs, isStateTimeout) {
		t.Errorf("a silent: quiet %v, leader %v, %v", after.quiet, after.leader, effs)
	}
}

func isStateTimeout(e actor.Effect) bool {
	_, ok := e.(genstatem.StartStateTimeout)
	return ok
}

// A member restarted is a process other than the one it first was: led
// by it, the group goes quiet all the same, and stays so.
func TestQuietRestartedLeader(t *testing.T) {
	g := newGroup(t, 1, three)
	g.run(time.Second)
	for range three { // each restarted, the last to lead among them
		l := g.leader()
		g.crash(l, 0)
		g.restart(l)
		g.run(3 * time.Second)
	}
	if !g.quietAll() {
		t.Fatal("not quiet")
	}
	from := len(g.s.Trace())
	g.run(time.Minute)
	if n := g.busySince(from); n != 0 {
		t.Errorf("quiet, %d messages and timers in a minute", n)
	}
}

package raft

import (
	"slices"
	"testing"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genstatem"
)

// The leader, quiet as well, hands over to a follower, which leads the
// next term, with all that was applied.
func TestTransfer(t *testing.T) {
	g := newGroup(t, 1, three)
	if _, err := g.command(5); err != nil {
		t.Fatal(err)
	}
	g.run(time.Second) // quiet
	old := g.leader()
	to := without(three, slices.Index(three, old))[0]
	if v, err := g.ask(transferLeader{To: to}); err != nil || v != (applied{}) {
		t.Fatalf("transfer: %#v, %v", v, err)
	}
	g.run(time.Second)
	if l := g.leader(); l != to {
		t.Fatalf("%s leads, not %s", l, to)
	}
	if sum, err := g.command(1); err != nil || sum != 6 {
		t.Errorf("command: %d, %v", sum, err)
	}
}

// A follower behind is sent what it misses, then handed over to.
func TestTransferBehind(t *testing.T) {
	g := newGroup(t, 1, three)
	g.run(time.Second)
	old := g.leader()
	to := without(three, slices.Index(three, old))[0]
	g.s.Partition([]string{to}, without(three, slices.Index(three, to)))
	for range 5 {
		if _, err := g.command(1); err != nil {
			t.Fatal(err)
		}
	}
	g.s.Heal()
	if v, err := g.ask(transferLeader{To: to}); err != nil || v != (applied{}) {
		t.Fatalf("transfer: %#v, %v", v, err)
	}
	g.run(time.Second)
	if l := g.leader(); l != to {
		t.Fatalf("%s leads, not %s", l, to)
	}
	if _, d, _ := g.member(to); d.state != 5 {
		t.Errorf("%s holds %d", to, d.state)
	}
}

// A handing over to a voter cut off gives up in ElectionTimeout: the
// leader leads on, and takes commands again.
func TestTransferTimeout(t *testing.T) {
	g := newGroup(t, 1, three)
	g.run(time.Second)
	old := g.leader()
	to := without(three, slices.Index(three, old))[0]
	g.s.Partition([]string{to}, without(three, slices.Index(three, to)))
	if v, err := g.ask(transferLeader{To: to}); err == nil {
		t.Fatalf("transfer to a voter cut off: %#v", v)
	}
	if l := g.leader(); l != old {
		t.Fatalf("%s leads, not %s", l, old)
	}
	if _, err := g.command(1); err != nil {
		t.Error(err)
	}
}

// Handing over to a node of no voter is refused; to the leader itself,
// done at once.
func TestTransferWhom(t *testing.T) {
	g := newGroup(t, 1, three)
	g.run(time.Second)
	if _, err := g.ask(transferLeader{To: "z"}); err == nil {
		t.Error("handed over to z")
	}
	if v, err := g.ask(transferLeader{To: g.leader()}); err != nil || v != (applied{}) {
		t.Errorf("to itself: %#v, %v", v, err)
	}
}

// A leader handing over takes no command, nor change of members: they
// wait.
func TestTransferBusy(t *testing.T) {
	b, d := leading(t)
	d.transfer = &transferring{to: ida, from: reader}
	for _, req := range []any{command{Cmd: 1}, openSession{}, removeMember{Node: "c"}} {
		_, _, effs := b.HandleEvent(leader, d, genstatem.Call{From: reader, Req: req})
		if len(effs) != 1 || effs[0] != (actor.Reply{To: reader, Value: busy{}}) {
			t.Errorf("%#v: %v", req, effs)
		}
	}
}

// The leader tells the voter it hands over to to stand only once it holds
// all its log; before, it sends it what it misses.
func TestHandOver(t *testing.T) {
	b, d := leading(t)
	d.log = d.log.add(Entry{Term: 1})
	d.logChanged()
	_, d, effs := b.HandleEvent(leader, d, genstatem.Call{From: reader, Req: transferLeader{To: ida.Node()}})
	if msgs := sent(effs); len(msgs) != 1 || !isAppend(msgs[0]) {
		t.Fatalf("a behind: %v", effs)
	}
	caughtUp := appendReply{Term: 1, From: ida, Success: true, Match: 3}
	_, _, effs = b.HandleEvent(leader, d, genstatem.Info{Msg: caughtUp})
	if !slices.Contains(sent(effs), any(timeoutNow{Term: 1, Leader: idb, To: ida})) {
		t.Errorf("a caught up: %v", effs)
	}
}

func isAppend(m any) bool {
	_, ok := m.(appendEntries)
	return ok
}

// A leader handing over steps down at the next term, answering its client:
// done.
func TestTransferAnswered(t *testing.T) {
	b, d := leading(t)
	_, d, _ = b.HandleEvent(leader, d, genstatem.Call{From: reader, Req: transferLeader{To: ida.Node()}})
	r, d, effs := b.HandleEvent(leader, d, genstatem.Info{Msg: requestVote{Term: 2, Candidate: ida, LastIndex: 2, LastTerm: 1}})
	if r != follower {
		t.Fatalf("still %v", r)
	}
	_, _, enter := b.HandleEvent(r, d, genstatem.Enter[role]{Old: leader})
	if !slices.Contains(append(effs, enter...), actor.Effect(actor.Reply{To: reader, Value: applied{}})) {
		t.Errorf("stepping down: %v, %v", effs, enter)
	}
}

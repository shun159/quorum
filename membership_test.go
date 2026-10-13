package quorum

import (
	"testing"
	"time"

	"github.com/shun159/molecule/behaviours/genstatem"
	"github.com/shun159/molecule/proc"
)

// Members started blank, never founded, elect no one, and take no command.
func TestUnfounded(t *testing.T) {
	g := unfounded(t, 1, three, func(string) Machine[int, int, int] { return counter{} })
	g.run(2 * time.Second)
	if l, ok := g.leader(); ok {
		t.Fatalf("%s leads a group never founded", l)
	}
	v, err := g.s.Call(g.at("a"), command{1})
	if err != nil || v != (redirect{}) {
		t.Errorf("command: %#v, %v", v, err)
	}
}

// A group is founded once; founding it again with the same configuration
// says so again, as when Found runs again after failing midway.
func TestFoundOnce(t *testing.T) {
	g := newGroup(t, 1, three)
	_, d, _ := g.member("a")
	if v, err := g.s.Call(g.at("a"), found{d.config}); err != nil || v != (applied{}) {
		t.Errorf("founding again alike: %#v, %v", v, err)
	}
	other := Config{Voters: []proc.PID{d.id}}
	if v, err := g.s.Call(g.at("a"), found{other}); err != nil || v == (applied{}) {
		t.Errorf("founding again otherwise: %#v, %v", v, err)
	}
	if _, err := g.command(1); err != nil {
		t.Fatal(err)
	}
	if v, err := g.s.Call(g.at("a"), found{d.config}); err != nil || v == (applied{}) {
		t.Errorf("founding a group with commands: %#v, %v", v, err)
	}
}

// A member restarted on a new disk is another member, not in the group:
// it neither votes nor stands, nor takes what is sent to the one before
// it, while the group goes on without it.
func TestNewDisk(t *testing.T) {
	g := newGroup(t, 1, three)
	for range 3 {
		if _, err := g.command(1); err != nil {
			t.Fatal(err)
		}
	}
	_, before, _ := g.member("c")
	g.loseDisk("c")
	g.restart("c")
	g.run(3 * time.Second)

	r, d, _ := g.member("c")
	if d.id == before.id || d.id.IsZero() {
		t.Fatalf("c is %v, as before: %v", d.id, before.id)
	}
	if r != follower || d.log.lastIndex() != 0 || !d.votedFor.IsZero() {
		t.Errorf("c, new: %v, log of %d, voted for %v", r, d.log.lastIndex(), d.votedFor)
	}
	if sum, err := g.command(1); err != nil || sum != 4 {
		t.Errorf("command without c: %d, %v", sum, err)
	}
}

// A member without a storage, restarted, is another member as well.
func TestRestartWithoutStorage(t *testing.T) {
	g := newGroup(t, 1, three)
	for _, m := range three {
		spec := g.specs[m]
		spec.Storage = nil
		g.specs[m] = spec
	}
	g.s.Crash("c")
	g.restart("c")
	g.run(2 * time.Second)
	if _, d, _ := g.member("c"); d.log.lastIndex() != 0 || d.config.has(d.id) {
		t.Errorf("c restarted without storage: log of %d, in %v", d.log.lastIndex(), d.config)
	}
}

// A member not a voter in its configuration refuses its vote, and does
// not stand.
func TestNonVoterRefuses(t *testing.T) {
	b, d := member(t)
	config := Config{Voters: []proc.PID{ida, idc}, Learners: []proc.PID{idb}}
	d.log, d.config = raftLog{{Config: &config}}, config

	_, d2, effs := b.HandleEvent(follower, d, genstatem.Info{Msg: requestVote{Term: 1, Candidate: ida, LastIndex: 1}})
	_, _, effs = b.HandleEvent(follower, d2, genstatem.Info{Msg: memDurable{written(t, effs)}})
	if msgs := sent(effs); len(msgs) != 1 || msgs[0] != (voteReply{Term: 1, From: idb}) {
		t.Errorf("a learner told %v", msgs)
	}
	if r, _, _ := b.HandleEvent(follower, d, genstatem.StateTimeout{}); r != follower {
		t.Errorf("a learner stood: %v", r)
	}
}

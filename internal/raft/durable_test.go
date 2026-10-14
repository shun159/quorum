package raft

import (
	"testing"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genstatem"
	"github.com/shun159/molecule/proc"
)

// The members a, b and c of a group, by their PIDs.
var ida, idb, idc = proc.NewNode("a").NewPID(), proc.NewNode("b").NewPID(), proc.NewNode("c").NewPID()

// member returns the member b of a group of three, founded, its storage
// in memory: a behaviour is pure, its events handled by hand.
func member(t *testing.T) (behaviour[int, int, int], replica[int]) {
	t.Helper()
	spec := Spec[int, int, int]{Group: "g", Machine: counter{}, Storage: &memStorage{}}
	b := behaviour[int, int, int]{spec: spec.withDefaults()}
	_, d, _, err := b.Init(idb)
	if err != nil {
		t.Fatal(err)
	}
	config := Config{Voters: []proc.PID{ida, idb, idc}}
	d.log, d.config = logOf(Entry{Config: &config}), config
	d.saved, d.stable = d.seq, 1 // founded, and saved
	return b, d
}

// sent returns the messages among effs, sent to other members.
func sent(effs []actor.Effect) []any {
	var msgs []any
	for _, e := range effs {
		if s, ok := e.(actor.Send); ok {
			msgs = append(msgs, s.Msg)
		}
	}
	return msgs
}

// written returns the tag of the last write among effs.
func written(t *testing.T, effs []actor.Effect) any {
	t.Helper()
	var tag any
	for _, e := range effs {
		if w, ok := e.(memEffect); ok {
			tag = w.w.tag
		}
	}
	if tag == nil {
		t.Fatal("nothing written")
	}
	return tag
}

// A vote is granted once saved: restarted before, the member could vote
// again in the term, for another.
func TestVoteWaitsForDurable(t *testing.T) {
	b, d := member(t)
	ask := requestVote{Term: 1, Candidate: ida, LastIndex: 1}
	r, d, effs := b.HandleEvent(follower, d, genstatem.Info{Msg: ask})
	if msgs := sent(effs); len(msgs) != 0 {
		t.Fatalf("told %v before the vote was saved", msgs)
	}
	_, _, effs = b.HandleEvent(r, d, genstatem.Info{Msg: memDurable{written(t, effs)}})
	if msgs := sent(effs); len(msgs) != 1 || msgs[0] != (voteReply{Term: 1, From: idb, Granted: true}) {
		t.Errorf("once saved, told %v", msgs)
	}
}

// Entries are acknowledged once saved: the leader counts on the member
// holding them, to commit.
func TestAckWaitsForDurable(t *testing.T) {
	b, d := member(t)
	add := appendEntries{Term: 1, Leader: ida, To: idb, PrevIndex: 1, Entries: []Entry{{Term: 1, Cmd: 1}}}
	r, d, effs := b.HandleEvent(follower, d, genstatem.Info{Msg: add})
	if msgs := sent(effs); len(msgs) != 0 {
		t.Fatalf("told %v before the entries were saved", msgs)
	}
	_, _, effs = b.HandleEvent(r, d, genstatem.Info{Msg: memDurable{written(t, effs)}})
	if msgs := sent(effs); len(msgs) != 1 || msgs[0] != (appendReply{Term: 1, From: idb, Success: true, Match: 2}) {
		t.Errorf("once saved, told %v", msgs)
	}
}

// A candidate asks for votes once its own is saved.
func TestStandWaitsForDurable(t *testing.T) {
	b, d := member(t)
	r, d, effs := b.stand(d)
	if r != candidate || len(sent(effs)) != 0 {
		t.Fatalf("%v, told %v before its vote was saved", r, sent(effs))
	}
	_, _, effs = b.HandleEvent(r, d, genstatem.Info{Msg: memDurable{written(t, effs)}})
	if msgs := sent(effs); len(msgs) != 2 {
		t.Errorf("once saved, told %v", msgs)
	}
}

// The leader counts itself as holding an entry once it is durable.
func TestLeaderCountsItselfOnceDurable(t *testing.T) {
	b, d := member(t)
	d.term = 1
	r, d, effs := b.HandleEvent(leader, d, genstatem.Enter[role]{Old: candidate})
	if d.commit != 0 {
		t.Fatalf("committed %d before its entry was saved", d.commit)
	}
	tag := written(t, effs)
	// One other member holds the entry: with the leader, a majority.
	r, d, _ = b.HandleEvent(r, d, genstatem.Info{Msg: appendReply{Term: 1, From: ida, Success: true, Match: 2}})
	if d.commit != 0 {
		t.Fatalf("committed %d before its entry was saved", d.commit)
	}
	_, d, _ = b.HandleEvent(r, d, genstatem.Info{Msg: memDurable{tag}})
	if d.commit != 2 {
		t.Errorf("commit %d once saved, want 2", d.commit)
	}
}

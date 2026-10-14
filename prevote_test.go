package quorum

import (
	"slices"
	"testing"
	"time"

	"github.com/shun159/molecule/behaviours/genstatem"
	"github.com/shun159/molecule/proc"
)

// A member cut off, then back, does not depose the leader: it asked
// whether the others would vote for it, and they would not, hearing from
// the leader, so it stood in no later term.
func TestRejoin(t *testing.T) {
	g := newGroup(t, 1, three)
	g.run(time.Second)
	l := g.leader()
	_, before, _ := g.member(l)
	i := (slices.Index(three, l) + 1) % len(three)
	g.s.Partition(three[i:i+1], without(three, i))
	g.run(3 * time.Second)
	g.s.Heal()
	g.run(3 * time.Second)

	_, after, _ := g.member(l)
	if now := g.leader(); now != l || after.term != before.term {
		t.Errorf("leader %s in term %d, after %s in term %d", now, after.term, l, before.term)
	}
	if _, d, _ := g.member(three[i]); d.term != before.term || d.leader.Node() != l {
		t.Errorf("%s back: term %d, leader %v", three[i], d.term, d.leader)
	}
}

// A member cut off from the leader only, the others still reaching both,
// does not depose it: they would not vote for it, hearing from the leader.
// It stood, they would, and the leader, unable to reach it, would be
// deposed again and again.
func TestCutFromLeader(t *testing.T) {
	g := newGroup(t, 1, three)
	g.run(time.Second)
	l := g.leader()
	_, before, _ := g.member(l)
	cut := three[(slices.Index(three, l)+1)%len(three)]
	g.s.Partition([]string{cut}, []string{l})
	g.run(5 * time.Second)

	_, after, _ := g.member(l)
	if now := g.leader(); now != l || after.term != before.term {
		t.Errorf("leader %s in term %d, after %s in term %d", now, after.term, l, before.term)
	}
	if sum, err := g.command(1); err != nil || sum != 1 {
		t.Errorf("command: %d, %v", sum, err)
	}
}

// A member answers whether it would vote, changing nothing: yes for a
// later term and a log not behind its own, while it hears from no leader.
func TestPreVote(t *testing.T) {
	b, d := member(t)
	d.term = 2
	ask := preVote{Term: 3, Candidate: ida, LastIndex: 1}
	for _, tc := range []struct {
		name  string
		r     role
		d     func(replica[int]) replica[int]
		ask   preVote
		reply preVoteReply
	}{
		{"granted", follower, nil, ask, preVoteReply{Term: 3, From: idb, Granted: true}},
		{"no later term", follower, nil, preVote{Term: 2, Candidate: ida, LastIndex: 1}, preVoteReply{Term: 2, From: idb}},
		{"log behind", follower, nil, preVote{Term: 3, Candidate: ida}, preVoteReply{Term: 2, From: idb}},
		{"hears the leader", follower, func(d replica[int]) replica[int] { d.leader = idc; return d }, ask, preVoteReply{Term: 2, From: idb}},
		{"leads", leader, nil, ask, preVoteReply{Term: 2, From: idb}},
	} {
		dt := d
		if tc.d != nil {
			dt = tc.d(dt)
		}
		r, after, effs := b.HandleEvent(tc.r, dt, genstatem.Info{Msg: tc.ask})
		if msgs := sent(effs); len(msgs) != 1 || msgs[0] != tc.reply {
			t.Errorf("%s: told %v, want %v", tc.name, msgs, tc.reply)
		}
		if r != tc.r || after.term != dt.term || after.votedFor != dt.votedFor || len(effs) != 1 {
			t.Errorf("%s: changed: %v, term %d, voted for %v, effects %v", tc.name, r, after.term, after.votedFor, effs)
		}
	}
}

// A precandidate asks all the others, the learners too, as a candidate
// does; stands once a majority of the voters would vote for it; and
// follows a member of a later term that refuses.
func TestPreStand(t *testing.T) {
	b, d := member(t)
	config := Config{Voters: []proc.PID{ida, idb}, Learners: []proc.PID{idc}}
	d.log, d.config, d.term = logOf(Entry{Config: &config}), config, 2
	r, d, effs := b.HandleEvent(follower, d, genstatem.StateTimeout{})
	want := preVote{Term: 3, Candidate: idb, LastIndex: 1}
	if msgs := sent(effs); r != precandidate || d.term != 2 || len(msgs) != 2 || msgs[0] != want || msgs[1] != want {
		t.Fatalf("%v in term %d, told %v", r, d.term, msgs)
	}
	if r2, d2, _ := b.HandleEvent(r, d, genstatem.Info{Msg: preVoteReply{Term: 3, From: ida, Granted: true}}); r2 != candidate || d2.term != 3 {
		t.Errorf("a majority would vote: %v in term %d", r2, d2.term)
	}
	if r2, d2, _ := b.HandleEvent(r, d, genstatem.Info{Msg: preVoteReply{Term: 5, From: ida}}); r2 != follower || d2.term != 5 {
		t.Errorf("refused in term 5: %v in term %d", r2, d2.term)
	}
	if r2, d2, _ := b.HandleEvent(r, d, genstatem.Info{Msg: preVoteReply{Term: 2, From: ida}}); r2 != precandidate || d2.term != 2 {
		t.Errorf("refused in term 2: %v in term %d", r2, d2.term)
	}
}

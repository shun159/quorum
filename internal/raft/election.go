package raft

import (
	"slices"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genstatem"
	"github.com/shun159/molecule/proc"
)

// electionTimer starts the timeout after which the member stands for
// election: base, plus up to as much again, drawn anew each time, so that
// the members do not all stand at once.
func (d *replica[S]) electionTimer(base time.Duration) actor.Effect {
	jitter := time.Duration(d.rng.Uint64() % uint64(base))
	return genstatem.StartStateTimeout{After: base + jitter}
}

// stand starts an election for the next term: the voter votes for itself,
// and asks the others for their votes, once its own is saved.
//
// It asks the learners too. One may be a voter by a later configuration
// than the candidate has: it decides by its own. Not asked, it would not
// learn the term, only refusing a candidate after the term had moved on,
// then waiting a whole timeout from there; the candidate, its timer
// started earlier, would stand again first, and again, never giving it a
// vote.
func (b behaviour[S, Cmd, Rep]) stand(d *replica[S]) (role, []actor.Effect) {
	d.term++
	d.votedFor, d.leader = d.id, proc.PID{}
	d.votes = []proc.PID{d.id}
	effs := b.saveState(d)
	if b.won(d) {
		return leader, effs // the only voter
	}
	effs = append(effs, d.electionTimer(b.spec.ElectionTimeout))
	ask := requestVote{Term: d.term, Candidate: d.id, LastIndex: d.log.lastIndex(), LastTerm: d.log.lastTerm()}
	var asks []actor.Effect
	for _, id := range d.others() {
		asks = append(asks, b.send(id, ask))
	}
	return candidate, append(effs, d.after(asks...)...)
}

// requestVote answers a candidate. A member votes once in a term, for a
// candidate whose log its own is not ahead of, and then waits for that
// candidate to win before standing itself. It answers once its vote is
// saved: restarted, it must not vote again in the term.
//
// It answers whatever its own configuration, as Raft's thesis has it
// (4.2.3): the candidate counts the votes of the voters of its own. A
// learner made a voter by entries it misses must vote all the same: the
// candidate holding them may need its vote to lead, and to send them.
func (b behaviour[S, Cmd, Rep]) requestVote(r role, d *replica[S], m requestVote) (role, []actor.Effect) {
	grant := m.Term == d.term &&
		(d.votedFor.IsZero() || d.votedFor == m.Candidate) &&
		d.log.notAheadOf(m.LastIndex, m.LastTerm)
	var effs []actor.Effect
	if grant {
		d.votedFor = m.Candidate
		effs = append(b.saveState(d), d.electionTimer(b.spec.ElectionTimeout))
	}
	reply := b.send(m.Candidate, voteReply{Term: d.term, From: d.id, Granted: grant})
	return r, append(effs, d.after(reply)...)
}

// voteReply counts a vote for the candidate: a majority of the voters
// makes it the leader.
func (b behaviour[S, Cmd, Rep]) voteReply(r role, d *replica[S], m voteReply) (role, []actor.Effect) {
	if r != candidate || m.Term != d.term || !m.Granted || slices.Contains(d.votes, m.From) {
		return r, nil
	}
	d.votes = append(d.votes, m.From)
	if b.won(d) {
		return leader, nil
	}
	return r, nil
}

// won reports whether a majority of the voters voted for the candidate.
func (b behaviour[S, Cmd, Rep]) won(d *replica[S]) bool {
	count := 0
	for _, v := range d.votes {
		if d.config.isVoter(v) {
			count++
		}
	}
	return count >= d.config.majority()
}

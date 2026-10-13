package quorum

import (
	"slices"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genstatem"
)

// electionTimer starts the timeout after which the member stands for
// election: base, plus up to as much again, drawn anew each time, so that
// the members do not all stand at once.
func (d *replica[S]) electionTimer(base time.Duration) actor.Effect {
	jitter := time.Duration(d.rng.Uint64() % uint64(base))
	return genstatem.StartStateTimeout{After: base + jitter}
}

// stand starts an election for the next term: the member votes for
// itself, and asks the others for their votes.
func (b behaviour[S, Cmd, Rep]) stand(d replica[S]) (role, replica[S], []actor.Effect) {
	d.term++
	d.votedFor, d.leader = d.self, ""
	d.votes = []string{d.self}
	effs := b.saveState(&d)
	if b.won(d) {
		return leader, d, effs // a group of one
	}
	effs = append(effs, d.electionTimer(b.spec.ElectionTimeout))
	ask := requestVote{Term: d.term, Candidate: d.self, LastIndex: d.log.lastIndex(), LastTerm: d.log.lastTerm()}
	var asks []actor.Effect
	for _, m := range b.others(d) {
		asks = append(asks, b.send(m, ask))
	}
	return candidate, d, append(effs, d.after(asks...)...)
}

// requestVote answers a candidate. A member votes once in a term, for a
// candidate whose log its own is not ahead of, and then waits for that
// candidate to win before standing itself. It answers once its vote is
// saved: restarted, it must not vote again in the term.
func (b behaviour[S, Cmd, Rep]) requestVote(r role, d replica[S], m requestVote) (role, replica[S], []actor.Effect) {
	grant := m.Term == d.term &&
		(d.votedFor == "" || d.votedFor == m.Candidate) &&
		d.log.notAheadOf(m.LastIndex, m.LastTerm)
	var effs []actor.Effect
	if grant {
		d.votedFor = m.Candidate
		effs = append(b.saveState(&d), d.electionTimer(b.spec.ElectionTimeout))
	}
	reply := b.send(m.Candidate, voteReply{Term: d.term, From: d.self, Granted: grant})
	return r, d, append(effs, d.after(reply)...)
}

// voteReply counts a vote for the candidate: a majority makes it the
// leader.
func (b behaviour[S, Cmd, Rep]) voteReply(r role, d replica[S], m voteReply) (role, replica[S], []actor.Effect) {
	if r != candidate || m.Term != d.term || !m.Granted || slices.Contains(d.votes, m.From) {
		return r, d, nil
	}
	d.votes = append(d.votes, m.From)
	if b.won(d) {
		return leader, d, nil
	}
	return r, d, nil
}

func (b behaviour[S, Cmd, Rep]) won(d replica[S]) bool {
	return len(d.votes) >= b.spec.majority()
}

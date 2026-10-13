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
	if b.won(d) {
		return leader, d, nil // a group of one
	}
	ask := requestVote{Term: d.term, Candidate: d.self, LastIndex: d.log.lastIndex(), LastTerm: d.log.lastTerm()}
	effs := []actor.Effect{d.electionTimer(b.spec.ElectionTimeout)}
	for _, m := range b.others(d) {
		effs = append(effs, b.send(m, ask))
	}
	return candidate, d, effs
}

// requestVote answers a candidate. A member votes once in a term, for a
// candidate whose log its own is not ahead of, and then waits for that
// candidate to win before standing itself.
func (b behaviour[S, Cmd, Rep]) requestVote(r role, d replica[S], m requestVote) (role, replica[S], []actor.Effect) {
	grant := m.Term == d.term &&
		(d.votedFor == "" || d.votedFor == m.Candidate) &&
		d.log.notAheadOf(m.LastIndex, m.LastTerm)
	effs := []actor.Effect{b.send(m.Candidate, voteReply{Term: d.term, From: d.self, Granted: grant})}
	if grant {
		d.votedFor = m.Candidate
		effs = append(effs, d.electionTimer(b.spec.ElectionTimeout))
	}
	return r, d, effs
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

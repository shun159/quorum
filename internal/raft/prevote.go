package raft

import (
	"slices"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/proc"
)

// preStand asks the others whether they would vote for the member in the
// next term, before it stands, as Raft's Pre-Vote has it. Asking changes
// nothing of theirs, and they would not while they hear from a leader: a
// member cut off from the group, back, stands in no later term, which
// would depose the leader for nothing. A majority would, and it stands.
func (b behaviour[S, Cmd, Rep]) preStand(d replica[S]) (role, replica[S], []actor.Effect) {
	d.leader = proc.PID{}
	d.votes = []proc.PID{d.id}
	if b.won(d) {
		return b.stand(d) // the only voter
	}
	ask := preVote{Term: d.term + 1, Candidate: d.id, LastIndex: d.log.lastIndex(), LastTerm: d.log.lastTerm()}
	effs := actor.Do(d.electionTimer(b.spec.ElectionTimeout))
	for _, id := range d.others() {
		effs = append(effs, b.send(id, ask))
	}
	return precandidate, d, effs
}

// preVote answers whether the member would vote for a candidate in the
// term asked about: one later than its own, the candidate's log not
// behind its own, and no leader heard from. It changes nothing, so it need
// not wait for a write.
func (b behaviour[S, Cmd, Rep]) preVote(r role, d replica[S], m preVote) (role, replica[S], []actor.Effect) {
	grant := m.Term > d.term &&
		!d.hearsLeader(r) &&
		d.log.notAheadOf(m.LastIndex, m.LastTerm)
	reply := preVoteReply{Term: d.term, From: d.id, Granted: grant}
	if grant {
		reply.Term = m.Term
	}
	return r, d, actor.Do(b.send(m.Candidate, reply))
}

// hearsLeader reports whether the member hears from a leader: it leads,
// or it follows the leader of its term, its timeout not run out since.
func (d *replica[S]) hearsLeader(r role) bool {
	return r == leader || r == follower && !d.leader.IsZero()
}

// preVoteReply counts a member that would vote for this one, which stands
// once a majority would. A member of a later term refusing makes this one
// a follower in that term.
func (b behaviour[S, Cmd, Rep]) preVoteReply(r role, d replica[S], m preVoteReply) (role, replica[S], []actor.Effect) {
	switch {
	case !m.Granted && m.Term > d.term:
		d.newTerm(m.Term)
		return follower, d, b.saveState(&d)
	case r != precandidate || !m.Granted || m.Term != d.term+1 || slices.Contains(d.votes, m.From):
		return r, d, nil
	}
	d.votes = append(d.votes, m.From)
	if b.won(d) {
		return b.stand(d)
	}
	return r, d, nil
}

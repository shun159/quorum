package quorum

import "github.com/shun159/molecule/actor"

// takeSnapshot has the member take a snapshot of its machine in place of
// the entries it applied, once SnapshotEvery of them are past its last
// snapshot. Each member takes its own; the leader sends its own to a
// member missing entries it has no more.
func (b behaviour[S, Cmd, Rep]) takeSnapshot(d *replica[S]) []actor.Effect {
	if d.applied-d.log.start < b.spec.SnapshotEvery {
		return nil
	}
	d.snapshot = Snapshot{
		Index:  d.applied,
		Term:   d.log.termAt(d.applied),
		Config: d.log.configUpTo(d.applied),
		State:  d.state,
	}
	d.log = d.log.compact(d.snapshot, true)
	return b.saveSnapshot(d)
}

// installSnapshot takes the leader's snapshot, for a member missing
// entries the leader has no more. It replaces the log up to the snapshot,
// and the machine: the entries after it stay if the log goes on with the
// leader's, else none does. The member replies once it is saved.
func (b behaviour[S, Cmd, Rep]) installSnapshot(r role, d replica[S], m installSnapshot) (role, replica[S], []actor.Effect) {
	if m.Term < d.term {
		return r, d, d.after(b.send(m.Leader, appendReply{Term: d.term, From: d.id}))
	}
	r, d.leader = follower, m.Leader
	effs := []actor.Effect{d.electionTimer(b.spec.ElectionTimeout)}
	snap := m.Snapshot
	reply := b.send(m.Leader, appendReply{Term: d.term, From: d.id, Success: true, Match: snap.Index})
	if snap.Index <= d.commit {
		return r, d, append(effs, d.after(reply)...) // committed here already
	}
	keep := d.log.has(snap.Index, snap.Term)
	d.snapshot, d.state = snap, snap.State.(S)
	d.log = d.log.compact(snap, keep)
	d.logChanged()
	d.commit, d.applied = snap.Index, snap.Index
	effs = append(effs, b.saveSnapshot(&d)...)
	if !keep {
		effs = append(effs, b.saveLog(&d, snap.Index+1)...) // none after it
	}
	return r, d, append(effs, d.after(reply)...)
}

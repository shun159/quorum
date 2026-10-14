package raft

import (
	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/proc"
)

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

		Sessions: d.sessions,
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
	d.snapshot, d.state, d.sessions = snap, snap.State.(S), snap.Sessions
	d.log = d.log.compact(snap, keep)
	d.logChanged()
	d.commit, d.applied = snap.Index, snap.Index
	effs = append(effs, b.saveSnapshot(&d)...)
	if !keep {
		effs = append(effs, b.saveLog(&d, snap.Index+1)...) // none after it
	}
	return r, d, append(effs, d.after(reply)...)
}

// snapshotTo is the tag of the monitor of a member the leader sends its
// snapshot to.
type snapshotTo struct{ id proc.PID }

// sendSnapshot sends p the leader's snapshot, unless one is on its way to
// it: a large one takes long to arrive, and sent again each beat until
// then, it would pile up. The leader watches the member meanwhile, to
// send it again if the member dies, the snapshot lost.
func (b behaviour[S, Cmd, Rep]) sendSnapshot(d *replica[S], p peer) []actor.Effect {
	if p.sending != 0 {
		return nil
	}
	d.peer(p.id).sending = d.snapshot.Index
	return actor.Do(
		actor.Monitor{Target: b.at(p.id), Tag: snapshotTo{p.id}},
		b.send(p.id, installSnapshot{Term: d.term, Leader: d.id, To: p.id, Snapshot: d.snapshot}),
	)
}

// snapshotArrived notes that the snapshot on its way to p arrived, once
// m tells p holds the log as far. A reply sent before it arrived does not.
func (p *peer) snapshotArrived(m appendReply) []actor.Effect {
	if p.sending == 0 || max(m.Match, m.LastIndex) < p.sending {
		return nil
	}
	p.sending = 0
	return actor.Do(actor.Demonitor{Tag: snapshotTo{p.id}})
}

// snapshotLost has the leader send its snapshot again, with its next
// beat, to the member it was on its way to, which died.
func (b behaviour[S, Cmd, Rep]) snapshotLost(r role, d replica[S], to snapshotTo) (role, replica[S], []actor.Effect) {
	if r != leader {
		return r, d, nil
	}
	if p := d.peer(to.id); p != nil {
		p.sending = 0
	}
	return r, d, nil
}

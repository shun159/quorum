package raft

import (
	"errors"
	"fmt"

	"github.com/shun159/molecule/actor"
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
	return b.compact(d)
}

var errHead = errors.New("the head of a snapshot past its end")

// installSnapshot takes a chunk of the leader's snapshot, for a member
// missing entries the leader has no more, and tells how much it has; with
// the last, it installs the snapshot.
func (b behaviour[S, Cmd, Rep]) installSnapshot(r role, d replica[S], m installSnapshot) (role, replica[S], []actor.Effect) {
	if m.Term < d.term {
		return r, d, d.after(b.send(m.Leader, appendReply{Term: d.term, From: d.id}))
	}
	r, d.leader = follower, m.Leader
	effs := []actor.Effect{d.electionTimer(b.spec.ElectionTimeout)}
	if m.Index <= d.commit {
		d.incoming = receiving{}
		reply := appendReply{Term: d.term, From: d.id, Success: true, Match: m.Index}
		return r, d, append(effs, d.after(b.send(m.Leader, reply))...) // committed here already
	}
	if has := d.receive(m); has < m.Size {
		ack := snapshotAck{Term: d.term, From: d.id, Index: m.Index, Offset: has}
		return r, d, append(effs, b.send(m.Leader, ack))
	}
	var snap Snapshot
	err := errHead
	if data := d.incoming.data; m.Head <= len(data) {
		snap, err = b.decode(Encoded{Head: data[:m.Head], State: data[m.Head:]})
	}
	d.incoming = receiving{}
	if err != nil {
		return r, d, actor.Do(actor.Stop{Reason: fmt.Errorf("quorum: group %s cannot decode a snapshot: %w", b.spec.Group, err)})
	}
	return r, d, append(effs, b.install(&d, snap)...)
}

// install replaces the log up to snap, and the machine: the entries after
// it stay if the log goes on with the leader's, else none does. The
// member replies once it is saved.
func (b behaviour[S, Cmd, Rep]) install(d *replica[S], snap Snapshot) []actor.Effect {
	keep := d.log.has(snap.Index, snap.Term)
	d.snapshot, d.state, d.sessions = snap, snap.State.(S), snap.Sessions
	d.log = d.log.compact(snap, keep)
	d.logChanged(0)
	d.commit, d.applied = snap.Index, snap.Index
	effs := b.saveSnapshot(d)
	if !keep {
		effs = append(effs, b.saveLog(d, snap.Index+1)...) // none after it
	}
	reply := appendReply{Term: d.term, From: d.id, Success: true, Match: snap.Index}
	return append(effs, d.after(b.send(d.leader, reply))...)
}

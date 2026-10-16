package raft

import (
	"slices"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genstatem"
	"github.com/shun159/molecule/proc"
)

// A group with nothing to do goes quiet, as CockroachDB's ranges do: its
// leader, with nothing to send, its members holding all it holds, beats
// for Spec.Quiet more, then tells them it goes quiet, and beats no more.
// Its followers stop waiting for it to beat: they stand only once its
// node falls silent or down, as dist tells, or it dies, as a monitor of
// it tells. Idle, a group costs no messages, however many there are; the
// nodes tick to each other once.
//
// The leader wakes at a call, and at a member asking for votes, which
// one that missed the leader going quiet does: restarted, or the word
// lost. A quiet leader steps down once its node hears from no majority of
// the voters, as CheckQuorum would have it. A quiet follower refuses the
// votes of others, as it hears its leader, but the leader's own: asking,
// it leads no more.

// watchLeader is the tag of the monitor of the leader by a quiet follower.
type watchLeader struct{}

// mayQuiet reports whether the leader has nothing to send nor to wait
// for: every entry committed, and held by every member.
func (d *replica[S]) mayQuiet() bool {
	if d.commit != d.log.lastIndex() || len(d.pending) > 0 || len(d.reads) > 0 || len(d.joining) > 0 {
		return false
	}
	for _, p := range d.peers {
		if p.match != d.log.lastIndex() {
			return false
		}
	}
	return true
}

// beat is the time for the leader to beat: it beats, unless it has had
// nothing to do for Quiet, when it beats a last time, telling the others
// it goes quiet.
func (b behaviour[S, Cmd, Rep]) beat(d *replica[S]) []actor.Effect {
	if !d.mayQuiet() || b.spec.Quiet < 0 {
		d.idle = 0
		return b.heartbeat(d)
	}
	d.idle++
	if time.Duration(d.idle)*b.spec.Heartbeat < b.spec.Quiet {
		return b.heartbeat(d)
	}
	d.quiet = true
	effs := []actor.Effect{genstatem.CancelTimeout{Name: checkQuorum{}}}
	for _, p := range d.peers {
		effs = append(effs, b.appendTo(d, p)...)
	}
	return effs
}

// wake has a quiet leader beat again, the others heard from as when it
// was elected.
func (b behaviour[S, Cmd, Rep]) wake(d *replica[S]) []actor.Effect {
	if !d.quiet {
		return nil
	}
	d.quiet, d.idle = false, 0
	d.peers = slices.Clone(d.peers)
	for i := range d.peers {
		d.heard(&d.peers[i])
	}
	return append(b.heartbeat(d), b.quorumTimer())
}

// follow has a follower, heard from its leader, wait for it again. Told
// the leader goes quiet, holding all the leader does, it waits no more,
// but watches it.
func (b behaviour[S, Cmd, Rep]) follow(d *replica[S], quiet bool) []actor.Effect {
	switch {
	case quiet:
		d.quiet = true
		return actor.Do(genstatem.CancelStateTimeout{}, actor.Monitor{Target: b.at(d.leader), Tag: watchLeader{}})
	case d.quiet:
		d.quiet = false
		return actor.Do(actor.Demonitor{Tag: watchLeader{}}, d.electionTimer(b.spec.ElectionTimeout))
	}
	return actor.Do(d.electionTimer(b.spec.ElectionTimeout))
}

// unhush has a quiet follower, its leader lost, wait for a leader again,
// knowing none.
func (d *replica[S]) unhush(timer actor.Effect) []actor.Effect {
	d.quiet, d.leader = false, proc.PID{}
	return actor.Do(actor.Demonitor{Tag: watchLeader{}}, timer)
}

// wakeTimer starts the election timeout of a quiet follower that lost its
// leader. It waited for the leader's node to fall silent, or the leader to
// die, already: it waits only up to base more, drawn anew, so that the
// members do not all stand at once.
func (d *replica[S]) wakeTimer(base time.Duration) actor.Effect {
	return genstatem.StartStateTimeout{After: time.Duration(d.rng.Uint64() % uint64(base))}
}

// nodeEvent notes a node falling silent or down, or heard or up again. A
// quiet follower whose leader's node is lost stops following it; a quiet
// leader that hears from no majority steps down.
func (b behaviour[S, Cmd, Rep]) nodeEvent(r role, d *replica[S], node string, lost bool) (role, []actor.Effect) {
	d.rarely().unheard = slices.DeleteFunc(slices.Clone(d.unheard), func(n string) bool { return n == node })
	if !lost {
		return r, nil
	}
	d.rarely().unheard = append(d.rarely().unheard, node)
	switch {
	case r == follower && d.quiet && d.leader.Node() == node:
		return r, d.unhush(d.wakeTimer(b.spec.ElectionTimeout))
	case r == leader && d.quiet && d.hears() < d.config.majority():
		d.quiet, d.leader = false, proc.PID{}
		return follower, nil
	}
	return r, nil
}

// hears counts the voters on nodes the member hears from, itself
// included if it votes.
func (d *replica[S]) hears() int {
	count := 0
	for _, id := range d.config.Voters {
		if id == d.id || !slices.Contains(d.unheard, id.Node()) {
			count++
		}
	}
	return count
}

// leaderDown has a quiet follower whose leader died stop following it:
// the process on the leader's node, the leader known by the PID it first
// had.
func (b behaviour[S, Cmd, Rep]) leaderDown(r role, d *replica[S], m actor.Down) (role, []actor.Effect) {
	if r != follower || !d.quiet || m.PID.Node() != d.leader.Node() {
		return r, nil
	}
	return r, d.unhush(d.wakeTimer(b.spec.ElectionTimeout))
}

// nodeLost reports whether msg tells of a node lost, or found again.
func nodeLost(msg any) (node string, lost, ok bool) {
	switch m := msg.(type) {
	case proc.NodeSilent:
		return m.Node, true, true
	case proc.NodeDown:
		return m.Node, true, true
	case proc.NodeHeard:
		return m.Node, false, true
	case proc.NodeUp:
		return m.Node, false, true
	}
	return "", false, false
}

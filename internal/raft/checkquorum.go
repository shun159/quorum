package raft

import (
	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genstatem"
	"github.com/shun159/molecule/proc"
)

// A leader cut off from a majority keeps leading, as far as Raft goes,
// until it hears of a later term, while the others may have elected
// another: two members would act as leaders, the effects of Elected done
// twice. So the leader checks, as etcd's CheckQuorum does, that it has
// heard from a majority lately, and steps down if not.
//
// It checks checksPerTimeout times per ElectionTimeout, counting the
// members it heard from within the last ElectionTimeout: cut off, it steps
// down at most ElectionTimeout/checksPerTimeout after the others could
// elect another, which takes them ElectionTimeout at least.
const checksPerTimeout = 4

// checkQuorum is the name of the timeout of the check.
type checkQuorum struct{}

func (b behaviour[S, Cmd, Rep]) quorumTimer() actor.Effect {
	return genstatem.StartTimeout{Name: checkQuorum{}, After: b.spec.ElectionTimeout / checksPerTimeout}
}

// checkQuorum has the leader step down if it has not heard from a
// majority within the last ElectionTimeout.
func (b behaviour[S, Cmd, Rep]) checkQuorum(r role, d *replica[S]) (role, []actor.Effect) {
	if r != leader {
		return r, nil
	}
	if d.heardFrom() < d.config.majority() {
		d.leader = proc.PID{}
		return follower, nil
	}
	d.round++
	return r, actor.Do(b.quorumTimer())
}

// heard notes that the leader heard from the member of p, in this round.
func (d *replica[S]) heard(p *peer) { p.heardIn = d.round }

// heardFrom counts the voters the leader heard from in the last
// checksPerTimeout rounds, itself included if it votes. A leader just
// elected counts them all, their votes the latest it heard.
func (d *replica[S]) heardFrom() int {
	count := 0
	if d.config.isVoter(d.id) {
		count++
	}
	for _, p := range d.peers {
		if d.config.isVoter(p.id) && d.round-p.heardIn < checksPerTimeout {
			count++
		}
	}
	return count
}

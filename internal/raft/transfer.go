package raft

import (
	"context"
	"fmt"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genstatem"
	"github.com/shun159/molecule/proc"
)

// The leader hands its place to another voter, as Raft's thesis has it
// (3.10): it takes no new command, waits for the voter to hold all its
// log, then tells it to stand at once, without asking first whether it
// would be voted for. The voter elected, in the next term, the leader
// steps down. Not within ElectionTimeout, the leader takes commands again.

// transferring is a handing over under way: to the member to, for the
// client of from, timeoutNow sent once it caught up.
type transferring struct {
	to   proc.PID
	from actor.From
	told bool
}

// transferTimeout is the name of the timeout of a handing over.
type transferTimeout struct{}

// TransferLeader is quorum.TransferLeader.
func TransferLeader(ctx context.Context, caller actor.Caller, group string, nodes []string, to string) error {
	if _, err := ask(ctx, caller, group, nodes, transferLeader{To: to}); err != nil {
		return err
	}
	for { // the old leader stepped down: wait for the new one to know it leads
		v, err := actor.Call(ctx, caller, actor.Remote{Node: to, Name: replicaName(group)}, whoLeads{})
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if r, ok := v.(redirect); err == nil && ok && r.Leader == to {
			return nil
		}
		if err := pause(ctx, retryAfter); err != nil {
			return err
		}
	}
}

// transferLeader starts handing the place of the leader to the voter on
// node m.To.
func (b behaviour[S, Cmd, Rep]) transferLeader(r role, d replica[S], c genstatem.Call, m transferLeader) (role, replica[S], []actor.Effect) {
	if r != leader {
		return r, d, actor.Do(c.Reply(redirect{Leader: d.leader.Node()}))
	}
	var to proc.PID
	for _, id := range d.config.Voters {
		if id.Node() == m.To {
			to = id
		}
	}
	switch {
	case to.IsZero():
		return r, d, actor.Do(c.Reply(rejected{fmt.Sprintf("quorum: group %s has no voter on %s", b.spec.Group, m.To)}))
	case to == d.id:
		return r, d, actor.Do(c.Reply(applied{}))
	case d.transfer != nil:
		return r, d, actor.Do(c.Reply(busy{}))
	}
	d.transfer = &transferring{to: to, from: c.From}
	effs := actor.Do(genstatem.StartTimeout{Name: transferTimeout{}, After: b.spec.ElectionTimeout})
	return r, d, append(effs, b.handOver(&d)...)
}

// handOver tells the voter the leader hands over to that it may stand,
// once it holds all the leader's log; else it sends it what it misses.
func (b behaviour[S, Cmd, Rep]) handOver(d *replica[S]) []actor.Effect {
	t := d.transfer
	if t == nil || t.told {
		return nil
	}
	p := d.peer(t.to)
	if p == nil {
		return nil
	}
	if p.match < d.log.lastIndex() {
		return b.appendTo(d, *p)
	}
	d.transfer = &transferring{to: t.to, from: t.from, told: true}
	return actor.Do(b.send(t.to, timeoutNow{Term: d.term, Leader: d.id, To: t.to}))
}

// endTransfer ends the handing over, if any, answering its client: the
// leader stepped down, or it took too long.
func (d *replica[S]) endTransfer(answer any) []actor.Effect {
	if d.transfer == nil {
		return nil
	}
	from := d.transfer.from
	d.transfer = nil
	return actor.Do(genstatem.CancelTimeout{Name: transferTimeout{}}, actor.Reply{To: from, Value: answer})
}

// transferTimedOut gives up handing over: the leader takes commands again.
func (b behaviour[S, Cmd, Rep]) transferTimedOut(r role, d replica[S]) (role, replica[S], []actor.Effect) {
	if r != leader {
		return r, d, nil
	}
	return r, d, d.endTransfer(rejected{fmt.Sprintf("quorum: group %s: no other took the lead in time", b.spec.Group)})
}

// timeoutNow has the member stand at once, its leader handing over to it.
func (b behaviour[S, Cmd, Rep]) timeoutNow(r role, d replica[S], m timeoutNow) (role, replica[S], []actor.Effect) {
	if r != follower || m.Term != d.term || m.Leader != d.leader || !d.config.isVoter(d.id) {
		return r, d, nil
	}
	var effs []actor.Effect
	if d.quiet {
		d.quiet = false
		effs = actor.Do(actor.Demonitor{Tag: watchLeader{}})
	}
	r, d, more := b.stand(d)
	return r, d, append(effs, more...)
}

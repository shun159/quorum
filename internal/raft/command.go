package raft

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/gen"
	"github.com/shun159/molecule/behaviours/genstatem"
	"github.com/shun159/molecule/proc"
)

// Child is quorum.Child.
func Child[S, Cmd, Rep any](spec Spec[S, Cmd, Rep]) gen.Child {
	spec = spec.withDefaults()
	return genstatem.Child(behaviour[S, Cmd, Rep]{spec: spec}, actor.WithName(actor.Local(replicaName(spec.Group))))
}

// ErrLeaderChanged is quorum.ErrLeaderChanged.
var ErrLeaderChanged = errors.New("quorum: the leader changed; the command may have been applied or not")

// retryAfter is how long a client waits before asking again, when no
// member knows the leader, during an election, or the leader is busy with
// a change of members.
const retryAfter = 50 * time.Millisecond

// Command is quorum.Command.
func Command[Rep, Cmd any](ctx context.Context, caller actor.Caller, group string, nodes []string, cmd Cmd) (Rep, error) {
	var zero Rep
	v, err := ask(ctx, caller, group, nodes, command{Cmd: cmd})
	if err != nil {
		return zero, err
	}
	rep, _ := v.(applied).Rep.(Rep)
	return rep, nil
}

// ask sends req to the leader of group, found among the members on nodes,
// and returns what it replied once done: applied, or an error. A command
// goes only to a member that just said it leads; see answerOf.
func ask(ctx context.Context, caller actor.Caller, group string, nodes []string, req any) (any, error) {
	_, unsafe := req.(command)
	node := nodes[0]
	for {
		if unsafe {
			var err error
			if node, err = findLeader(ctx, caller, group, nodes, node); err != nil {
				return nil, err
			}
		}
		v, err := actor.Call(ctx, caller, actor.Remote{Node: node, Name: replicaName(group)}, req)
		ans := answerOf(nodes, node, v, err, !unsafe)
		if ans.done {
			return ans.v, ans.err
		}
		node = ans.node
		if ans.pause {
			if err := pause(ctx, retryAfter); err != nil {
				return nil, err
			}
		}
	}
}

// findLeader asks the members on nodes who leads, from the one on node
// on, and returns the node of the first that says it does itself.
func findLeader(ctx context.Context, caller actor.Caller, group string, nodes []string, node string) (string, error) {
	for {
		v, err := actor.Call(ctx, caller, actor.Remote{Node: node, Name: replicaName(group)}, whoLeads{})
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		next, leads, wait := probed(nodes, node, v, err)
		if leads {
			return node, nil
		}
		node = next
		if wait {
			if err := pause(ctx, retryAfter); err != nil {
				return "", err
			}
		}
	}
}

// probed returns what to make of v or err, the answer of the member on
// node to whoLeads: that it leads, or the node to ask next, at once or
// after a pause.
func probed(nodes []string, node string, v any, err error) (next string, leads, wait bool) {
	r, ok := v.(redirect)
	switch {
	case err != nil || !ok:
		return nextOf(nodes, node), false, true // down, or no member there
	case r.Leader == node:
		return node, true, false
	case r.Leader != "":
		return r.Leader, false, false
	}
	return nextOf(nodes, node), false, true // no leader known
}

// answer is what a client makes of a member's answer to a request: it is
// done, with v or err; or to be asked again of the member on node, at
// once or after a pause.
type answer struct {
	done  bool
	v     any
	err   error
	node  string
	pause bool
}

// answerOf returns what to make of v or err, the answer of the member on
// node, one of nodes or not. A member that is not the leader tells where
// it is, if it knows; the leader busy with a change of members has the
// request wait for it.
//
// A request safe to send twice, as a read or a command of a session, goes
// to the next member if the node is down, or did not answer in time, and
// is sent again if its leader lost its place. A command without a session
// is not: the node may have had it, and applied it.
func answerOf(nodes []string, node string, v any, err error, safe bool) answer {
	switch {
	case errors.Is(err, proc.NoProc):
		// No member there, which took nothing: ask the next.
		return answer{node: nextOf(nodes, node)}
	case safe && (errors.Is(err, proc.NoConnection) || errors.Is(err, context.DeadlineExceeded)):
		return answer{node: nextOf(nodes, node), pause: true}
	case err != nil:
		return answer{done: true, err: err}
	}
	switch r := v.(type) {
	case applied:
		return answer{done: true, v: r}
	case deposed:
		if safe {
			return answer{node: node, pause: true} // sent again, applied once
		}
		return answer{done: true, err: ErrLeaderChanged}
	case expired:
		return answer{done: true, err: ErrSessionExpired}
	case rejected:
		return answer{done: true, err: errors.New(r.Reason)}
	case redirect:
		if r.Leader == "" {
			// No leader known, as during an election: ask the next in a while.
			return answer{node: nextOf(nodes, node), pause: true}
		}
		return answer{node: r.Leader}
	}
	return answer{node: node, pause: true} // busy
}

// nextOf returns the node after node in nodes, the first if node is not
// one of them.
func nextOf(nodes []string, node string) string {
	return nodes[(slices.Index(nodes, node)+1)%len(nodes)]
}

func pause(ctx context.Context, d time.Duration) error {
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// call handles a call from outside the group: a command from a client, or
// the founding of the group.
func (b behaviour[S, Cmd, Rep]) call(r role, d *replica[S], c genstatem.Call) (role, []actor.Effect) {
	switch c.Req.(type) {
	case command, openSession, addMember, removeMember:
		if r == leader && d.transfer != nil {
			return r, actor.Do(c.Reply(busy{})) // handing over
		}
	}
	switch c.Req.(type) {
	case command, openSession, read, addMember, removeMember, transferLeader:
		if r == leader {
			waking := b.wake(d)
			r, effs := b.serve(r, d, c)
			return r, append(waking, effs...)
		}
	}
	return b.serve(r, d, c)
}

// serve handles the call c.
func (b behaviour[S, Cmd, Rep]) serve(r role, d *replica[S], c genstatem.Call) (role, []actor.Effect) {
	switch m := c.Req.(type) {
	case whoLeads:
		return r, actor.Do(c.Reply(redirect{Leader: d.leader.Node()}))
	case command:
		return b.command(r, d, c, m)
	case openSession:
		return b.openSession(r, d, c)
	case read:
		return b.read(r, d, c, m)
	case identify:
		return r, d.after(c.Reply(identity{ID: d.id, Blank: d.log.lastIndex() == 0}))
	case found:
		return b.found(r, d, c, m)
	case addMember, removeMember:
		return b.change(r, d, c, m)
	case transferLeader:
		return b.transferLeader(r, d, c, m)
	}
	return r, nil
}

// command handles a command. The leader appends it, and replies once it is
// applied; another member tells where the leader is.
func (b behaviour[S, Cmd, Rep]) command(r role, d *replica[S], c genstatem.Call, m command) (role, []actor.Effect) {
	if _, ok := m.Cmd.(Cmd); !ok {
		var want Cmd
		reason := fmt.Sprintf("quorum: group %s takes commands of type %T, not %T", b.spec.Group, want, m.Cmd)
		return r, actor.Do(c.Reply(rejected{reason}))
	}
	if r != leader {
		return r, actor.Do(c.Reply(redirect{Leader: d.leader.Node()}))
	}
	return r, b.appendCall(r, d, c, Entry{Cmd: m.Cmd, From: m.From})
}

// appendCall has the leader append e, to reply to the call c once it is
// applied.
func (b behaviour[S, Cmd, Rep]) appendCall(r role, d *replica[S], c genstatem.Call, e Entry) []actor.Effect {
	d.log = d.log.add(b.entry(d, e))
	d.pending = append(d.pending, pending{index: d.log.lastIndex(), from: c.From})
	effs := b.saveLog(d, d.log.lastIndex())
	effs = append(effs, b.replicate(d)...)
	return append(effs, b.commit(r, d)...)
}

// openSession handles the opening of a session: the leader appends it,
// and replies with its ID once it is applied.
func (b behaviour[S, Cmd, Rep]) openSession(r role, d *replica[S], c genstatem.Call) (role, []actor.Effect) {
	if r != leader {
		return r, actor.Do(c.Reply(redirect{Leader: d.leader.Node()}))
	}
	return r, b.appendCall(r, d, c, Entry{Open: true})
}

package quorum

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/gen"
	"github.com/shun159/molecule/behaviours/genstatem"
	"github.com/shun159/molecule/proc"
)

// Child returns the member of the group of spec on the node it starts on,
// to start under a supervisor. Started on a blank storage, it waits to be
// a member: founding the group, see Found, or added to it.
func Child[S, Cmd, Rep any](spec Spec[S, Cmd, Rep]) gen.Child {
	spec = spec.withDefaults()
	return genstatem.Child(behaviour[S, Cmd, Rep]{spec}, actor.WithName(actor.Local(replicaName(spec.Group))))
}

// ErrLeaderChanged is the error of a command whose leader lost its place
// before it could tell: the command may have been applied, or not.
var ErrLeaderChanged = errors.New("quorum: the leader changed; the command may have been applied or not")

// retryAfter is how long a client waits before asking again, when no
// member knows the leader, during an election, or the leader is busy with
// a change of members.
const retryAfter = 50 * time.Millisecond

// Command has group apply cmd, and returns its reply. It asks the members
// on nodes in turn until it finds the leader, which replies once cmd is
// committed and applied.
//
// An error other than ctx's means cmd may have been applied or not: its
// leader lost its place, see ErrLeaderChanged, or could not be reached.
// Command does not try it again, as applying it twice may differ from once.
func Command[Rep, Cmd any](ctx context.Context, caller actor.Caller, group string, nodes []string, cmd Cmd) (Rep, error) {
	var zero Rep
	v, err := ask(ctx, caller, group, nodes, command{cmd})
	if err != nil {
		return zero, err
	}
	rep, _ := v.(applied).Rep.(Rep)
	return rep, nil
}

// ask sends req to the leader of group, found among the members on nodes,
// and returns what it replied once done: applied, or an error. A member
// that is not the leader tells where it is, if it knows; the leader busy
// with a change of members has the request wait for it.
func ask(ctx context.Context, caller actor.Caller, group string, nodes []string, req any) (any, error) {
	next := nodes[0]
	for i := 1; ; i++ {
		v, err := actor.Call(ctx, caller, actor.Remote{Node: next, Name: replicaName(group)}, req)
		switch {
		case errors.Is(err, proc.NoProc):
			// No member there, which took nothing: ask the next.
			next = nodes[i%len(nodes)]
			continue
		case err != nil:
			return nil, err
		}
		switch r := v.(type) {
		case applied:
			return r, nil
		case deposed:
			return nil, ErrLeaderChanged
		case rejected:
			return nil, errors.New(r.Reason)
		case redirect:
			if next = r.Leader; next != "" {
				continue
			}
			next = nodes[i%len(nodes)]
		}
		// No leader known yet, or the leader busy: wait a little.
		if err := pause(ctx, retryAfter); err != nil {
			return nil, err
		}
	}
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
func (b behaviour[S, Cmd, Rep]) call(r role, d replica[S], c genstatem.Call) (role, replica[S], []actor.Effect) {
	switch m := c.Req.(type) {
	case command:
		return b.command(r, d, c, m)
	case identify:
		return r, d, d.after(c.Reply(identity{ID: d.id, Blank: d.log.lastIndex() == 0}))
	case found:
		return b.found(r, d, c, m)
	case addMember, removeMember:
		return b.change(r, d, c, m)
	}
	return r, d, nil
}

// command handles a command. The leader appends it, and replies once it is
// applied; another member tells where the leader is.
func (b behaviour[S, Cmd, Rep]) command(r role, d replica[S], c genstatem.Call, m command) (role, replica[S], []actor.Effect) {
	if _, ok := m.Cmd.(Cmd); !ok {
		var want Cmd
		reason := fmt.Sprintf("quorum: group %s takes commands of type %T, not %T", b.spec.Group, want, m.Cmd)
		return r, d, actor.Do(c.Reply(rejected{reason}))
	}
	if r != leader {
		return r, d, actor.Do(c.Reply(redirect{Leader: d.leader.Node()}))
	}
	d.log = append(d.log, Entry{Term: d.term, Cmd: m.Cmd})
	d.pending = append(d.pending, pending{index: d.log.lastIndex(), from: c.From})
	effs := b.saveLog(&d, d.log.lastIndex())
	effs = append(effs, b.replicate(d)...)
	return r, d, append(effs, b.commit(r, &d)...)
}

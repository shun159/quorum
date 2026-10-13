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
// to start under a supervisor. The node must be one of spec.Members.
func Child[S, Cmd, Rep any](spec Spec[S, Cmd, Rep]) gen.Child {
	spec = spec.withDefaults()
	return genstatem.Child(behaviour[S, Cmd, Rep]{spec}, actor.WithName(actor.Local(replicaName(spec.Group))))
}

// ErrLeaderChanged is the error of a command whose leader lost its place
// before it could tell: the command may have been applied, or not.
var ErrLeaderChanged = errors.New("quorum: the leader changed; the command may have been applied or not")

// retryAfter is how long Command waits before asking again, when no
// member knows the leader, during an election.
const retryAfter = 50 * time.Millisecond

// Command has group, on members, apply cmd, and returns its reply. It asks
// the members in turn until it finds the leader, which replies once cmd is
// committed and applied.
//
// An error other than ctx's means cmd may have been applied or not: its
// leader lost its place, see ErrLeaderChanged, or could not be reached. Command
// does not try it again, as applying it twice may differ from once.
func Command[Rep, Cmd any](ctx context.Context, caller actor.Caller, group string, members []string, cmd Cmd) (Rep, error) {
	var zero Rep
	next := members[0]
	for i := 1; ; i++ {
		v, err := actor.Call(ctx, caller, actor.Remote{Node: next, Name: replicaName(group)}, command{cmd})
		switch {
		case errors.Is(err, proc.NoProc):
			// No member there, which took nothing: ask the next.
			next = members[i%len(members)]
			continue
		case err != nil:
			return zero, err
		}
		switch r := v.(type) {
		case applied:
			rep, _ := r.Rep.(Rep)
			return rep, nil
		case deposed:
			return zero, ErrLeaderChanged
		case rejected:
			return zero, errors.New(r.Reason)
		case redirect:
			next = r.Leader
			if next == "" {
				next = members[i%len(members)]
				if err := pause(ctx, retryAfter); err != nil {
					return zero, err
				}
			}
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

// call handles a command from a client. The leader appends it, and replies
// once it is applied; another member tells where the leader is.
func (b behaviour[S, Cmd, Rep]) call(r role, d replica[S], c genstatem.Call) (role, replica[S], []actor.Effect) {
	cmd, ok := c.Req.(command)
	if !ok {
		return r, d, nil
	}
	if _, ok := cmd.Cmd.(Cmd); !ok {
		var want Cmd
		return r, d, actor.Do(c.Reply(rejected{fmt.Sprintf("quorum: group %s takes commands of type %T, not %T", b.spec.Group, want, cmd.Cmd)}))
	}
	if r != leader {
		return r, d, actor.Do(c.Reply(redirect{Leader: d.leader}))
	}
	d.log = append(d.log, entry{Term: d.term, Cmd: cmd.Cmd})
	d.pending = append(d.pending, pending{index: d.log.lastIndex(), from: c.From})
	effs := b.replicate(d)
	return r, d, append(effs, b.commit(r, &d)...)
}

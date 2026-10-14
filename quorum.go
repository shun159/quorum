package quorum

import (
	"context"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/gen"
	"github.com/shun159/quorum/internal/raft"
)

// Child returns the member of the group of spec on the node it starts on,
// to start under a supervisor. Started on a blank storage, it waits to be
// a member: founding the group, see Found, or added to it.
func Child[S, Cmd, Rep any](spec Spec[S, Cmd, Rep]) gen.Child {
	return raft.Child(spec.raft())
}

// ErrLeaderChanged is the error of a command whose leader lost its place
// before it could tell: the command may have been applied, or not.
var ErrLeaderChanged = raft.ErrLeaderChanged

// ErrSessionExpired is the error of a command of a Client whose session
// expired before it was applied. Sent before, it may have been applied
// then.
var ErrSessionExpired = raft.ErrSessionExpired

// Command has group apply cmd, and returns its reply. It asks the members
// on nodes in turn who leads, past those down, and sends cmd to the one
// that says it does, which replies once cmd is committed and applied.
//
// An error other than ctx's means cmd may have been applied or not: its
// leader lost its place, see ErrLeaderChanged, or was lost while it had
// cmd. Command does not try it again, as applying it twice may differ from
// once. A Client does, in a session, which has the group apply it once.
func Command[Rep, Cmd any](ctx context.Context, caller actor.Caller, group string, nodes []string, cmd Cmd) (Rep, error) {
	return raft.Command[Rep](ctx, caller, group, nodes, cmd)
}

// Read has group answer cmd from its state, as its Machine's Read does,
// without cmd going through the log: nothing is appended nor written. The
// answer is as fresh as a command's: it has every command applied before
// Read was called. The Machine must be a Reader.
//
// A read changes nothing: on an error, it may be tried again.
func Read[Rep, Cmd any](ctx context.Context, caller actor.Caller, group string, nodes []string, cmd Cmd) (Rep, error) {
	return raft.Read[Rep](ctx, caller, group, nodes, cmd)
}

// Found founds group with its members on nodes, all voters, each started
// on a blank storage: their configuration becomes the first entry of their
// logs, the same in all. A group is founded once: a member that is in
// another group, or in this one founded otherwise, refuses, and Found
// fails. Should Found fail midway, it can be run again with the same
// nodes, the members started anew if any was.
func Found(ctx context.Context, caller actor.Caller, group string, nodes []string) error {
	return raft.Found(ctx, caller, group, nodes)
}

// AddMember adds the member on node to group, asking the members on nodes
// for the leader. The member joins as a learner, following the log without
// voting; once it has caught up, the leader makes it a voter. AddMember
// returns then, once the member counts toward a majority, which one cut
// off may take long to: ctx bounds the wait. Should the leader change, it
// asks the next, and waits on.
//
// The node must have no member in the group: one on a new disk, which
// replaces another on the node, is added once that one is removed. Disks
// swapped one node after another, each AddMember returning before the next
// node is taken down, the group keeps its majority throughout.
func AddMember(ctx context.Context, caller actor.Caller, group string, nodes []string, node string) error {
	return raft.AddMember(ctx, caller, group, nodes, node)
}

// RemoveMember removes the member on node from group, whoever it is,
// asking the members on nodes for the leader, and returns once the removal
// is committed. A leader removing itself steps down then.
func RemoveMember(ctx context.Context, caller actor.Caller, group string, nodes []string, node string) error {
	return raft.RemoveMember(ctx, caller, group, nodes, node)
}

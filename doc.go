// Package quorum replicates a state machine over the nodes of a group, by
// the Raft consensus algorithm, on molecule.
//
// A [Machine] is pure: Apply takes its state and a command, and returns
// the next state, the reply to the command, and effects. Every member of
// the group applies the same commands in the same order, so every member
// holds the same state; the effects, which act on the world, only the
// leader of the group performs.
//
// A command goes to the leader, which appends it to its log and sends it
// to the others. Once a majority holds it, it is committed: it is never
// lost while a majority of the group lives, and the leader applies it and
// replies. A group of 2f+1 members bears f of them failing.
//
// [Command] sends a command and waits for its reply, which the callbacks
// of a behaviour must not do: a behaviour keeps a [Client] in its state
// instead, whose commands are effects, and whose results come of the
// messages it hands back to it.
//
// A Machine that also implements [Leader] learns when its member becomes
// the leader, and when it stops being one: to run something on one node of
// the group only, a singleton. A leader that has not heard from a majority
// within ElectionTimeout steps down, as etcd's CheckQuorum has it: cut off,
// it surrenders soon after the others could elect another, not when it
// hears of them again. Two members may still lead at once for that short
// while; what a leader does outside the group should carry its term.
//
// # Members
//
// Who the members are is in the log, a [Config] of voters and learners:
// a majority of the voters elects the leader and commits. [Found] founds
// a group once, its first configuration written alike into the logs of
// its first members. [AddMember] adds a member as a learner, which the
// leader makes a voter once it has caught up, and returns then; and
// [RemoveMember] removes one. A change of members waits for the one
// before it.
//
// A member is known by the PID it first had on its storage, blank. It
// keeps its identity, its term, its vote and its log in a [Storage], and,
// restarted, rejoins its group with them; it grants a vote, and
// acknowledges entries, once they are durable there. On a new disk, or
// without a storage, a member restarted is another, in no configuration:
// it neither votes nor stands, nor takes what is sent to the one before
// it. To swap the disk of a node, remove its member, then add the new one;
// once added, the next node may be swapped.
//
// # Snapshots
//
// Once SnapshotEvery entries are applied past its last snapshot, a member
// takes a [Snapshot] of its machine, in place of those entries, so that
// its log does not grow for good. The leader sends its own to a member
// missing entries it has no more, as one added to the group, which takes
// it in place of its machine and of its log up to there.
//
// # What there is not yet
//
// A snapshot is sent whole, in one message, which suits machines of a
// moderate size. A read is a command, committed as any other.
package quorum

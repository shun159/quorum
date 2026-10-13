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
// A Machine that also implements [Leader] learns when its member becomes
// the leader, and when it stops being one: to run something on one node of
// the group only, a singleton. A leader that has not heard from a majority
// within ElectionTimeout steps down, as etcd's CheckQuorum has it: cut off,
// it surrenders soon after the others could elect another, not when it
// hears of them again. Two members may still lead at once for that short
// while; what a leader does outside the group should carry its term.
//
// A member keeps its term, its vote and its log in a [Storage], and,
// restarted, rejoins its group with them. It grants a vote, and
// acknowledges entries, once they are durable there.
//
// # What there is not yet
//
// A member whose storage is lost, on a new disk, must not rejoin its
// group as it was: it would vote again in a term, and forget entries the
// others count on it for. Nothing stops it yet, and members cannot be
// removed and added again. There are no snapshots, and a read is a
// command, committed as any other.
package quorum

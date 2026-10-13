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
// the group only, a singleton.
//
// # What there is not yet
//
// The log is in memory: a member restarted must not rejoin its group,
// having forgotten its votes and its log. The members are fixed, there
// are no snapshots, and a read is a command, committed as any other.
package quorum

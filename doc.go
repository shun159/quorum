// Package quorum replicates a state machine over the nodes of a group, on
// molecule: the group agrees on the commands, and every member applies
// them alike.
//
// A [Machine] is pure: Apply takes its state and a command, and returns
// the next state, the reply to the command, and effects. Every member of
// the group applies the same commands in the same order, so every member
// holds the same state; the effects, which act on the world, only the
// leader of the group performs.
//
// A command goes to the leader, which sends it to the others. Once a
// majority holds it, it is committed: it is never lost while a majority of
// the group lives, and the leader applies it and replies. A group of 2f+1
// members bears f of them failing.
//
// A Machine that also implements [Leader] learns when its member becomes
// the leader, and when it stops being one: to run something on one node of
// the group only, a singleton. A leader that has not heard from a majority
// within ElectionTimeout steps down, soon after the others could elect
// another. Two members may still lead at once for that short while; what
// a leader does outside the group should carry its term. The other way
// round, a leader that the majority hears from stays: a member cut off and
// back, or cut off from the leader only, does not depose it.
//
// A group with nothing to do goes quiet, after Spec.Quiet: its members
// stop telling each other they live, and count on their nodes instead,
// which tell each other once for all the groups, see dist.Config.Silent.
// Idle, a group costs nothing, however many a node has; its leader lost,
// the others elect another once its node falls silent, or it dies. A
// command or a read wakes it.
//
// # Members
//
// Who the members are, voters and learners, is agreed on as commands are:
// a majority of the voters elects the leader and commits. [Found] founds
// a group once, with its first members. [AddMember] adds a member as a
// learner, which the leader makes a voter once it has caught up, and
// returns then; and [RemoveMember] removes one. A change of members waits
// for the one before it. [TransferLeader] moves the lead to another voter,
// as before taking a node down.
//
// # Storage
//
// A member keeps who it is and what it agreed on in the [Storage] of its
// node, in Spec.Dir, and, restarted, rejoins its group with them; it
// tells the others it holds a command once it is on disk. A member is
// known by who it first was on its storage, blank: on a new disk, or
// without a Dir, a member restarted is another, in no group. To swap the
// disk of a node, remove its member, then add the new one; once added, the
// next node may be swapped.
//
// Once SnapshotEvery commands are applied past its last snapshot, a member
// keeps the state of its machine in place of them, so that what it keeps
// does not grow for good. A member missing commands the others keep no
// more, as one just added, takes the state of the leader's machine.
//
// # Clients
//
// [Command] sends a command and waits for its reply; should its leader be
// lost meanwhile, it cannot know whether the command was applied, and
// says so. A [Client] waits for nothing, which the callbacks of a
// behaviour must not do: kept in the state of the behaviour, its commands
// are effects, and its results come of the messages handed back to it.
//
// A Client opens a session with the group, in which each of its commands
// is applied once however many times it is sent, so it sends them again
// until they are: through members down, leaders deposed, and replies
// lost. The members keep the sessions with their machine, and forget a
// session idle for SessionTimeout.
//
// # Reads
//
// A Machine that is also a [Reader] answers reads, [Read] and
// [Client.Read]: the leader answers from its state, as fresh as after a
// command, without anything written, once a majority has told it it still
// leads. A read costs a round trip to a majority, not a write to their
// disks.
//
// # What there is not yet
//
// The state of a machine is sent whole, in one message, to a member that
// needs it, which suits machines of a moderate size.
package quorum

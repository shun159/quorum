package raft

import (
	"github.com/shun159/molecule/dist"
	"github.com/shun159/molecule/proc"
)

func init() {
	dist.Register(preVote{}, preVoteReply{}, requestVote{}, voteReply{}, appendEntries{}, appendReply{},
		whoLeads{}, command{}, read{}, applied{}, expired{}, openSession{}, Origin{}, redirect{}, deposed{}, rejected{},
		identify{}, identity{}, found{}, Config{}, addMember{}, removeMember{}, busy{},
		installSnapshot{}, transferLeader{}, timeoutNow{})
}

// The messages between the members of a group, the RPCs of Raft, sent as
// messages: a reply is a message too. Members are named by who they are,
// as Config has it, and found on the node of that PID.
type (
	// preVote asks a member whether it would vote for Candidate to lead
	// Term, which changes nothing of the member: see preStand. The reply
	// has the Term asked about if granted, else that of the member. Of
	// types of their own, without the methods of termed, neither moves the
	// member that gets it to a later term.
	preVote      requestVote
	preVoteReply voteReply

	// requestVote asks for the vote of a member, for Candidate to lead Term.
	requestVote struct {
		Term      uint64
		Candidate proc.PID
		LastIndex uint64 // of the candidate's log
		LastTerm  uint64
	}
	voteReply struct {
		Term    uint64
		From    proc.PID
		Granted bool
	}
	// appendEntries has the member To append Entries after the entry of
	// PrevTerm at PrevIndex, if it has that one, and tells it what is
	// committed. Without entries, it is a heartbeat. A member other than
	// To on its node, a later one, drops it. Read is the leader's last
	// round of confirming reads, which the reply echoes. Quiet tells that
	// the leader goes quiet, see Quiet.
	appendEntries struct {
		Term      uint64
		Leader    proc.PID
		To        proc.PID
		PrevIndex uint64
		PrevTerm  uint64
		Entries   []Entry
		Commit    uint64
		Read      uint64
		Quiet     bool
	}
	// installSnapshot gives the member To the leader's snapshot, in place
	// of the entries it misses, which the leader has no more. It is
	// answered with an appendReply.
	installSnapshot struct {
		Term     uint64
		Leader   proc.PID
		To       proc.PID
		Snapshot Snapshot
	}
	// timeoutNow tells the member To, which holds all the log of the
	// leader of Term, to stand at once: the leader hands over to it.
	timeoutNow struct {
		Term   uint64
		Leader proc.PID
		To     proc.PID
	}
	// appendReply tells the leader how far the log of From matches its
	// own, Match, or, failing, how long it is, for the leader to go back.
	appendReply struct {
		Term      uint64
		From      proc.PID
		Success   bool
		Match     uint64
		LastIndex uint64
		Read      uint64
	}
)

// termed is a message between members, which carries the term of its
// sender.
type termed interface{ term() uint64 }

func (m requestVote) term() uint64   { return m.Term }
func (m voteReply) term() uint64     { return m.Term }
func (m appendEntries) term() uint64 { return m.Term }
func (m appendReply) term() uint64   { return m.Term }

func (m installSnapshot) term() uint64 { return m.Term }
func (m timeoutNow) term() uint64      { return m.Term }

// addressed is a message to one member, which a member other than To on
// its node, a later one, drops.
type addressed interface{ to() proc.PID }

func (m appendEntries) to() proc.PID   { return m.To }
func (m installSnapshot) to() proc.PID { return m.To }
func (m timeoutNow) to() proc.PID      { return m.To }

// A command from a client, and the replies it may get.
type (
	whoLeads struct{} // answered with a redirect, to the member itself if it leads
	command  struct {
		Cmd  any
		From Origin // its session, if any, see Client
	}
	read     struct{ Cmd any }       // answered by the leader, not appended
	applied  struct{ Rep any }       // applied, with its reply
	redirect struct{ Leader string } // not appended; the node of the leader, if known
	deposed  struct{}                // appended, but the leader lost its place: applied or not
	expired  struct{}                // not applied: its session expired

	openSession struct{}                // answered with applied, the ID of the session
	rejected    struct{ Reason string } // not taken
)

// Founding a group: who a member is, and the first configuration.
type (
	identify struct{}
	identity struct {
		ID    proc.PID
		Blank bool // its log is empty: no group has it yet
	}
	found struct{ Config Config }
)

// A change of the members, from a client, and the reply that it must wait
// for the change before.
type (
	addMember    struct{ ID proc.PID }
	removeMember struct{ Node string }

	transferLeader struct{ To string } // the node to hand the lead to
	busy           struct{}            // not appended: a change is under way
)

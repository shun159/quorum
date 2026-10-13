package quorum

import "github.com/shun159/molecule/dist"

func init() {
	dist.Register(requestVote{}, voteReply{}, appendEntries{}, appendReply{},
		command{}, applied{}, redirect{}, deposed{}, rejected{})
}

// The messages between the members of a group, the RPCs of Raft, sent as
// messages: a reply is a message too.
type (
	// requestVote asks for the vote of a member, for Candidate to lead Term.
	requestVote struct {
		Term      uint64
		Candidate string
		LastIndex uint64 // of the candidate's log
		LastTerm  uint64
	}
	voteReply struct {
		Term    uint64
		From    string
		Granted bool
	}
	// appendEntries has a follower append Entries after the entry of
	// PrevTerm at PrevIndex, if it has that one, and tells it what is
	// committed. Without entries, it is a heartbeat.
	appendEntries struct {
		Term      uint64
		Leader    string
		PrevIndex uint64
		PrevTerm  uint64
		Entries   []Entry
		Commit    uint64
	}
	// appendReply tells the leader how far the log of From matches its
	// own, Match, or, failing, how long it is, for the leader to go back.
	appendReply struct {
		Term      uint64
		From      string
		Success   bool
		Match     uint64
		LastIndex uint64
	}
)

// termed is a message between members, which carries the term of its
// sender.
type termed interface{ term() uint64 }

func (m requestVote) term() uint64   { return m.Term }
func (m voteReply) term() uint64     { return m.Term }
func (m appendEntries) term() uint64 { return m.Term }
func (m appendReply) term() uint64   { return m.Term }

// A command from a client, and the replies it may get.
type (
	command  struct{ Cmd any }
	applied  struct{ Rep any }       // applied, with its reply
	redirect struct{ Leader string } // not appended; Leader if known
	deposed  struct{}                // appended, but the leader lost its place: applied or not
	rejected struct{ Reason string } // not a command of the group
)

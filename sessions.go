package quorum

import (
	"errors"
	"maps"
	"time"
)

// A Client opens a session, an entry in the log whose index is its ID,
// and numbers its commands in it. The members remember the reply to each
// command applied, until the client has it: a command sent again, as
// after its leader lost its place, is answered with the reply of the
// first, not applied twice, as Raft's thesis has it.
//
// A session the log has not seen for SessionTimeout expires: its replies
// are forgotten, and a command in it is refused with ErrSessionExpired,
// not applied. Time is the log's: each entry carries the clock of the
// leader that appended it, so that the members all expire a session at
// the same entry.

// ErrSessionExpired is the error of a command whose session expired
// before it was applied. Sent before, it may have been applied then.
var ErrSessionExpired = errors.New("quorum: the session expired; the command may have been applied before or not")

// Origin tells a command of a session: the session, the number of the
// command in it, and the number up to which the client has the replies.
type Origin struct {
	Session uint64
	Seq     uint64
	Acked   uint64
}

// Sessions are the sessions of the clients of a group, as of an entry of
// its log, kept by the members with their machine.
type Sessions struct {
	Open  map[uint64]Session
	Time  int64 // of the log: the latest time a leader put in an entry
	Sweep int64 // when to look for sessions expired next
}

// Session is what the members remember of a session: when the log last
// saw it, and the replies to its commands the client may not have yet.
type Session struct {
	Last    int64
	Acked   uint64
	Replies map[uint64]any
}

// passed moves the time of the log to t, if later, and forgets the
// sessions not seen for timeout. It looks for them every quarter of
// timeout.
func (s Sessions) passed(t int64, timeout time.Duration) Sessions {
	s.Time = max(s.Time, t)
	if s.Time < s.Sweep {
		return s
	}
	s.Sweep = s.Time + int64(timeout/4)
	for id, ss := range s.Open {
		if s.Time-ss.Last > int64(timeout) {
			s.Open = maps.Clone(s.Open)
			delete(s.Open, id)
		}
	}
	return s
}

// opened opens the session id.
func (s Sessions) opened(id uint64) Sessions {
	s.Open = maps.Clone(s.Open)
	if s.Open == nil {
		s.Open = map[uint64]Session{}
	}
	s.Open[id] = Session{Last: s.Time}
	return s
}

// seen returns what the sessions know of the command o: whether its
// session is open, and, if it was applied, its reply.
func (s Sessions) seen(o Origin) (open, applied bool, rep any) {
	ss, open := s.Open[o.Session]
	if !open {
		return false, false, nil
	}
	rep, applied = ss.Replies[o.Seq]
	return true, applied || o.Seq <= ss.Acked, rep
}

// applied notes that the command o was applied, with rep, and forgets the
// replies the client has.
func (s Sessions) applied(o Origin, rep any) Sessions {
	ss := s.Open[o.Session]
	ss.Last, ss.Acked = s.Time, max(ss.Acked, o.Acked)
	ss.Replies = maps.Clone(ss.Replies)
	if ss.Replies == nil {
		ss.Replies = map[uint64]any{}
	}
	if o.Seq > ss.Acked {
		ss.Replies[o.Seq] = rep
	}
	maps.DeleteFunc(ss.Replies, func(seq uint64, _ any) bool { return seq <= ss.Acked })
	s.Open = maps.Clone(s.Open)
	s.Open[o.Session] = ss
	return s
}

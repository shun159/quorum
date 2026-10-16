package raft

import (
	"errors"
	"reflect"
	"testing"

	"github.com/shun159/molecule/dist"
	"github.com/shun159/molecule/proc"
)

// wired is a message between members, as dist sends it.
type wired interface {
	AppendBinary(b []byte) ([]byte, error)
}

// The messages between members come back as they were encoded, with the
// commands of the entries of the kinds dist encodes.
func TestWire(t *testing.T) {
	config := &Config{Voters: []proc.PID{ida, idb}, Learners: []proc.PID{idc}}
	msgs := []wired{
		requestVote{Term: 2, Candidate: ida, LastIndex: 10, LastTerm: 1},
		preVote{Term: 3, Candidate: idb, LastIndex: 11, LastTerm: 2},
		voteReply{Term: 2, From: idb, Granted: true},
		preVoteReply{Term: 3, From: idc},
		appendEntries{Term: 2, Leader: ida, To: idb, PrevIndex: 9, PrevTerm: 1, Commit: 8, Read: 4, Quiet: true},
		appendEntries{Term: 2, Leader: ida, To: idc, PrevIndex: 9, PrevTerm: 1, Commit: 9, Entries: []Entry{
			{Term: 2, Time: -1, Cmd: 7},
			{Term: 2, Time: 1 << 60, Cmd: "x", From: Origin{Session: 3, Seq: 4, Acked: 2}},
			{Term: 2, Config: config},
			{Term: 2, Open: true},
		}},
		appendReply{Term: 2, From: idb, Success: true, Match: 12, LastIndex: 12, Read: 4},
		installSnapshot{Term: 2, Leader: ida, To: idb, Index: 100, Offset: 64, Size: 200, Head: 10, Data: []byte("chunk")},
		installSnapshot{Term: 2, Leader: ida, To: idb, Index: 100, Size: 0},
		snapshotAck{Term: 2, From: idb, Index: 100, Offset: 128},
		timeoutNow{Term: 2, Leader: ida, To: idc},
	}
	for _, m := range msgs {
		b, err := m.AppendBinary(nil)
		if err != nil {
			t.Fatalf("%T: %v", m, err)
		}
		got := reflect.New(reflect.TypeOf(m))
		if err := got.Interface().(dist.BinaryDecoder).DecodeBinary(b); err != nil {
			t.Fatalf("%T: %v", m, err)
		}
		if !reflect.DeepEqual(got.Elem().Interface(), m) {
			t.Errorf("%#v, want %#v", got.Elem().Interface(), m)
		}
		for i := range len(b) {
			if err := got.Interface().(dist.BinaryDecoder).DecodeBinary(b[:i]); err == nil {
				t.Errorf("%T cut at %d of %d: no error", m, i, len(b))
			}
		}
	}
}

// An entry with a command dist cannot encode has the message go by gob.
func TestWireNotBinary(t *testing.T) {
	type cmd struct{ N int }
	m := appendEntries{Term: 2, Leader: ida, To: idb, Entries: []Entry{{Term: 2, Cmd: cmd{1}}}}
	if _, err := m.AppendBinary(nil); !errors.Is(err, dist.ErrNotBinary) {
		t.Errorf("%v, want dist.ErrNotBinary", err)
	}
}

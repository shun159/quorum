package quorum

import (
	"context"
	"testing"

	"github.com/shun159/molecule/proc"
)

// A node down, or silent, is passed for a request safe to send twice; for
// a command, which it may have had, it is an error.
func TestAnswerDown(t *testing.T) {
	nodes := []string{"a", "b"}
	if ans := answerOf(nodes, "a", nil, proc.NoConnection, true); ans.done || ans.node != "b" {
		t.Errorf("safe: %+v", ans)
	}
	if ans := answerOf(nodes, "a", nil, context.DeadlineExceeded, true); ans.done || ans.node != "b" {
		t.Errorf("safe, not answered in time: %+v", ans)
	}
	if ans := answerOf(nodes, "a", nil, proc.NoConnection, false); !ans.done || ans.err != proc.NoConnection {
		t.Errorf("a command: %+v", ans)
	}
}

// Asked who leads, a member says it does, or where, if it knows; one
// down, or not knowing, has the next asked after a pause.
func TestProbed(t *testing.T) {
	nodes := []string{"a", "b", "c"}
	for _, tc := range []struct {
		v     any
		err   error
		next  string
		leads bool
		wait  bool
	}{
		{v: redirect{Leader: "a"}, next: "a", leads: true},
		{v: redirect{Leader: "c"}, next: "c"},
		{v: redirect{}, next: "b", wait: true},
		{err: proc.NoConnection, next: "b", wait: true},
		{err: noProc, next: "b", wait: true},
	} {
		next, leads, wait := probed(nodes, "a", tc.v, tc.err)
		if next != tc.next || leads != tc.leads || wait != tc.wait {
			t.Errorf("%#v %v: %s %v %v, want %s %v %v", tc.v, tc.err, next, leads, wait, tc.next, tc.leads, tc.wait)
		}
	}
}

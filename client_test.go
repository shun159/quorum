package quorum

import (
	"context"
	"errors"
	"testing"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/proc"
)

// asked returns the node and command of the one request in effs, failing
// the test unless that is all there is.
func asked(t *testing.T, effs []actor.Effect) (string, actor.SendRequest) {
	t.Helper()
	if len(effs) != 1 {
		t.Fatalf("effects %#v", effs)
	}
	req, ok := effs[0].(actor.SendRequest)
	if !ok {
		t.Fatalf("effect %#v", effs[0])
	}
	return req.To.(actor.Remote).Node, req
}

// waited returns the timer in effs, failing the test unless that is all
// there is.
func waited(t *testing.T, effs []actor.Effect) actor.StartTimer {
	t.Helper()
	if len(effs) != 1 {
		t.Fatalf("effects %#v", effs)
	}
	timer, ok := effs[0].(actor.StartTimer)
	if !ok {
		t.Fatalf("effect %#v", effs[0])
	}
	return timer
}

// said returns the member's answer to req.
func said(req actor.SendRequest, v any, err error) actor.Response {
	return actor.Response{Tag: req.Tag, Value: v, Err: err}
}

var noProc = &actor.ExitError{Reason: proc.NoProc}

// TestClientFindsLeader has a client follow the members' answers to the
// leader: the next member if none is there, the leader where it is said
// to be, the next member after a pause if no leader is known yet, the
// same member after a pause if it is busy.
func TestClientFindsLeader(t *testing.T) {
	c := NewClient[int, int]("g", []string{"a", "b", "c"}, 0)
	c, effs := c.Command("x", 1)
	node, req := asked(t, effs)
	if node != "a" || req.Req != (command{1}) {
		t.Fatalf("asked %s %#v", node, req.Req)
	}
	steps := []struct {
		answer any
		err    error
		node   string // asked next
		pause  bool   // after a pause
	}{
		{err: noProc, node: "b"},
		{answer: redirect{Leader: ""}, node: "c", pause: true},
		{answer: redirect{Leader: "b"}, node: "b"},
		{answer: busy{}, node: "b", pause: true},
		{err: noProc, node: "c"},
		{err: noProc, node: "a"},
	}
	for i, s := range steps {
		var res *Result[int]
		c, effs, res = c.Handle(said(req, s.answer, s.err))
		if res != nil {
			t.Fatalf("step %d: done %#v", i, res)
		}
		if s.pause {
			timer := waited(t, effs)
			c, effs, _ = c.Handle(timer.Msg)
		}
		if node, req = asked(t, effs); node != s.node {
			t.Fatalf("step %d: asked %s, not %s", i, node, s.node)
		}
	}
	c, effs, res := c.Handle(said(req, applied{Rep: 7}, nil))
	if len(effs) != 0 || res == nil || *res != (Result[int]{Tag: "x", Rep: 7}) {
		t.Fatalf("done %#v, effects %#v", res, effs)
	}
	if c.Out() != 0 {
		t.Errorf("%d out", c.Out())
	}
}

// TestClientErrors has commands end with the errors Command returns: not
// asked again, as they may have been applied.
func TestClientErrors(t *testing.T) {
	for _, tc := range []struct {
		answer any
		err    error
		want   error
	}{
		{answer: deposed{}, want: ErrLeaderChanged},
		{err: context.DeadlineExceeded, want: context.DeadlineExceeded},
		{err: proc.NoConnection, want: proc.NoConnection},
	} {
		c := NewClient[int, int]("g", []string{"a"}, 0)
		c, effs := c.Command("x", 1)
		_, req := asked(t, effs)
		_, effs, res := c.Handle(said(req, tc.answer, tc.err))
		if len(effs) != 0 || res == nil || !errors.Is(res.Err, tc.want) {
			t.Errorf("%#v %v: done %#v, effects %#v", tc.answer, tc.err, res, effs)
		}
	}
	c := NewClient[int, int]("g", []string{"a"}, 0)
	c, effs := c.Command("x", 1)
	_, req := asked(t, effs)
	if _, _, res := c.Handle(said(req, rejected{"no"}, nil)); res == nil || res.Err == nil || res.Err.Error() != "no" {
		t.Errorf("rejected: done %#v", res)
	}
}

// TestClientStale has a client drop what answers a command cancelled, or
// an earlier attempt, and what is not its own.
func TestClientStale(t *testing.T) {
	c := NewClient[int, int]("g", []string{"a", "b"}, 0)
	c, effs := c.Command("x", 1)
	_, first := asked(t, effs)
	c, _ = c.Cancel("x")
	c, effs = c.Command("x", 2)
	_, second := asked(t, effs)
	other := NewClient[int, int]("h", []string{"a"}, 0)
	other, effs = other.Command("x", 3)
	_, theirs := asked(t, effs)

	for _, msg := range []any{
		said(first, applied{Rep: 1}, nil),   // cancelled
		said(theirs, applied{Rep: 3}, nil),  // of group h
		actor.Response{Tag: "x"},            // not of a client
		retry{group: "g", tag: "x", seq: 1}, // not waiting
	} {
		var res *Result[int]
		if c, effs, res = c.Handle(msg); len(effs) != 0 || res != nil {
			t.Fatalf("%#v: done %#v, effects %#v", msg, res, effs)
		}
	}
	if _, _, res := c.Handle(said(second, applied{Rep: 2}, nil)); res == nil || res.Rep != 2 {
		t.Fatalf("done %#v", res)
	}
	if _, _, res := other.Handle(said(theirs, applied{Rep: 3}, nil)); res == nil || res.Rep != 3 {
		t.Fatalf("group h: done %#v", res)
	}
}

// TestClientMany has two commands out at once, each done on its own.
func TestClientMany(t *testing.T) {
	c := NewClient[int, int]("g", []string{"a"}, 0)
	c, effs := c.Command("x", 1)
	_, x := asked(t, effs)
	c, effs = c.Command("y", 2)
	_, y := asked(t, effs)
	if c.Out() != 2 {
		t.Fatalf("%d out", c.Out())
	}
	c, _, res := c.Handle(said(y, applied{Rep: 20}, nil))
	if res == nil || res.Tag != "y" || res.Rep != 20 {
		t.Fatalf("y: %#v", res)
	}
	c, _, res = c.Handle(said(x, applied{Rep: 10}, nil))
	if res == nil || res.Tag != "x" || res.Rep != 10 || c.Out() != 0 {
		t.Fatalf("x: %#v, %d out", res, c.Out())
	}
}

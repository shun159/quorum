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

// TestClientFindsLeader has a client ask the members who leads, past
// those down or not knowing, and send the command to the one that says it
// does; then to it at once, the next time.
func TestClientFindsLeader(t *testing.T) {
	c := NewClient[int, int]("g", []string{"a", "b", "c"}, 0)
	c, effs := c.Command("x", 1)
	node, req := asked(t, effs)
	if node != "a" || req.Req != (whoLeads{}) {
		t.Fatalf("asked %s %#v", node, req.Req)
	}
	who, cmd := whoLeads{}, command{1}
	steps := []struct {
		answer any
		err    error
		node   string // asked next
		req    any    // what
		pause  bool   // after a pause
	}{
		{err: noProc, node: "b", req: who, pause: true},
		{answer: redirect{Leader: ""}, node: "c", req: who, pause: true},
		{answer: redirect{Leader: "b"}, node: "b", req: who},
		{answer: redirect{Leader: "b"}, node: "b", req: cmd},
		{answer: busy{}, node: "b", req: cmd, pause: true},
		{answer: redirect{Leader: "c"}, node: "c", req: who},
		{err: proc.NoConnection, node: "a", req: who, pause: true},
		{answer: redirect{Leader: "a"}, node: "a", req: cmd},
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
		if node, req = asked(t, effs); node != s.node || req.Req != s.req {
			t.Fatalf("step %d: asked %s %#v, not %s %#v", i, node, req.Req, s.node, s.req)
		}
	}
	c, effs, res := c.Handle(said(req, applied{Rep: 7}, nil))
	if len(effs) != 0 || res == nil || *res != (Result[int]{Tag: "x", Rep: 7}) {
		t.Fatalf("done %#v, effects %#v", res, effs)
	}
	if c.Out() != 0 {
		t.Errorf("%d out", c.Out())
	}
	_, effs = c.Command("y", 2)
	if node, req := asked(t, effs); node != "a" || req.Req != (command{2}) {
		t.Errorf("next: asked %s %#v", node, req.Req)
	}
}

// TestClientForgetsLeader has a client that lost the leader ask who leads
// again, for the next command.
func TestClientForgetsLeader(t *testing.T) {
	c := knowingLeader("a", "b")
	c, effs := c.Command("x", 1)
	_, req := asked(t, effs)
	c, _, _ = c.Handle(said(req, nil, proc.NoConnection))
	_, effs = c.Command("y", 2)
	if node, req := asked(t, effs); node != "a" || req.Req != (whoLeads{}) {
		t.Errorf("asked %s %#v", node, req.Req)
	}
}

// TestClientReadsPastDown has a read, safe to send twice, go to the next
// member when one is down: a command would not, as it may have been
// applied there.
func TestClientReadsPastDown(t *testing.T) {
	c := NewClient[int, int]("g", []string{"a", "b"}, 0)
	c, effs := c.Read("r", 0)
	node, req := asked(t, effs)
	if node != "a" || req.Req != (read{0}) {
		t.Fatalf("asked %s %#v", node, req.Req)
	}
	c, effs, _ = c.Handle(said(req, nil, proc.NoConnection))
	_, effs, _ = c.Handle(waited(t, effs).Msg)
	if node, req := asked(t, effs); node != "b" || req.Req != (read{0}) {
		t.Errorf("then asked %s %#v", node, req.Req)
	}
}

// knowingLeader returns a client of group g on nodes, knowing the leader on the
// first: a command goes to it at once.
func knowingLeader(nodes ...string) Client[int, int] {
	c := NewClient[int, int]("g", nodes, 0)
	c.leader = nodes[0]
	return c
}

// TestClientErrors has commands end with the errors Command returns: not
// sent again, as they may have been applied.
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
		c, effs := knowingLeader("a").Command("x", 1)
		_, req := asked(t, effs)
		_, effs, res := c.Handle(said(req, tc.answer, tc.err))
		if len(effs) != 0 || res == nil || !errors.Is(res.Err, tc.want) {
			t.Errorf("%#v %v: done %#v, effects %#v", tc.answer, tc.err, res, effs)
		}
	}
	c, effs := knowingLeader("a").Command("x", 1)
	_, req := asked(t, effs)
	if _, _, res := c.Handle(said(req, rejected{"no"}, nil)); res == nil || res.Err == nil || res.Err.Error() != "no" {
		t.Errorf("rejected: done %#v", res)
	}
}

// TestClientStale has a client drop what answers a command cancelled, or
// an earlier attempt, and what is not its own.
func TestClientStale(t *testing.T) {
	c, effs := knowingLeader("a", "b").Command("x", 1)
	_, first := asked(t, effs)
	c, _ = c.Cancel("x")
	c, effs = c.Command("x", 2)
	_, second := asked(t, effs)
	other := knowingLeader("a")
	other.group = "h"
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
	c, effs := knowingLeader("a").Command("x", 1)
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

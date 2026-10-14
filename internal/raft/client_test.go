package raft

import (
	"context"
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

// inSessionOn returns a client of group g on nodes, its session 7 open.
func inSessionOn(nodes ...string) Client[int, int] {
	c := NewClient[int, int]("g", nodes, 0)
	c.session = 7
	return c
}

// cmd is the command n of session 7, the client having the replies up to
// acked.
func cmd(n int, seq, acked uint64) command {
	return command{Cmd: n, From: Origin{Session: 7, Seq: seq, Acked: acked}}
}

// TestClientOpensSession has the first command open the session, and the
// commands wait for it, then go in it, numbered.
func TestClientOpensSession(t *testing.T) {
	c := NewClient[int, int]("g", []string{"a", "b"}, 0)
	c, effs := c.Command("x", 1)
	node, open := asked(t, effs)
	if node != "a" || open.Req != (openSession{}) {
		t.Fatalf("asked %s %#v", node, open.Req)
	}
	c, effs = c.Command("y", 2)
	if len(effs) != 0 || c.Out() != 2 {
		t.Fatalf("while opening: %v, %d out", effs, c.Out())
	}
	c, effs, _ = c.Handle(said(open, redirect{Leader: "b"}, nil))
	_, open = asked(t, effs)
	_, effs, _ = c.Handle(said(open, applied{Rep: uint64(7)}, nil))
	got := map[any]bool{}
	for _, e := range effs {
		r := e.(actor.SendRequest)
		if r.To.(actor.Remote).Node != "b" {
			t.Errorf("sent to %v", r.To)
		}
		got[r.Req] = true
	}
	if len(got) != 2 || !got[cmd(1, 1, 0)] || !got[cmd(2, 2, 0)] {
		t.Errorf("sent %v", effs)
	}
}

// TestClientFindsLeader has a client follow the members' answers to the
// leader, sending the command again, the same, past members down or
// silent and leaders deposed, until it is applied; then to that leader at
// once, the next time.
func TestClientFindsLeader(t *testing.T) {
	c := inSessionOn("a", "b", "c")
	c, effs := c.Command("x", 1)
	node, req := asked(t, effs)
	if node != "a" || req.Req != cmd(1, 1, 0) {
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
		{answer: deposed{}, node: "b", pause: true},
		{err: proc.NoConnection, node: "c", pause: true},
		{err: context.DeadlineExceeded, node: "a", pause: true},
	}
	for i, s := range steps {
		var res *Result[int]
		c, effs, res = c.Handle(said(req, s.answer, s.err))
		if res != nil {
			t.Fatalf("step %d: done %#v", i, res)
		}
		if s.pause {
			c, effs, _ = c.Handle(waited(t, effs).Msg)
		}
		if node, req = asked(t, effs); node != s.node || req.Req != cmd(1, 1, 0) {
			t.Fatalf("step %d: asked %s %#v, not %s", i, node, req.Req, s.node)
		}
	}
	c, effs, res := c.Handle(said(req, applied{Rep: 7}, nil))
	if len(effs) != 0 || res == nil || *res != (Result[int]{Tag: "x", Rep: 7}) || c.Out() != 0 {
		t.Fatalf("done %#v, effects %#v, %d out", res, effs, c.Out())
	}
	_, effs = c.Command("y", 2)
	if node, req := asked(t, effs); node != "a" || req.Req != cmd(2, 2, 1) {
		t.Errorf("next: asked %s %#v", node, req.Req)
	}
}

// TestClientAcked has a command tell the replies the client has: those
// before the first command still out.
func TestClientAcked(t *testing.T) {
	c := inSessionOn("a")
	c, effs := c.Command("x", 1)
	_, x := asked(t, effs)
	c, _ = c.Command("y", 2)
	c, _, _ = c.Handle(said(x, applied{Rep: 1}, nil))
	_, effs = c.Command("z", 3)
	if _, req := asked(t, effs); req.Req != cmd(3, 3, 1) {
		t.Errorf("asked %#v", req.Req)
	}
}

// TestClientExpired has a command sent once in a session expired go in a
// new session; one sent before, which may have been applied then, end
// with ErrSessionExpired.
func TestClientExpired(t *testing.T) {
	c := inSessionOn("a")
	c, effs := c.Command("x", 1)
	_, req := asked(t, effs)
	c, effs, res := c.Handle(said(req, expired{}, nil))
	_, open := asked(t, effs)
	if res != nil || open.Req != (openSession{}) {
		t.Fatalf("expired: done %#v, asked %#v", res, open.Req)
	}
	_, effs, _ = c.Handle(said(open, applied{Rep: uint64(9)}, nil))
	want := command{Cmd: 1, From: Origin{Session: 9, Seq: 1}}
	if _, req := asked(t, effs); req.Req != want {
		t.Fatalf("in the new session: asked %#v", req.Req)
	}

	c = inSessionOn("a")
	c, effs = c.Command("x", 1)
	_, req = asked(t, effs)
	c, effs, _ = c.Handle(said(req, nil, proc.NoConnection))
	c, effs, _ = c.Handle(waited(t, effs).Msg)
	_, req = asked(t, effs)
	if _, _, res := c.Handle(said(req, expired{}, nil)); res == nil || res.Err != ErrSessionExpired {
		t.Errorf("sent twice, expired: done %#v", res)
	}
}

// TestClientReadsPastDown has a read, safe to send twice, go to the next
// member when one is down.
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

// TestClientRejected has a command refused end with the reason.
func TestClientRejected(t *testing.T) {
	c, effs := inSessionOn("a").Command("x", 1)
	_, req := asked(t, effs)
	if _, _, res := c.Handle(said(req, rejected{"no"}, nil)); res == nil || res.Err == nil || res.Err.Error() != "no" {
		t.Errorf("rejected: done %#v", res)
	}
}

// TestClientStale has a client drop what answers a command cancelled, or
// an earlier attempt, and what is not its own.
func TestClientStale(t *testing.T) {
	c, effs := inSessionOn("a", "b").Command("x", 1)
	_, first := asked(t, effs)
	c, _ = c.Cancel("x")
	c, effs = c.Command("x", 2)
	_, second := asked(t, effs)
	other := inSessionOn("a")
	other.group = "h"
	other, effs = other.Command("x", 3)
	_, theirs := asked(t, effs)

	for _, msg := range []any{
		said(first, applied{Rep: 1}, nil),   // cancelled
		said(theirs, applied{Rep: 3}, nil),  // of group h
		actor.Response{Tag: "x"},            // not of a client
		retry{group: "g", tag: "x", try: 1}, // not waiting
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
	c, effs := inSessionOn("a").Command("x", 1)
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

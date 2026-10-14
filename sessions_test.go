package quorum

import (
	"testing"
	"time"
)

// A session remembers the replies its client may not have, and forgets
// those it has.
func TestSessionReplies(t *testing.T) {
	s := Sessions{}.opened(7)
	s = s.applied(Origin{Session: 7, Seq: 1}, "one")
	s = s.applied(Origin{Session: 7, Seq: 2}, "two")
	if open, done, rep := s.seen(Origin{Session: 7, Seq: 1}); !open || !done || rep != "one" {
		t.Fatalf("seen 1: %v %v %v", open, done, rep)
	}
	if open, done, _ := s.seen(Origin{Session: 7, Seq: 3}); !open || done {
		t.Fatalf("seen 3: %v %v", open, done)
	}
	s = s.applied(Origin{Session: 7, Seq: 3, Acked: 2}, "three")
	if got := s.Open[7].Replies; len(got) != 1 || got[3] != "three" {
		t.Errorf("replies after 2 acked: %v", got)
	}
	if open, _, _ := s.seen(Origin{Session: 8, Seq: 1}); open {
		t.Error("session 8 open")
	}
}

// A session expires once the log has not seen it for the timeout, by the
// time of the log, which goes only forward.
func TestSessionExpires(t *testing.T) {
	const timeout = time.Minute
	s := Sessions{}.passed(int64(time.Hour), timeout).opened(7)
	s = s.passed(int64(time.Hour+timeout/2), timeout)
	s = s.passed(int64(time.Hour), timeout) // a leader whose clock is behind
	if s.Time != int64(time.Hour+timeout/2) {
		t.Errorf("time went back to %v", time.Duration(s.Time))
	}
	if _, ok := s.Open[7]; !ok {
		t.Fatal("expired within the timeout")
	}
	s = s.passed(int64(time.Hour+2*timeout), timeout)
	if _, ok := s.Open[7]; ok {
		t.Error("open past the timeout")
	}
}

// A command of a session sent again is answered with the first reply, not
// applied twice; one of a session expired is not applied.
func TestSessionCommands(t *testing.T) {
	g := newGroup(t, 1, three)
	v, err := g.ask(openSession{})
	if err != nil {
		t.Fatal(err)
	}
	id := v.(applied).Rep.(uint64)
	add := command{Cmd: 1, From: Origin{Session: id, Seq: 1}}
	for range 3 {
		if v, err := g.ask(add); err != nil || v != (applied{Rep: 1}) {
			t.Fatalf("add: %#v, %v", v, err)
		}
	}

	g.s.Advance(2 * DefaultSessionTimeout)
	if sum, err := g.command(10); err != nil || sum != 11 {
		t.Fatalf("command without a session: %d, %v", sum, err)
	}
	late := command{Cmd: 1, From: Origin{Session: id, Seq: 2, Acked: 1}}
	if v, err := g.s.Call(g.at(g.leader()), late); err != nil || v != (expired{}) {
		t.Errorf("in the session expired: %#v, %v", v, err)
	}
	if _, d, _ := g.member(g.leader()); d.state != 11 {
		t.Errorf("holds %d, want 11", d.state)
	}
}

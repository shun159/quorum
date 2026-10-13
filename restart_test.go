package quorum

import (
	"slices"
	"testing"
	"time"
)

// A follower restarted comes back with what it saved, and catches up.
func TestFollowerRestart(t *testing.T) {
	g := newGroup(t, 1, three)
	for range 3 {
		if _, err := g.command(1); err != nil {
			t.Fatal(err)
		}
	}
	l := g.leader()
	f := without(three, slices.Index(three, l))[0]
	g.crash(f, 0)
	for range 2 {
		if _, err := g.command(1); err != nil {
			t.Fatal(err)
		}
	}
	g.restart(f)
	g.run(2 * time.Second)
	if _, d, _ := g.member(f); d.state != 5 {
		t.Errorf("%s holds %d, want 5", f, d.state)
	}
}

// The leader restarted: the others go on without it, and it rejoins them,
// with what was committed.
func TestLeaderRestart(t *testing.T) {
	g := newGroup(t, 1, three)
	for range 3 {
		if _, err := g.command(1); err != nil {
			t.Fatal(err)
		}
	}
	old := g.leader()
	g.crash(old, 0)
	g.run(2 * time.Second)
	if sum, err := g.command(1); err != nil || sum != 4 {
		t.Fatalf("command without %s: %d, %v", old, sum, err)
	}
	g.restart(old)
	g.run(2 * time.Second)
	if _, d, _ := g.member(old); d.state != 4 {
		t.Errorf("%s holds %d, want 4", old, d.state)
	}
}

// All members restarted at once lose nothing committed.
func TestAllRestart(t *testing.T) {
	g := newGroup(t, 1, three)
	for range 3 {
		if _, err := g.command(1); err != nil {
			t.Fatal(err)
		}
	}
	for _, m := range three {
		g.crash(m, 0)
	}
	for _, m := range three {
		g.restart(m)
	}
	if sum, err := g.command(1); err != nil || sum != 4 {
		t.Errorf("command after all restarted: %d, %v", sum, err)
	}
}

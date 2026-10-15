package raft

import (
	"testing"
	"time"

	"github.com/shun159/molecule/actor"
)

// clocked keeps the time of the last command, in UnixNano: it replies
// with it, and reads the time it reads at.
type clocked struct{}

func (clocked) Init() int { return 0 }

func (clocked) Apply(int, int) (int, int, []actor.Effect) { panic("Apply, not ApplyAt") }

func (clocked) ApplyAt(_ int, _ int, at time.Time) (int, int, []actor.Effect) {
	return int(at.UnixNano()), int(at.UnixNano()), nil
}

func (clocked) ReadAt(_ int, _ int, at time.Time) int { return int(at.UnixNano()) }

// A Timed machine applies each command at the time the leader took it,
// the same on every member, going forward; it reads at the leader's time,
// not before the last command's, one entry bringing the log to it for
// the reads of a while.
func TestTimed(t *testing.T) {
	g := newGroupOf(t, 1, three, func(string) Machine[int, int, int] { return clocked{} })
	first, err := g.command(0)
	if err != nil {
		t.Fatal(err)
	}
	g.s.Advance(10 * time.Second)
	second, err := g.command(0)
	if err != nil {
		t.Fatal(err)
	}
	if d := time.Duration(second - first); d < 10*time.Second {
		t.Errorf("applied %v apart, past 10s", d)
	}
	g.run(time.Second)
	for _, m := range g.members {
		if _, d, _ := g.member(m); d.state != second {
			t.Errorf("%s at %d, not %d", m, d.state, second)
		}
	}
	g.s.Advance(time.Minute)
	v, err := g.ask(read{0})
	if err != nil {
		t.Fatal(err)
	}
	if at := v.(applied).Rep.(int); at < second+int(time.Minute) || int64(at) > g.s.Now().UnixNano() {
		t.Errorf("read at %v, the last command at %v, now %v", time.Unix(0, int64(at)), time.Unix(0, int64(second)), g.s.Now())
	}
	_, d, _ := g.member(g.leader())
	for range 3 {
		if _, err := g.ask(read{0}); err != nil {
			t.Fatal(err)
		}
	}
	if _, after, _ := g.member(g.leader()); after.log.lastIndex() != d.log.lastIndex() {
		t.Errorf("3 reads at once appended %d entries", after.log.lastIndex()-d.log.lastIndex())
	}
}

// The clocks of the members are off from each other: commands are applied
// at times going forward all the same, and a command after a read, at a
// time no earlier than the read's.
func TestTimedSkewed(t *testing.T) {
	g := newGroupOf(t, 1, three, func(string) Machine[int, int, int] { return clocked{} })
	g.s.Skew("b", -10*time.Second)
	g.s.Skew("c", 10*time.Second)
	g.run(time.Second)
	lead := func(to string) {
		t.Helper()
		if g.leader() != to {
			if v, err := g.ask(transferLeader{To: to}); err != nil || v != (applied{}) {
				t.Fatalf("transfer to %s: %#v, %v", to, v, err)
			}
		}
		g.run(time.Second)
	}
	var last int
	for _, to := range []string{"c", "b", "a", "c", "b"} {
		lead(to)
		at, err := g.command(0)
		if err != nil {
			t.Fatal(err)
		}
		if at < last {
			t.Errorf("%s applied a command at %v, before %v", to, time.Unix(0, int64(at)), time.Unix(0, int64(last)))
		}
		g.s.Advance(time.Second)
		v, err := g.ask(read{0})
		if err != nil {
			t.Fatal(err)
		}
		last = max(at, v.(applied).Rep.(int))
	}
}

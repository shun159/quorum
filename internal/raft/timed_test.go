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
// not before the last command's.
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
}

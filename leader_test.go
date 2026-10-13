package quorum

import (
	"slices"
	"testing"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/gensim"
	"github.com/shun159/molecule/proc"
)

// seen is what a member told the probe: Kind is "elected", "surrendered"
// or "applied".
type seen struct {
	Kind string
	Node string
	Term uint64 // of an election
	Sum  int    // of a command applied
}

// probe keeps what it is told, on a node of its own, which partitions of
// the members do not cut off.
type probe struct{ genserver.Default[[]seen] }

func (probe) HandleInfo(log []seen, msg any) ([]seen, []actor.Effect) {
	return append(slices.Clip(log), msg.(seen)), nil
}

var probeAt = actor.Remote{Node: "probe", Name: "probe"}

// watched is a counter telling the probe when its member is elected or
// surrenders, and, as its effect, each command applied.
type watched struct {
	counter
	node string
}

func (w watched) Apply(sum, n int) (int, int, []actor.Effect) {
	sum, rep, _ := w.counter.Apply(sum, n)
	return sum, rep, actor.Do(w.tell(seen{Kind: "applied", Sum: sum}))
}

func (w watched) Elected(_ int, term uint64) []actor.Effect {
	return actor.Do(w.tell(seen{Kind: "elected", Term: term}))
}

func (w watched) Surrendered(int) []actor.Effect {
	return actor.Do(w.tell(seen{Kind: "surrendered"}))
}

func (w watched) tell(s seen) actor.Effect {
	s.Node = w.node
	return actor.Send{To: probeAt, Msg: s}
}

// newWatchedGroup starts a group of watched counters, and the probe.
func newWatchedGroup(t *testing.T, seed uint64, members []string) (*group, proc.PID) {
	t.Helper()
	g := newGroupOf(t, seed, members, func(node string) Machine[int, int, int] { return watched{node: node} })
	p, err := gensim.Spawn(g.s, genserver.Gen(probe{}), nil, gensim.On(probeAt.Node), gensim.Named(probeAt.Name))
	if err != nil {
		t.Fatal(err)
	}
	return g, p
}

// told returns what the probe was told of kind.
func told(g *group, p proc.PID, kind string) []seen {
	log, _ := gensim.State[[]seen](g.s, p)
	var of []seen
	for _, s := range log {
		if s.Kind == kind {
			of = append(of, s)
		}
	}
	return of
}

// The member elected is told so, with its term, once.
func TestElected(t *testing.T) {
	g, p := newWatchedGroup(t, 1, three)
	g.run(2 * time.Second)
	l, _ := g.leader()
	_, d, _ := g.member(l)
	if got, want := told(g, p, "elected"), []seen{{Kind: "elected", Node: l, Term: d.term}}; !slices.Equal(got, want) {
		t.Errorf("elected %v, want %v", got, want)
	}
}

// A leader cut off from the others surrenders, not hearing from a
// majority, while they elect another.
func TestSurrendered(t *testing.T) {
	g, p := newWatchedGroup(t, 1, three)
	g.run(2 * time.Second)
	old, _ := g.leader()
	_, d, _ := g.member(old)
	oldTerm := d.term

	g.s.Partition([]string{old}, without(three, slices.Index(three, old)))
	g.run(2 * time.Second)
	if got, want := told(g, p, "surrendered"), []seen{{Kind: "surrendered", Node: old}}; !slices.Equal(got, want) {
		t.Errorf("surrendered %v, want %v", got, want)
	}
	if r, _, _ := g.member(old); r == leader {
		t.Errorf("%s still leads, cut off", old)
	}
	l, _ := g.leader()
	_, d, _ = g.member(l)
	if l == old || d.term <= oldTerm {
		t.Fatalf("leader %s of term %d, after %s of term %d", l, d.term, old, oldTerm)
	}
	if got := told(g, p, "elected"); len(got) != 2 || got[1] != (seen{Kind: "elected", Node: l, Term: d.term}) {
		t.Errorf("elected %v", got)
	}
}

// The effects of a command are performed once, by the leader, though
// every member applies it.
func TestEffectsOnLeader(t *testing.T) {
	g, p := newWatchedGroup(t, 1, three)
	for range 3 {
		if _, err := g.command(1); err != nil {
			t.Fatal(err)
		}
	}
	g.run(time.Second)
	l, _ := g.leader()
	want := []seen{{Kind: "applied", Node: l, Sum: 1}, {Kind: "applied", Node: l, Sum: 2}, {Kind: "applied", Node: l, Sum: 3}}
	if got := told(g, p, "applied"); !slices.Equal(got, want) {
		t.Errorf("applied %v, want %v", got, want)
	}
}

// A leader cut off surrenders at most ElectionTimeout/checksPerTimeout
// after the others elect another: two members never lead at once longer.
func TestLeadersOverlap(t *testing.T) {
	bound := DefaultElectionTimeout / checksPerTimeout
	for seed := range uint64(20) {
		g := newGroup(t, seed, three)
		g.run(2 * time.Second)
		old, _ := g.leader()
		g.s.Partition([]string{old}, without(three, slices.Index(three, old)))

		var since time.Time // two members lead since then
		var longest time.Duration
		err := g.s.Run(2*time.Second, func() error {
			if g.leading() < 2 {
				since = time.Time{}
				return g.check()
			}
			if since.IsZero() {
				since = g.s.Now()
			}
			longest = max(longest, g.s.Now().Sub(since))
			return g.check()
		})
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		if longest >= bound {
			t.Errorf("seed %d: two members led at once for %v, not under %v", seed, longest, bound)
		}
	}
}

// leading counts the members that are leaders, of any term.
func (g *group) leading() int {
	count := 0
	for _, m := range g.members {
		if r, _, ok := g.member(m); ok && r == leader {
			count++
		}
	}
	return count
}

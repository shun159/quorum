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

// A leader cut off keeps leading until it hears of a later term: healed,
// it surrenders to the leader the others elected meanwhile.
func TestSurrendered(t *testing.T) {
	g, p := newWatchedGroup(t, 1, three)
	g.run(2 * time.Second)
	old, _ := g.leader()
	_, d, _ := g.member(old)
	oldTerm := d.term

	g.s.Partition([]string{old}, without(three, slices.Index(three, old)))
	g.run(2 * time.Second)
	if len(told(g, p, "surrendered")) != 0 {
		t.Fatal("surrendered while cut off")
	}

	g.s.Heal()
	g.run(time.Second)
	l, _ := g.leader()
	_, d, _ = g.member(l)
	if l == old || d.term <= oldTerm {
		t.Fatalf("leader %s of term %d, after %s of term %d", l, d.term, old, oldTerm)
	}
	if got, want := told(g, p, "surrendered"), []seen{{Kind: "surrendered", Node: old}}; !slices.Equal(got, want) {
		t.Errorf("surrendered %v, want %v", got, want)
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

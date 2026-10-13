package quorum

import (
	"math/rand/v2"
	"testing"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/gensim"
	"github.com/shun159/molecule/proc"
)

// client adds 1 to a group of counters every tick, one command at a time,
// and counts what comes of them.
type client struct {
	genserver.Default[outcomes]
	group   string
	members []string
}

// outcomes are what a client knows of its commands.
type outcomes struct {
	quorum  Client[int, int]
	acked   int // applied, the leader said
	unknown int // applied or not: the leader changed, or did not answer
}

type tick struct{}

const tickEvery = 50 * time.Millisecond

func (c client) Init(proc.PID) (outcomes, []actor.Effect, error) {
	o := outcomes{quorum: NewClient[int, int](c.group, c.members, time.Second)}
	return o, actor.Do(nextTick()), nil
}

func nextTick() actor.Effect {
	return actor.StartTimer{Key: tick{}, After: tickEvery, Msg: tick{}}
}

func (c client) HandleInfo(o outcomes, msg any) (outcomes, []actor.Effect) {
	if _, ok := msg.(tick); ok {
		return c.tick(o)
	}
	var effs []actor.Effect
	var res *Result[int]
	o.quorum, effs, res = o.quorum.Handle(msg)
	if res != nil {
		o = c.count(o, *res)
	}
	return o, effs
}

// tick sends the next command, unless one is out.
func (c client) tick(o outcomes) (outcomes, []actor.Effect) {
	effs := actor.Do(nextTick())
	if o.quorum.Out() > 0 {
		return o, effs
	}
	var ask []actor.Effect
	o.quorum, ask = o.quorum.Command("add", 1)
	return o, append(effs, ask...)
}

// count counts what came of a command.
func (c client) count(o outcomes, res Result[int]) outcomes {
	if res.Err == nil {
		o.acked++
	} else {
		o.unknown++
	}
	return o
}

// TestChaos has a client add to a group of three or five, while members
// are cut off or crashed at random, and messages lost, for many seeds.
func TestChaos(t *testing.T) {
	for seed := range uint64(30) {
		chaos(t, seed)
	}
}

// FuzzChaos is TestChaos with the order of events searched by the fuzzer,
// the faults still those of the seed.
func FuzzChaos(f *testing.F) {
	f.Add(uint64(0), []byte{})
	f.Add(uint64(1), []byte{3, 1, 4, 1, 5, 9, 2, 6})
	f.Fuzz(func(t *testing.T, seed uint64, schedule []byte) {
		chaos(t, seed, gensim.WithChoices(schedule))
	})
}

// chaos runs a group under partitions, crashes and lost messages, of three
// members or five as the seed has it,
// checking it at every step. Once healed and quiet, the members all hold the same
// count, which has every command acknowledged, and no more than were sent.
func chaos(t *testing.T, seed uint64, opts ...gensim.Option) {
	t.Helper()
	// Three are the most fragile: one member losing a write may lose what
	// was committed. Five see two members cut off at once.
	members := []string{"a", "b", "c", "d", "e"}[:[]int{3, 5}[seed%2]]
	g := newGroup(t, seed, members, opts...)
	g.s.Loss = 0.05
	cl, err := gensim.Spawn(g.s, genserver.Gen(client{group: g.name, members: members}), nil, gensim.On("client"))
	if err != nil {
		t.Fatal(err)
	}

	r := rand.New(rand.NewPCG(seed, 5))
	for range 20 {
		g.recover()
		g.disturb(r)
		g.run(time.Duration(200+r.IntN(800)) * time.Millisecond)
	}
	// The client stops, its count kept, for the group to settle.
	o, _ := gensim.State[outcomes](g.s, cl)
	g.s.Exit(cl, proc.Kill)
	g.recover()
	g.s.Loss = 0
	g.run(5 * time.Second)

	_, d, _ := g.member(members[0])
	for _, m := range members {
		if _, dm, _ := g.member(m); dm.state != d.state {
			t.Fatalf("seed %d: %s holds %d, %s %d", seed, m, dm.state, members[0], d.state)
		}
	}
	out := o.unknown + o.quorum.Out()
	if d.state < o.acked || d.state > o.acked+out {
		t.Fatalf("seed %d: count %d, with %d acknowledged and %d unknown", seed, d.state, o.acked, out)
	}
	if o.acked == 0 {
		t.Errorf("seed %d: nothing acknowledged", seed)
	}
}

// disturb may cut a member or two off the others, or crash one amid the
// steps to come, its storage keeping some of the writes it did not know
// were durable.
func (g *group) disturb(r *rand.Rand) {
	members := g.members
	switch r.IntN(4) {
	case 0: // all well
	case 1:
		i := r.IntN(len(members))
		g.s.Partition(members[i:i+1], without(members, i))
	case 2:
		perm := r.Perm(len(members))
		minority := []string{members[perm[0]], members[perm[1]]}
		var majority []string
		for _, i := range perm[2:] {
			majority = append(majority, members[i])
		}
		g.s.Partition(minority, majority)
	case 3:
		g.due = &crashLater{member: members[r.IntN(len(members))], steps: r.IntN(300), keep: r.IntN(3)}
	}
}

// recover heals the partitions, and restarts the members crashed.
func (g *group) recover() {
	g.due = nil
	g.s.Heal()
	for _, m := range g.members {
		if _, _, ok := g.member(m); !ok {
			g.restart(m)
		}
	}
}

func without(members []string, i int) []string {
	return append(append([]string{}, members[:i]...), members[i+1:]...)
}

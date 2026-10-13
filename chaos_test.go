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
	acked   int  // applied, the leader said
	unknown int  // applied or not: the leader changed, or did not answer
	busy    bool // a command is out
	next    int  // the member to ask
}

type tick struct{}

const tickEvery = 50 * time.Millisecond

func (c client) Init(proc.PID) (outcomes, []actor.Effect, error) {
	return outcomes{}, actor.Do(nextTick()), nil
}

func nextTick() actor.Effect {
	return actor.StartTimer{Key: tick{}, After: tickEvery, Msg: tick{}}
}

func (c client) HandleInfo(o outcomes, msg any) (outcomes, []actor.Effect) {
	switch m := msg.(type) {
	case tick:
		return c.tick(o)
	case actor.Response:
		return c.outcome(o, m), nil
	}
	return o, nil
}

// tick sends the next command, unless one is out.
func (c client) tick(o outcomes) (outcomes, []actor.Effect) {
	effs := actor.Do(nextTick())
	if o.busy {
		return o, effs
	}
	o.busy = true
	ask := actor.SendRequest{
		To:      actor.Remote{Node: c.members[o.next], Name: replicaName(c.group)},
		Req:     command{1},
		Tag:     "add",
		Timeout: time.Second,
	}
	return o, append(effs, ask)
}

// outcome counts the reply to a command, or its failure.
func (c client) outcome(o outcomes, m actor.Response) outcomes {
	o.busy = false
	switch r := m.Value.(type) {
	case applied:
		o.acked++
	case redirect:
		o.next = c.memberOrNext(r.Leader, o.next)
	default: // deposed, or no reply
		o.unknown++
		o.next = (o.next + 1) % len(c.members)
	}
	return o
}

// memberOrNext returns the index of member, or the one after i if member
// is not one, as when the leader is unknown.
func (c client) memberOrNext(member string, i int) int {
	for j, m := range c.members {
		if m == member {
			return j
		}
	}
	return (i + 1) % len(c.members)
}

// TestChaos has a client add to a group of five, while members are cut
// off at random and messages lost, for many seeds.
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

// chaos runs a group of five under partitions and lost messages, checking
// it at every step. Once healed and quiet, the members all hold the same
// count, which has every command acknowledged, and no more than were sent.
func chaos(t *testing.T, seed uint64, opts ...gensim.Option) {
	t.Helper()
	five := []string{"a", "b", "c", "d", "e"}
	g := newGroup(t, seed, five, opts...)
	g.s.Loss = 0.05
	cl, err := gensim.Spawn(g.s, genserver.Gen(client{group: g.name, members: five}), nil, gensim.On("client"))
	if err != nil {
		t.Fatal(err)
	}

	r := rand.New(rand.NewPCG(seed, 5))
	for range 20 {
		cutOff(g.s, r, five)
		g.run(time.Duration(200+r.IntN(800)) * time.Millisecond)
	}
	// The client stops, its count kept, for the group to settle.
	o, _ := gensim.State[outcomes](g.s, cl)
	g.s.Exit(cl, proc.Kill)
	g.s.Heal()
	g.s.Loss = 0
	g.run(5 * time.Second)

	_, d, _ := g.member(five[0])
	for _, m := range five {
		if _, dm, _ := g.member(m); dm.state != d.state {
			t.Fatalf("seed %d: %s holds %d, %s %d", seed, m, dm.state, five[0], d.state)
		}
	}
	out := o.unknown
	if o.busy {
		out++
	}
	if d.state < o.acked || d.state > o.acked+out {
		t.Fatalf("seed %d: count %d, with %d acknowledged and %d unknown", seed, d.state, o.acked, out)
	}
	if o.acked == 0 {
		t.Errorf("seed %d: nothing acknowledged", seed)
	}
}

// cutOff heals the group, then may cut a member or two off the others.
func cutOff(s *gensim.Sim, r *rand.Rand, members []string) {
	s.Heal()
	switch r.IntN(3) {
	case 0: // all together
	case 1:
		i := r.IntN(len(members))
		s.Partition(members[i:i+1], without(members, i))
	case 2:
		perm := r.Perm(len(members))
		minority := []string{members[perm[0]], members[perm[1]]}
		var majority []string
		for _, i := range perm[2:] {
			majority = append(majority, members[i])
		}
		s.Partition(minority, majority)
	}
}

func without(members []string, i int) []string {
	return append(append([]string{}, members[:i]...), members[i+1:]...)
}

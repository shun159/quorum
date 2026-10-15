package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/proc"
	"github.com/shun159/quorum"
)

// sender puts keys one after the other, in a session of its own: the keys
// from first, every step, below until, to load them; or keys of those at
// random, until end, to measure.
type sender struct {
	genserver.Default[sent]
	first, step, until int
	random             bool
	end                time.Time
	val                []byte
}

type sent struct {
	client  quorum.Client[Put, int]
	next    int
	started time.Time
	lat     []time.Duration
	failed  int
	done    bool
	rng     *rand.Rand
}

type (
	putTag     struct{}
	resultsReq struct{}
	results    struct {
		lat    []time.Duration
		failed int
		done   bool
	}
)

func key(i int) string { return fmt.Sprintf("k%09d", i) }

func (s sender) Init(self proc.PID) (sent, []actor.Effect, error) {
	st := sent{client: quorum.NewClient[Put, int](group, names, 5*time.Second), next: s.first,
		rng: rand.New(rand.NewPCG(uint64(s.first), 1))}
	st, effs := s.put(st)
	return st, effs, nil
}

// put puts the next key, unless done.
func (s sender) put(st sent) (sent, []actor.Effect) {
	var k string
	switch {
	case s.random && time.Now().Before(s.end):
		k = key(st.rng.IntN(s.until))
	case !s.random && st.next < s.until:
		k, st.next = key(st.next), st.next+s.step
	default:
		st.done = true
		return st, nil
	}
	var effs []actor.Effect
	st.started = time.Now()
	st.client, effs = st.client.Command(putTag{}, Put{Key: k, Val: s.val})
	return st, effs
}

func (s sender) HandleInfo(st sent, msg any) (sent, []actor.Effect) {
	var effs []actor.Effect
	var res *quorum.Result[int]
	st.client, effs, res = st.client.Handle(msg)
	if res == nil {
		return st, effs
	}
	if res.Err != nil {
		st.failed++
	} else {
		st.lat = append(st.lat, time.Since(st.started))
	}
	st, more := s.put(st)
	return st, append(effs, more...)
}

func (s sender) HandleCall(st sent, _ resultsReq, from genserver.From[results]) (sent, []actor.Effect) {
	return st, actor.Do(from.Reply(results{st.lat, st.failed, st.done}))
}

type senders []genserver.Ref[resultsReq, results, genserver.None]

func startSenders(n *proc.Node, count int, s sender) (senders, error) {
	var ss senders
	for i := range count {
		s.first, s.step = i, count
		ref, err := genserver.Start(context.Background(), n, s)
		if err != nil {
			return nil, err
		}
		ss = append(ss, ref)
	}
	return ss, nil
}

// wait waits until the senders are done, and returns the latencies of
// what they put, and how many puts failed.
func (ss senders) wait(n *proc.Node) ([]time.Duration, int, error) {
	var all []time.Duration
	failed := 0
	for _, ref := range ss {
		for {
			r, err := ref.Call(context.Background(), n, resultsReq{})
			if err != nil {
				return nil, 0, err
			}
			if r.done {
				all, failed = append(all, r.lat...), failed+r.failed
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		ref.Stop(context.Background(), n)
	}
	return all, failed, nil
}

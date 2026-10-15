package main

import (
	"context"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/proc"
	"github.com/shun159/quorum"
)

// sender sends a group commands one after the other until end, as a
// quorum.Client does: in a session of its own, to the leader it knows.
type sender struct {
	genserver.Default[sent]
	group string
	end   time.Time
}

// sent is what came of the commands of a sender.
type sent struct {
	client  quorum.Client[int, int]
	started time.Time // the command out
	lat     []time.Duration
	failed  int
}

type (
	commandTag struct{}
	resultsReq struct{}
	results    struct {
		lat    []time.Duration
		failed int
	}
)

func (s sender) Init(proc.PID) (sent, []actor.Effect, error) {
	st := sent{client: quorum.NewClient[int, int](s.group, names, 5*time.Second)}
	st, effs := s.next(st)
	return st, effs, nil
}

// next sends the next command, unless the time is up.
func (s sender) next(st sent) (sent, []actor.Effect) {
	if time.Now().After(s.end) {
		return st, nil
	}
	var effs []actor.Effect
	st.started = time.Now()
	st.client, effs = st.client.Command(commandTag{}, 1)
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
	st, more := s.next(st)
	return st, append(effs, more...)
}

func (s sender) HandleCall(st sent, _ resultsReq, from genserver.From[results]) (sent, []actor.Effect) {
	return st, actor.Do(from.Reply(results{st.lat, st.failed}))
}

type senders []genserver.Ref[resultsReq, results, genserver.None]

// startSenders starts clients senders for each group, sending until end.
func startSenders(n *proc.Node, groups, clients int, end time.Time) (senders, error) {
	var ss senders
	for g := range groups {
		for range clients {
			ref, err := genserver.Start(context.Background(), n, sender{group: group(g), end: end})
			if err != nil {
				return nil, err
			}
			ss = append(ss, ref)
		}
	}
	return ss, nil
}

// results returns the latencies of the commands applied, and how many
// failed, and stops the senders.
func (ss senders) results(n *proc.Node) ([]time.Duration, int, error) {
	var all []time.Duration
	failed := 0
	for _, ref := range ss {
		r, err := ref.Call(context.Background(), n, resultsReq{})
		if err != nil {
			return nil, 0, err
		}
		all, failed = append(all, r.lat...), failed+r.failed
		ref.Stop(context.Background(), n)
	}
	return all, failed, nil
}

package main

import (
	"context"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/behaviours/supervisor"
	"github.com/shun159/molecule/proc"
	"github.com/shun159/quorum"
)

// subsName is the supervisor of the subscribers of a node.
var subsName = actor.Local("pubsub_subs")

// pollEvery is how long a subscriber waits for messages, having read all
// there were.
const pollEvery = 200 * time.Millisecond

// subscriber reads Topic for Sub, from where Sub has read it to, on any
// node: it shows each message on Display, then commits that it read it.
// A message shown is not shown again, but if the subscriber stops between
// the two: at least once.
type subscriber struct {
	genserver.Default[reading]
	Svc     Service
	Sub     string
	Topic   string
	Display proc.PID
}

// reading is where a subscriber is.
type reading struct {
	client quorum.Client[Op, Reply]
	next   uint64 // the offset of the next message to read
	known  bool   // next read from the group
}

type (
	whereTag  struct{}
	fetchTag  struct{}
	commitTag struct{}
	poll      struct{}
)

func (s subscriber) Init(proc.PID) (reading, []actor.Effect, error) {
	r := reading{client: quorum.NewClient[Op, Reply](group, s.Svc.Nodes, time.Second)}
	r, effs := s.read(r)
	return r, effs, nil
}

// read asks where Sub has read to, until known, then the next messages.
func (s subscriber) read(r reading) (reading, []actor.Effect) {
	var effs []actor.Effect
	if !r.known {
		r.client, effs = r.client.Read(whereTag{}, Op{Kind: Where, Sub: s.Sub, Topic: s.Topic})
		return r, effs
	}
	r.client, effs = r.client.Read(fetchTag{}, Op{Kind: Fetch, Topic: s.Topic, Offset: r.next, Max: 100})
	return r, effs
}

func later() actor.Effect { return actor.StartTimer{Key: poll{}, After: pollEvery, Msg: poll{}} }

func (s subscriber) HandleInfo(r reading, msg any) (reading, []actor.Effect) {
	if _, ok := msg.(poll); ok {
		return s.read(r)
	}
	var effs []actor.Effect
	var res *quorum.Result[Reply]
	r.client, effs, res = r.client.Handle(msg)
	if res == nil {
		return r, effs
	}
	switch {
	case res.Tag == commitTag{}: // a later commit makes up for one lost
		return r, effs
	case res.Err != nil:
		return r, append(effs, later())
	case res.Tag == whereTag{}:
		r.next, r.known = res.Rep.Offset, true
	default: // the messages fetched
		if len(res.Rep.Msgs) == 0 {
			return r, append(effs, later())
		}
		for _, m := range res.Rep.Msgs {
			effs = append(effs, actor.Send{To: s.Display, Msg: m})
		}
		r.next = res.Rep.Offset
		var commit []actor.Effect
		r.client, commit = r.client.Command(commitTag{}, Op{Kind: Commit, Sub: s.Sub, Topic: s.Topic, Offset: r.next})
		effs = append(effs, commit...)
	}
	r, more := s.read(r)
	return r, append(effs, more...)
}

// Subscribe starts, on the node of caller, the subscriber sub of topic,
// showing what it reads on display.
func Subscribe(ctx context.Context, caller actor.Caller, svc Service, sub, topic string, display proc.PID) (proc.PID, error) {
	b := subscriber{Svc: svc, Sub: sub, Topic: topic, Display: display}
	return supervisor.StartChild(ctx, caller, subsName, supervisor.ChildSpec{Start: genserver.Child(b), Restart: supervisor.Transient})
}

// Unsubscribe stops the subscriber pid: where it read to stays.
func Unsubscribe(ctx context.Context, caller actor.Caller, pid proc.PID) error {
	return supervisor.TerminateChild(ctx, caller, subsName, pid)
}

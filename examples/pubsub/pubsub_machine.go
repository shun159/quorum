package main

import (
	"context"
	"slices"
	"strconv"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/dist"
	"github.com/shun159/molecule/pmap"
	"github.com/shun159/quorum"
)

// group is the group of the topics.
const group = "pubsub"

// Topics is the state of the machine: the messages of each topic, the
// latest of them, and where each subscriber of a topic has read to.
type Topics struct {
	Logs      pmap.Map[string, Log]
	Positions pmap.Map[Position, uint64] // the offset of the next to read
}

// Log is the messages of a topic kept, by offset, from First to Next.
type Log struct {
	First, Next uint64
	Msgs        pmap.Map[uint64, Message]
}

// Message is a message published on a topic, at Offset.
type Message struct {
	Topic  string
	Offset uint64
	Text   string
	From   string // the node it was published on
}

func (m Message) String() string {
	return "[" + m.Topic + " " + itoa(m.Offset) + "] " + m.Text + " (from " + m.From + ")"
}

// Position names where a subscriber has read a topic to.
type Position struct{ Sub, Topic string }

// Op is a command or a read of the machine.
type Op struct {
	Kind   Kind
	Topic  string
	Sub    string
	Text   string
	From   string
	Offset uint64 // of Fetch, the first wanted; of Commit, the next to read
	Max    int    // of Fetch, at most
}

type Kind int

const (
	Publish Kind = iota // a command: Text on Topic; replies its offset
	Commit              // a command: Sub has read Topic up to Offset
	Fetch               // a read: Max messages of Topic from Offset
	Where               // a read: where Sub has read Topic to
	List                // a read: the topics
)

// Reply is what a command or a read replies.
type Reply struct {
	Offset uint64    // of Publish, the message's; of Where, the next to read; of Fetch, the next after
	Msgs   []Message // of Fetch
	Topics []string  // of List, each with its offsets
}

func init() { dist.Register(Op{}, Reply{}, Topics{}) }

// machine keeps the last keep messages of each topic.
type machine struct{ keep uint64 }

func (machine) Init() Topics { return Topics{} }

func (m machine) Apply(t Topics, op Op) (Topics, Reply, []actor.Effect) {
	switch op.Kind {
	case Publish:
		l, _ := t.Logs.Get(op.Topic)
		msg := Message{Topic: op.Topic, Offset: l.Next, Text: op.Text, From: op.From}
		l.Msgs, l.Next = l.Msgs.Set(msg.Offset, msg), l.Next+1
		for l.Next-l.First > m.keep {
			l.Msgs, l.First = l.Msgs.Delete(l.First), l.First+1
		}
		t.Logs = t.Logs.Set(op.Topic, l)
		return t, Reply{Offset: msg.Offset}, nil
	case Commit:
		p := Position{op.Sub, op.Topic}
		if at, _ := t.Positions.Get(p); op.Offset > at {
			t.Positions = t.Positions.Set(p, op.Offset)
		}
	}
	return t, Reply{}, nil
}

func (machine) Read(t Topics, op Op) Reply {
	switch op.Kind {
	case Fetch:
		l, _ := t.Logs.Get(op.Topic)
		var r Reply
		for i := max(op.Offset, l.First); i < l.Next && len(r.Msgs) < op.Max; i++ {
			msg, _ := l.Msgs.Get(i)
			r.Msgs = append(r.Msgs, msg)
		}
		r.Offset = max(op.Offset, l.First) + uint64(len(r.Msgs))
		return r
	case Where:
		at, _ := t.Positions.Get(Position{op.Sub, op.Topic})
		return Reply{Offset: at}
	case List:
		var r Reply
		for name, l := range t.Logs.All() {
			r.Topics = append(r.Topics, name+" "+itoa(l.First)+".."+itoa(l.Next))
		}
		slices.Sort(r.Topics) // All has no order
		return r
	}
	return Reply{}
}

// Service is the group on Nodes.
type Service struct{ Nodes []string }

// Publish publishes text on topic, from node, and returns its offset.
func (s Service) Publish(ctx context.Context, caller actor.Caller, node, topic, text string) (uint64, error) {
	r, err := quorum.Command[Reply](ctx, caller, group, s.Nodes, Op{Kind: Publish, Topic: topic, Text: text, From: node})
	return r.Offset, err
}

// Topics returns the topics, each with its offsets kept.
func (s Service) Topics(ctx context.Context, caller actor.Caller) ([]string, error) {
	r, err := quorum.Read[Reply](ctx, caller, group, s.Nodes, Op{Kind: List})
	return r.Topics, err
}

func itoa(n uint64) string { return strconv.FormatUint(n, 10) }

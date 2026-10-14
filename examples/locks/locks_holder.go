package main

import (
	"context"
	"fmt"
	"io"
	"maps"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/proc"
)

// holderName is the name of the holder on every node.
const holderName = actor.Local("locks_holder")

// holderAt is the holder on node, which owners of locks are named after.
func holderAt(node string) actor.Remote { return actor.Remote{Node: node, Name: string(holderName)} }

// Granted tells the holder of a node that the node holds the lock Name,
// with Token; Released, that it holds it no more. The leader of the group
// sends them as it applies the lock and the unlock, the effects of the
// machine: a leader lost before it does may not. They are for show: a
// node does not learn again, restarted, what it holds.
type (
	Granted struct {
		Name  string
		Token uint64
	}
	Released struct {
		Name  string
		Token uint64
	}
)

// Leading tells the holder of a node that its member of Group leads it,
// in Term; Following, that it leads no more. The member sends them as it
// becomes the leader, and stops being one.
type (
	Leading struct {
		Group string
		Term  uint64
	}
	Following struct{ Group string }
)

// leadsReq asks the holder of a node which groups the node leads; Leads
// answers, the term by group.
type (
	leadsReq struct{}
	Leads    struct{ Groups map[string]uint64 }
)

// holder shows, on its display, the locks its node takes and frees, and
// the groups it comes to lead, and leads no more; it tells which it
// leads.
type holder struct {
	genserver.Default[holding]
	Display proc.PID
}

// holding is what the node holds: the token by lock, and the term by
// group led.
type holding struct {
	held  map[string]uint64
	leads map[string]uint64
}

func (h holder) HandleInfo(s holding, msg any) (holding, []actor.Effect) {
	switch m := msg.(type) {
	case Granted:
		s.held = with(s.held, m.Name, m.Token)
		return s, actor.Do(h.show("this node holds the lock %q, token %d", m.Name, m.Token))
	case Released:
		if s.held[m.Name] != m.Token {
			return s, nil
		}
		s.held = without(s.held, m.Name)
		return s, actor.Do(h.show("this node freed the lock %q", m.Name))
	case Leading:
		s.leads = with(s.leads, m.Group, m.Term)
		return s, actor.Do(h.show("this node leads %s, term %d", m.Group, m.Term))
	case Following:
		s.leads = without(s.leads, m.Group)
		return s, actor.Do(h.show("this node leads %s no more", m.Group))
	}
	return s, nil
}

func (holder) HandleCall(s holding, _ leadsReq, from genserver.From[Leads]) (holding, []actor.Effect) {
	return s, actor.Do(from.Reply(Leads{maps.Clone(s.leads)}))
}

// with returns a copy of m with k set to v.
func with(m map[string]uint64, k string, v uint64) map[string]uint64 {
	m = maps.Clone(m)
	if m == nil {
		m = map[string]uint64{}
	}
	m[k] = v
	return m
}

// without returns a copy of m without k.
func without(m map[string]uint64, k string) map[string]uint64 {
	m = maps.Clone(m)
	delete(m, k)
	return m
}

func (h holder) show(format string, args ...any) actor.Effect {
	return actor.Send{To: h.Display, Msg: "*** " + fmt.Sprintf(format, args...)}
}

// startDisplay starts the process that prints the lines it is sent to w.
func startDisplay(n *proc.Node, w io.Writer) proc.PID {
	return n.Spawn(func(s *proc.Self) error {
		for {
			msg, err := s.Receive(context.Background())
			if err != nil {
				return err
			}
			if line, ok := msg.(string); ok {
				fmt.Fprintln(w, line)
			}
		}
	})
}

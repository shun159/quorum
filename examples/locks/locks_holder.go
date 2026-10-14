package main

import (
	"context"
	"fmt"
	"io"
	"maps"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/proc"
	"github.com/shun159/quorum"
)

// holderName is the name of the holder on every node.
const holderName = actor.Local("locks_holder")

// holderAt is the holder on node, which owners of locks are named after.
func holderAt(node string) actor.Remote { return actor.Remote{Node: node, Name: string(holderName)} }

// Granted tells the holder of a node that the node holds the lock Name,
// with Token; Released, that it freed it; Lost, that another took it, its
// lease expired. The leader of the group sends them as it applies the
// commands, the effects of the machine: a leader lost before it does may
// not. Restarted, a holder does not learn again what its node holds, and
// renews none: they expire.
type (
	Granted struct {
		Name  string
		Token uint64
	}
	Released struct {
		Name  string
		Token uint64
	}
	Lost struct {
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

// holder shows, on its display, the locks Node takes, frees and loses,
// and the groups it comes to lead, and leads no more; it tells which it
// leads. It renews the leases of the locks Node holds, a third of a lease
// apart, through a client of each group of Svc.
type holder struct {
	genserver.Default[holding]
	Display proc.PID
	Svc     Service
	Node    string
}

// holding is what the node holds: the token by lock, and the term by
// group led; and the clients renewing the locks, by group.
type holding struct {
	held    map[string]uint64
	leads   map[string]uint64
	clients map[string]quorum.Client[Op, Reply]
}

// renew is the time to renew the leases.
type renew struct{}

func (h holder) HandleInfo(s holding, msg any) (holding, []actor.Effect) {
	switch m := msg.(type) {
	case Granted:
		s.held = with(s.held, m.Name, m.Token)
		return s, actor.Do(h.show("this node holds the lock %q, token %d", m.Name, m.Token), h.renewLater())
	case Released:
		if s.held[m.Name] != m.Token {
			return s, nil
		}
		s.held = without(s.held, m.Name)
		return s, actor.Do(h.show("this node freed the lock %q", m.Name))
	case Lost:
		return h.lost(s, m.Name, m.Token)
	case renew:
		return h.renew(s)
	case Leading:
		s.leads = with(s.leads, m.Group, m.Term)
		return s, actor.Do(h.show("this node leads %s, term %d", m.Group, m.Term))
	case Following:
		s.leads = without(s.leads, m.Group)
		return s, actor.Do(h.show("this node leads %s no more", m.Group))
	}
	return h.answered(s, msg)
}

// lost forgets the lock name, held with token, which the node lost.
func (h holder) lost(s holding, name string, token uint64) (holding, []actor.Effect) {
	if s.held[name] != token {
		return s, nil
	}
	s.held = without(s.held, name)
	return s, actor.Do(h.show("this node lost the lock %q, its lease expired", name))
}

// renew renews the lease of every lock the node holds, and has it done
// again later, while it holds any.
func (h holder) renew(s holding) (holding, []actor.Effect) {
	if len(s.held) == 0 {
		return s, nil
	}
	effs := actor.Do(h.renewLater())
	for name := range s.held {
		g := h.Svc.groupOf(name)
		c, ok := s.clients[g]
		if !ok {
			c = quorum.NewClient[Op, Reply](g, h.Svc.Nodes, time.Second)
		}
		var more []actor.Effect
		c, more = c.Command(name, Op{Kind: Acquire, Name: name, Owner: h.Node, Lease: h.Svc.Lease})
		s.clients = with(s.clients, g, c)
		effs = append(effs, more...)
	}
	return s, effs
}

// answered hands msg to the clients: a renewal refused, the lock was
// lost.
func (h holder) answered(s holding, msg any) (holding, []actor.Effect) {
	var effs []actor.Effect
	for g, c := range s.clients {
		c, more, res := c.Handle(msg)
		if more == nil && res == nil {
			continue
		}
		s.clients = with(s.clients, g, c)
		effs = append(effs, more...)
		if res == nil {
			continue
		}
		name, _ := res.Tag.(string)
		if token, held := s.held[name]; held && res.Err == nil && !res.Rep.OK {
			var lost []actor.Effect
			s, lost = h.lost(s, name, token)
			effs = append(effs, lost...)
		}
	}
	return s, effs
}

func (h holder) renewLater() actor.Effect {
	return actor.StartTimer{Key: renew{}, After: h.Svc.Lease / 3, Msg: renew{}}
}

func (holder) HandleCall(s holding, _ leadsReq, from genserver.From[Leads]) (holding, []actor.Effect) {
	return s, actor.Do(from.Reply(Leads{maps.Clone(s.leads)}))
}

// with returns a copy of m with k set to v.
func with[V any](m map[string]V, k string, v V) map[string]V {
	m = maps.Clone(m)
	if m == nil {
		m = map[string]V{}
	}
	m[k] = v
	return m
}

// without returns a copy of m without k.
func without[V any](m map[string]V, k string) map[string]V {
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

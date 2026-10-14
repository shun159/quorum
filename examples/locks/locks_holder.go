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

// holder shows, on its display, the locks its node takes and frees: the
// token by lock.
type holder struct {
	genserver.Default[map[string]uint64]
	Display proc.PID
}

func (h holder) HandleInfo(held map[string]uint64, msg any) (map[string]uint64, []actor.Effect) {
	switch m := msg.(type) {
	case Granted:
		held = maps.Clone(held)
		if held == nil {
			held = map[string]uint64{}
		}
		held[m.Name] = m.Token
		return held, actor.Do(h.show("this node holds the lock %q, token %d", m.Name, m.Token))
	case Released:
		if held[m.Name] != m.Token {
			return held, nil
		}
		held = maps.Clone(held)
		delete(held, m.Name)
		return held, actor.Do(h.show("this node freed the lock %q", m.Name))
	}
	return held, nil
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

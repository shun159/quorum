package quorum

import (
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/proc"
)

// operator swaps the member of a node whose disk was replaced, as people
// running a group do: it removes the member the node had, then adds the
// new one. It tries each step until it is done, which it may: removing a
// member gone, or adding one in, changes nothing.
type operator struct {
	genserver.Default[swapping]
	group   string
	members []string
}

// swapping is where an operator is in its swap of node, if any. One
// request is out at a time, or one timer to try again.
type swapping struct {
	node  string
	step  swapStep
	id    proc.PID // the new member, once identified
	ask   string   // the member to ask, for the leader
	swaps int      // the swaps done
}

type swapStep int

const (
	idle swapStep = iota
	removing
	identifying // the new member, for its ID
	adding
)

// swap has an operator swap the member of node.
type swap struct{ node string }

// retryStep is the timer of an operator to try its step again.
type retryStep struct{}

const operatorTimeout = time.Second

func (o operator) Init(proc.PID) (swapping, []actor.Effect, error) {
	return swapping{}, nil, nil
}

func (o operator) HandleCast(s swapping, req any) (swapping, []actor.Effect) {
	m, ok := req.(swap)
	if !ok || s.step != idle {
		return s, nil
	}
	s.node, s.step, s.ask = m.node, removing, o.members[0]
	return s, o.try(s)
}

func (o operator) HandleInfo(s swapping, msg any) (swapping, []actor.Effect) {
	switch m := msg.(type) {
	case retryStep:
		return s, o.try(s)
	case actor.Response:
		if s.step == identifying {
			return o.identified(s, m)
		}
		return o.changed(s, m)
	}
	return s, nil
}

// try tries the step the operator is at.
func (o operator) try(s swapping) []actor.Effect {
	switch s.step {
	case removing:
		return o.request(s.ask, removeMember{s.node})
	case identifying:
		return o.request(s.node, identify{})
	case adding:
		return o.request(s.ask, addMember{s.id})
	}
	return nil
}

// identified moves the operator on to adding the new member, once it
// knows who it is.
func (o operator) identified(s swapping, m actor.Response) (swapping, []actor.Effect) {
	id, ok := m.Value.(identity)
	if !ok {
		return s, actor.Do(retryLater())
	}
	s.id, s.step = id.ID, adding
	return s, o.try(s)
}

// changed moves the operator on by what the group answered a change: to
// the next step once done, else to the same again. A change that failed,
// applied or not, is tried again, which changes nothing if it was.
func (o operator) changed(s swapping, m actor.Response) (swapping, []actor.Effect) {
	ans := answerOf(o.members, s.ask, m.Value, m.Err, true)
	switch {
	case !ans.done:
		s.ask = ans.node
		if ans.pause {
			return s, actor.Do(retryLater())
		}
		return s, o.try(s)
	case ans.err != nil:
		return s, actor.Do(retryLater())
	case s.step == removing:
		s.step = identifying
		return s, o.try(s)
	}
	s.step, s.swaps = idle, s.swaps+1
	return s, nil
}

func (o operator) request(node string, req any) []actor.Effect {
	return actor.Do(actor.SendRequest{
		To:      actor.Remote{Node: node, Name: replicaName(o.group)},
		Req:     req,
		Timeout: operatorTimeout,
	})
}

func retryLater() actor.Effect {
	return actor.StartTimer{Key: retryStep{}, After: retryAfter, Msg: retryStep{}}
}

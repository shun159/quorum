package quorum

import (
	"cmp"
	"maps"
	"time"

	"github.com/shun159/molecule/actor"
)

// Client sends commands to a group from a behaviour, which must not block
// on Command: a command is a few effects, and what comes of it messages to
// the behaviour, which it hands to Handle. A client is a value, kept in
// the state of the behaviour; each method returns the client after.
//
// A client finds the leader as Command does, asking the members on nodes
// in turn, and keeps it: a command goes to the leader that applied the one
// before, without asking first. It may have many commands out at once,
// each under a tag of the behaviour's choosing; those out at once may be
// applied in any order.
type Client[Cmd, Rep any] struct {
	group   string
	nodes   []string
	timeout time.Duration
	leader  string          // the node of the leader that answered last, if any
	seq     uint64          // the last attempt made
	out     map[any]attempt // the commands out, by tag
}

// attempt is a command out: asked of the member on node, or waiting to
// ask it. seq tells what answers this attempt from what answers an
// earlier one, or a command cancelled.
type attempt struct {
	cmd   any
	read  bool // a read, see Read
	probe bool // asking who leads, before sending the command
	node  string
	seq   uint64
}

// request is what the attempt sends.
func (a attempt) request() any {
	switch {
	case a.probe:
		return whoLeads{}
	case a.read:
		return read{a.cmd}
	}
	return command{a.cmd}
}

// Result is what came of a command: its reply, or an error as Command
// returns it.
type Result[Rep any] struct {
	Tag any
	Rep Rep
	Err error
}

// The messages of a client to itself: the tag of a member's answer, and
// the timer to ask again, with its key. They hold the group, for a
// behaviour with clients of several groups.
type (
	answerTag struct {
		group string
		tag   any
		seq   uint64
	}
	retry struct {
		group string
		tag   any
		seq   uint64
	}
	retryKey struct {
		group string
		tag   any
	}
)

// NewClient returns a client of group, whose members are on nodes. A
// positive timeout bounds the wait for each member asked; a command whose
// member does not answer in time ends with context.DeadlineExceeded,
// applied or not. A read asks the next member, until one answers.
func NewClient[Cmd, Rep any](group string, nodes []string, timeout time.Duration) Client[Cmd, Rep] {
	return Client[Cmd, Rep]{group: group, nodes: nodes, timeout: timeout}
}

// Command sends cmd to the group under tag, which must be comparable: its
// Result comes of a later Handle. A command already out under tag is
// cancelled.
func (c Client[Cmd, Rep]) Command(tag any, cmd Cmd) (Client[Cmd, Rep], []actor.Effect) {
	if c.leader == "" {
		return c.send(tag, attempt{cmd: cmd, probe: true, node: c.nodes[0]})
	}
	return c.send(tag, attempt{cmd: cmd, node: c.leader})
}

// Read sends cmd to the group under tag, to answer as Read does: its
// Result comes of a later Handle, as for Command.
func (c Client[Cmd, Rep]) Read(tag any, cmd Cmd) (Client[Cmd, Rep], []actor.Effect) {
	return c.send(tag, attempt{cmd: cmd, read: true, node: cmp.Or(c.leader, c.nodes[0])})
}

// Cancel forgets the command under tag: no Result comes of it. It may
// still be applied.
func (c Client[Cmd, Rep]) Cancel(tag any) (Client[Cmd, Rep], []actor.Effect) {
	c.out = maps.Clone(c.out)
	delete(c.out, tag)
	return c, actor.Do(actor.CancelTimer{Key: retryKey{c.group, tag}})
}

// Out returns how many commands are out.
func (c Client[Cmd, Rep]) Out() int { return len(c.out) }

// Handle takes msg, if it is the client's: a member's answer, or the time
// to ask again. It returns the effects of what the client does next and,
// once a command is done, its Result. Any other msg, it leaves alone.
func (c Client[Cmd, Rep]) Handle(msg any) (Client[Cmd, Rep], []actor.Effect, *Result[Rep]) {
	switch m := msg.(type) {
	case actor.Response:
		if t, ok := m.Tag.(answerTag); ok && t.group == c.group {
			return c.answered(t, m)
		}
	case retry:
		if a, ok := c.current(m.group, m.tag, m.seq); ok {
			c, effs := c.send(m.tag, a)
			return c, effs, nil
		}
	}
	return c, nil, nil
}

// answered handles a member's answer: the command is done, or to be asked
// again, at once or after a pause. A command goes to another member only
// once it says it leads.
func (c Client[Cmd, Rep]) answered(t answerTag, m actor.Response) (Client[Cmd, Rep], []actor.Effect, *Result[Rep]) {
	a, ok := c.current(t.group, t.tag, t.seq)
	if !ok {
		return c, nil, nil
	}
	if a.probe {
		c, effs := c.probed(t.tag, a, m)
		return c, effs, nil
	}
	ans := answerOf(c.nodes, a.node, m.Value, m.Err, a.read)
	if ans.done {
		c.out = maps.Clone(c.out)
		delete(c.out, t.tag)
		c.leader = ""
		if ans.err == nil {
			c.leader = a.node
		}
		return c, nil, c.result(t.tag, ans)
	}
	if ans.node != a.node {
		c.leader, a.probe = "", !a.read
	}
	a.node = ans.node
	c, effs := c.next(t.tag, a, ans.pause)
	return c, effs, nil
}

// probed handles a member's answer to who leads: the command goes to it,
// if it leads, or the client asks another.
func (c Client[Cmd, Rep]) probed(tag any, a attempt, m actor.Response) (Client[Cmd, Rep], []actor.Effect) {
	next, leads, wait := probed(c.nodes, a.node, m.Value, m.Err)
	if leads {
		c.leader, a.probe = a.node, false
	}
	a.node = next
	return c.next(tag, a, wait)
}

// next sends a, after a pause if wait.
func (c Client[Cmd, Rep]) next(tag any, a attempt, wait bool) (Client[Cmd, Rep], []actor.Effect) {
	if wait {
		return c.wait(tag, a)
	}
	return c.send(tag, a)
}

// current returns the command out under tag, if seq is its last attempt.
func (c Client[Cmd, Rep]) current(group string, tag any, seq uint64) (attempt, bool) {
	a, ok := c.out[tag]
	return a, ok && group == c.group && a.seq == seq
}

// send sends the request of a to the member on a.node.
func (c Client[Cmd, Rep]) send(tag any, a attempt) (Client[Cmd, Rep], []actor.Effect) {
	c, a = c.track(tag, a)
	return c, actor.Do(actor.SendRequest{
		To:      actor.Remote{Node: a.node, Name: replicaName(c.group)},
		Req:     a.request(),
		Tag:     answerTag{c.group, tag, a.seq},
		Timeout: c.timeout,
	})
}

// wait has the client send a again in a little while.
func (c Client[Cmd, Rep]) wait(tag any, a attempt) (Client[Cmd, Rep], []actor.Effect) {
	c, a = c.track(tag, a)
	return c, actor.Do(actor.StartTimer{
		Key:   retryKey{c.group, tag},
		After: retryAfter,
		Msg:   retry{c.group, tag, a.seq},
	})
}

// track makes a the last attempt of the command under tag.
func (c Client[Cmd, Rep]) track(tag any, a attempt) (Client[Cmd, Rep], attempt) {
	c.seq++
	a.seq = c.seq
	c.out = maps.Clone(c.out)
	if c.out == nil {
		c.out = map[any]attempt{}
	}
	c.out[tag] = a
	return c, a
}

func (c Client[Cmd, Rep]) result(tag any, ans answer) *Result[Rep] {
	res := &Result[Rep]{Tag: tag, Err: ans.err}
	if ans.err == nil {
		res.Rep, _ = ans.v.(applied).Rep.(Rep)
	}
	return res
}

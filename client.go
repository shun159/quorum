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
// A client opens a session with the group, see Sessions, in which each of
// its commands is applied once, however many times it is sent. So it sends
// a command again until it is applied: past members down or silent, and
// leaders that lose their place. A command ends with an error only if it
// is refused, or its session expired after it was sent.
//
// A client finds the leader asking the members on nodes in turn, and
// keeps it for the next command. It may have many commands out at once,
// each under a tag of the behaviour's choosing; those out at once may be
// applied in any order.
type Client[Cmd, Rep any] struct {
	group   string
	nodes   []string
	timeout time.Duration
	leader  string          // the node of the leader that answered last, if any
	session uint64          // the session, once open
	seq     uint64          // the last command numbered in it
	tries   uint64          // the last attempt made
	out     map[any]attempt // the commands out, by tag, and the opening of the session
}

// attempt is a request out: asked of the member on node, or waiting to
// ask it. try tells what answers this attempt from what answers an
// earlier one, or a request cancelled.
type attempt struct {
	cmd     any
	read    bool   // a read, see Read
	open    bool   // the opening of the session
	seq     uint64 // of the command in the session
	session uint64 // the session it was sent in, and how many times
	sent    int
	held    bool // until the session is open
	node    string
	try     uint64
}

// opening is the tag of the opening of the session among the requests.
type opening struct{}

// request is what the attempt sends, the client having the replies to its
// commands up to acked.
func (a attempt) request(acked uint64) any {
	switch {
	case a.open:
		return openSession{}
	case a.read:
		return read{a.cmd}
	}
	return command{Cmd: a.cmd, From: Origin{Session: a.session, Seq: a.seq, Acked: acked}}
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
		try   uint64
	}
	retry struct {
		group string
		tag   any
		try   uint64
	}
	retryKey struct {
		group string
		tag   any
	}
)

// NewClient returns a client of group, whose members are on nodes. A
// positive timeout bounds the wait for each member asked: one silent past
// it is taken for down.
func NewClient[Cmd, Rep any](group string, nodes []string, timeout time.Duration) Client[Cmd, Rep] {
	return Client[Cmd, Rep]{group: group, nodes: nodes, timeout: timeout}
}

// Command sends cmd to the group under tag, which must be comparable: its
// Result comes of a later Handle. A command already out under tag is
// cancelled. The first command opens the session, which the others wait
// for.
func (c Client[Cmd, Rep]) Command(tag any, cmd Cmd) (Client[Cmd, Rep], []actor.Effect) {
	c.seq++
	return c.inSession(tag, attempt{cmd: cmd, seq: c.seq})
}

// Read sends cmd to the group under tag, to answer as Read does: its
// Result comes of a later Handle, as for Command.
func (c Client[Cmd, Rep]) Read(tag any, cmd Cmd) (Client[Cmd, Rep], []actor.Effect) {
	return c.send(tag, attempt{cmd: cmd, read: true})
}

// Cancel forgets the command under tag: no Result comes of it. It may
// still be applied.
func (c Client[Cmd, Rep]) Cancel(tag any) (Client[Cmd, Rep], []actor.Effect) {
	c.out = maps.Clone(c.out)
	delete(c.out, tag)
	return c, actor.Do(actor.CancelTimer{Key: retryKey{c.group, tag}})
}

// Out returns how many commands are out.
func (c Client[Cmd, Rep]) Out() int {
	if _, ok := c.out[opening{}]; ok {
		return len(c.out) - 1
	}
	return len(c.out)
}

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
		if a, ok := c.current(m.group, m.tag, m.try); ok {
			c, effs := c.send(m.tag, a)
			return c, effs, nil
		}
	}
	return c, nil, nil
}

// inSession sends the command a in the session, once it is open.
func (c Client[Cmd, Rep]) inSession(tag any, a attempt) (Client[Cmd, Rep], []actor.Effect) {
	if c.session != 0 {
		return c.send(tag, a)
	}
	a.held = true
	c, _ = c.track(tag, a)
	if _, ok := c.out[opening{}]; ok {
		return c, nil
	}
	return c.send(opening{}, attempt{open: true})
}

// answered handles a member's answer: the request is done, or to be asked
// again, at once or after a pause.
func (c Client[Cmd, Rep]) answered(t answerTag, m actor.Response) (Client[Cmd, Rep], []actor.Effect, *Result[Rep]) {
	a, ok := c.current(t.group, t.tag, t.try)
	if !ok {
		return c, nil, nil
	}
	ans := answerOf(c.nodes, a.node, m.Value, m.Err, true)
	switch {
	case !ans.done:
		a.node = ans.node
		c, effs := c.next(t.tag, a, ans.pause)
		return c, effs, nil
	case a.open:
		c, effs := c.opened(a, ans)
		return c, effs, nil
	case ans.err == ErrSessionExpired && a.sent == 1:
		// Sent once, in a session that had expired: never applied.
		c.session, a.sent = 0, 0
		c, effs := c.inSession(t.tag, a)
		return c, effs, nil
	}
	c.out = maps.Clone(c.out)
	delete(c.out, t.tag)
	if ans.err == nil {
		c.leader = a.node
	}
	return c, nil, c.result(t.tag, ans)
}

// opened takes the session the group opened, and sends the commands that
// waited for it. Refused, the client asks again in a while.
func (c Client[Cmd, Rep]) opened(a attempt, ans answer) (Client[Cmd, Rep], []actor.Effect) {
	if ans.err != nil {
		return c.wait(opening{}, a)
	}
	c.session, c.leader = ans.v.(applied).Rep.(uint64), a.node
	c.out = maps.Clone(c.out)
	delete(c.out, opening{})
	var effs []actor.Effect
	for tag, held := range c.out {
		if held.held {
			held.held = false
			var more []actor.Effect
			c, more = c.send(tag, held)
			effs = append(effs, more...)
		}
	}
	return c, effs
}

// next sends a, after a pause if wait.
func (c Client[Cmd, Rep]) next(tag any, a attempt, wait bool) (Client[Cmd, Rep], []actor.Effect) {
	if wait {
		return c.wait(tag, a)
	}
	return c.send(tag, a)
}

// current returns the request out under tag, if try is its last attempt.
func (c Client[Cmd, Rep]) current(group string, tag any, try uint64) (attempt, bool) {
	a, ok := c.out[tag]
	return a, ok && group == c.group && a.try == try && !a.held
}

// send sends the request of a to the member on a.node, or else to the
// leader known, or else to the first member.
func (c Client[Cmd, Rep]) send(tag any, a attempt) (Client[Cmd, Rep], []actor.Effect) {
	a.node = cmp.Or(a.node, c.leader, c.nodes[0])
	if !a.read && !a.open && a.session != c.session {
		a.session, a.sent = c.session, 0
	}
	a.sent++
	c, a = c.track(tag, a)
	return c, actor.Do(actor.SendRequest{
		To:      actor.Remote{Node: a.node, Name: replicaName(c.group)},
		Req:     a.request(c.acked()),
		Tag:     answerTag{c.group, tag, a.try},
		Timeout: c.timeout,
	})
}

// acked returns the number up to which the client has the replies to its
// commands: below the first still out.
func (c Client[Cmd, Rep]) acked() uint64 {
	acked := c.seq
	for _, a := range c.out {
		if a.seq > 0 && a.seq <= acked {
			acked = a.seq - 1
		}
	}
	return acked
}

// wait has the client send a again in a little while.
func (c Client[Cmd, Rep]) wait(tag any, a attempt) (Client[Cmd, Rep], []actor.Effect) {
	c, a = c.track(tag, a)
	return c, actor.Do(actor.StartTimer{
		Key:   retryKey{c.group, tag},
		After: retryAfter,
		Msg:   retry{c.group, tag, a.try},
	})
}

// track makes a the last attempt of the request under tag.
func (c Client[Cmd, Rep]) track(tag any, a attempt) (Client[Cmd, Rep], attempt) {
	c.tries++
	a.try = c.tries
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

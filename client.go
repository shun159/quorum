package quorum

import (
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/quorum/internal/raft"
)

// Client sends commands to a group from a behaviour, which must not block
// on Command: a command is a few effects, and what comes of it messages to
// the behaviour, which it hands to Handle. A client is a value, kept in
// the state of the behaviour; each method returns the client after.
//
// A client opens a session with the group, in which each of its commands
// is applied once, however many times it is sent. So it sends a command
// again until it is applied: past members down or silent, and leaders
// that lose their place. A command ends with an error only if it is
// refused, or its session expired after it was sent, see
// ErrSessionExpired.
//
// A client finds the leader asking the members on nodes in turn, and
// keeps it for the next command. It may have many commands out at once,
// each under a tag of the behaviour's choosing; those out at once may be
// applied in any order.
type Client[Cmd, Rep any] struct {
	c raft.Client[Cmd, Rep]
}

// Result is what came of a command: its reply, or an error as Command
// returns it.
type Result[Rep any] struct {
	Tag any
	Rep Rep
	Err error
}

// NewClient returns a client of group, whose members are on nodes. A
// positive timeout bounds the wait for each member asked: one silent past
// it is taken for down.
func NewClient[Cmd, Rep any](group string, nodes []string, timeout time.Duration) Client[Cmd, Rep] {
	return Client[Cmd, Rep]{raft.NewClient[Cmd, Rep](group, nodes, timeout)}
}

// Command sends cmd to the group under tag, which must be comparable: its
// Result comes of a later Handle. A command already out under tag is
// cancelled. The first command opens the session, which the others wait
// for.
func (c Client[Cmd, Rep]) Command(tag any, cmd Cmd) (Client[Cmd, Rep], []actor.Effect) {
	var effs []actor.Effect
	c.c, effs = c.c.Command(tag, cmd)
	return c, effs
}

// Read sends cmd to the group under tag, to answer as Read does: its
// Result comes of a later Handle, as for Command.
func (c Client[Cmd, Rep]) Read(tag any, cmd Cmd) (Client[Cmd, Rep], []actor.Effect) {
	var effs []actor.Effect
	c.c, effs = c.c.Read(tag, cmd)
	return c, effs
}

// Cancel forgets the command under tag: no Result comes of it. It may
// still be applied.
func (c Client[Cmd, Rep]) Cancel(tag any) (Client[Cmd, Rep], []actor.Effect) {
	var effs []actor.Effect
	c.c, effs = c.c.Cancel(tag)
	return c, effs
}

// Out returns how many commands are out.
func (c Client[Cmd, Rep]) Out() int { return c.c.Out() }

// Handle takes msg, if it is the client's: a member's answer, or the time
// to ask again. It returns the effects of what the client does next and,
// once a command is done, its Result. Any other msg, it leaves alone.
func (c Client[Cmd, Rep]) Handle(msg any) (Client[Cmd, Rep], []actor.Effect, *Result[Rep]) {
	var effs []actor.Effect
	var r *raft.Result[Rep]
	c.c, effs, r = c.c.Handle(msg)
	if r == nil {
		return c, effs, nil
	}
	return c, effs, &Result[Rep]{Tag: r.Tag, Rep: r.Rep, Err: r.Err}
}

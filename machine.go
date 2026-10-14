package quorum

import (
	"time"

	"github.com/shun159/molecule/actor"
)

// Machine is the state machine a group replicates: see the package
// documentation for what it asks of one. Its state, commands and replies
// go between nodes and to disk: their types of their own are registered
// with dist.Register.
type Machine[S, Cmd, Rep any] interface {
	// Init returns the state before any command.
	Init() S
	// Apply applies cmd to s, and returns the next state and the reply to
	// cmd. Every member calls it with the same commands in the same order:
	// it must depend on nothing else, neither the clock nor randomness.
	// The effects are performed by the leader only.
	Apply(s S, cmd Cmd) (S, Rep, []actor.Effect)
}

// Reader is implemented by a Machine that answers reads: commands that
// change nothing, which the leader answers from its state, without them
// going through the log. See Read, and TimedReader.
type Reader[S, Cmd, Rep any] interface {
	// Read returns the reply to cmd in s.
	Read(s S, cmd Cmd) Rep
}

// Timed is implemented by a Machine whose commands depend on time, as
// leases and TTLs do: ApplyAt is called in place of Apply, with the time
// of the command. That time is the same on every member: the clock of the
// leader as it took the command, which only goes forward, from one leader
// to the next. Nothing happens at a time by itself: a lease expired is
// seen so by the next command, or read.
type Timed[S, Cmd, Rep any] interface {
	ApplyAt(s S, cmd Cmd, at time.Time) (S, Rep, []actor.Effect)
}

// TimedReader is implemented by a Machine whose reads depend on time:
// ReadAt is called in place of Read, at the time of the leader's clock,
// never before the time of the last command applied.
type TimedReader[S, Cmd, Rep any] interface {
	ReadAt(s S, cmd Cmd, at time.Time) Rep
}

// Leader is implemented by a Machine that acts on becoming the leader of
// its group, and on ceasing to be.
type Leader[S any] interface {
	// Elected is called when the member becomes the leader of term. A
	// member deposed may not know it at once: what it does outside the
	// group should carry term, for the outside to refuse an older one.
	Elected(s S, term uint64) []actor.Effect
	// Surrendered is called when the member stops being the leader.
	Surrendered(s S) []actor.Effect
}

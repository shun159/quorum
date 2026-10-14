package quorum

import "github.com/shun159/molecule/actor"

// Machine is the state machine a group replicates.
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
// going through the log. See Read.
type Reader[S, Cmd, Rep any] interface {
	// Read returns the reply to cmd in s.
	Read(s S, cmd Cmd) Rep
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

package raft

import "github.com/shun159/molecule/actor"

// Machine is quorum.Machine.
type Machine[S, Cmd, Rep any] interface {
	Init() S
	Apply(s S, cmd Cmd) (S, Rep, []actor.Effect)
}

// Reader is quorum.Reader.
type Reader[S, Cmd, Rep any] interface {
	Read(s S, cmd Cmd) Rep
}

// Leader is quorum.Leader.
type Leader[S any] interface {
	Elected(s S, term uint64) []actor.Effect
	Surrendered(s S) []actor.Effect
}

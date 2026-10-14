package raft

import (
	"time"

	"github.com/shun159/molecule/actor"
)

// Machine is quorum.Machine.
type Machine[S, Cmd, Rep any] interface {
	Init() S
	Apply(s S, cmd Cmd) (S, Rep, []actor.Effect)
}

// Reader is quorum.Reader.
type Reader[S, Cmd, Rep any] interface {
	Read(s S, cmd Cmd) Rep
}

// Timed is quorum.Timed.
type Timed[S, Cmd, Rep any] interface {
	ApplyAt(s S, cmd Cmd, at time.Time) (S, Rep, []actor.Effect)
}

// TimedReader is quorum.TimedReader.
type TimedReader[S, Cmd, Rep any] interface {
	ReadAt(s S, cmd Cmd, at time.Time) Rep
}

// Leader is quorum.Leader.
type Leader[S any] interface {
	Elected(s S, term uint64) []actor.Effect
	Surrendered(s S) []actor.Effect
}

// applyMachine applies cmd to s, at the time of the log, if the machine is
// Timed.
func (b behaviour[S, Cmd, Rep]) applyMachine(s S, cmd Cmd, at int64) (S, Rep, []actor.Effect) {
	if m, ok := b.spec.Machine.(Timed[S, Cmd, Rep]); ok {
		return m.ApplyAt(s, cmd, time.Unix(0, at))
	}
	return b.spec.Machine.Apply(s, cmd)
}

// readMachine answers cmd in s, at the time of the leader, if the
// machine is a TimedReader: its clock, but not before the time of the log.
func (b behaviour[S, Cmd, Rep]) readMachine(d *replica[S], cmd Cmd) Rep {
	if m, ok := b.spec.Machine.(TimedReader[S, Cmd, Rep]); ok {
		at := d.sessions.Time
		if b.now != nil {
			at = max(at, b.now().UnixNano())
		}
		return m.ReadAt(d.state, cmd, time.Unix(0, at))
	}
	return b.spec.Machine.(Reader[S, Cmd, Rep]).Read(d.state, cmd)
}

// reads reports whether the machine answers reads.
func (b behaviour[S, Cmd, Rep]) reads() bool {
	_, reader := b.spec.Machine.(Reader[S, Cmd, Rep])
	_, timed := b.spec.Machine.(TimedReader[S, Cmd, Rep])
	return reader || timed
}

package raft

import (
	"errors"
	"fmt"
	"time"
)

// Spec is quorum.Spec, its fields as there.
type Spec[S, Cmd, Rep any] struct {
	Group           string
	Machine         Machine[S, Cmd, Rep]
	ElectionTimeout time.Duration
	Heartbeat       time.Duration
	SnapshotEvery   uint64
	SessionTimeout  time.Duration
	Quiet           time.Duration // negative: never
	Storage         Storage       // nil keeps all in memory

	chunk int // the size of the chunks of a snapshot sent, for tests
}

// DefaultElectionTimeout is Spec.ElectionTimeout when zero.
const DefaultElectionTimeout = 300 * time.Millisecond

// DefaultSnapshotEvery is Spec.SnapshotEvery when zero.
const DefaultSnapshotEvery = 10000

// DefaultSessionTimeout is Spec.SessionTimeout when zero.
const DefaultSessionTimeout = time.Minute

// replicaName is the name of the member of group on each node.
func replicaName(group string) string { return "quorum/" + group }

// withDefaults returns spec with its zero durations set.
func (spec Spec[S, Cmd, Rep]) withDefaults() Spec[S, Cmd, Rep] {
	if spec.ElectionTimeout == 0 {
		spec.ElectionTimeout = DefaultElectionTimeout
	}
	if spec.Heartbeat == 0 {
		spec.Heartbeat = spec.ElectionTimeout / 6
	}
	if spec.SnapshotEvery == 0 {
		spec.SnapshotEvery = DefaultSnapshotEvery
	}
	if spec.SessionTimeout == 0 {
		spec.SessionTimeout = DefaultSessionTimeout
	}
	if spec.chunk == 0 {
		spec.chunk = snapshotChunk
	}
	if spec.Quiet == 0 {
		spec.Quiet = spec.ElectionTimeout
	}
	return spec
}

// validate reports what is wrong with spec, once with its defaults.
func (spec Spec[S, Cmd, Rep]) validate() error {
	switch {
	case spec.Group == "":
		return errors.New("quorum: a group needs a name")
	case spec.Machine == nil:
		return fmt.Errorf("quorum: group %s has no machine", spec.Group)
	case spec.Heartbeat >= spec.ElectionTimeout:
		return fmt.Errorf("quorum: group %s beats every %v, not under its election timeout %v",
			spec.Group, spec.Heartbeat, spec.ElectionTimeout)
	}
	return nil
}

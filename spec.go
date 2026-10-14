package quorum

import (
	"time"

	"github.com/shun159/quorum/internal/raft"
)

// Spec says what the member of a group on a node is: the group, the
// machine it replicates, and where it keeps what it must not forget. Who
// the members are is in the log of the group, see Found.
type Spec[S, Cmd, Rep any] struct {
	// Group names the group. Its member on each node is registered under
	// "quorum/" followed by it.
	Group   string
	Machine Machine[S, Cmd, Rep]
	// ElectionTimeout is how long a follower waits to hear from a leader
	// before it stands for election, plus up to as much again, which
	// differs between the members so that one of them stands first.
	// Zero means DefaultElectionTimeout.
	ElectionTimeout time.Duration
	// Heartbeat is how often a leader tells the others it lives, well
	// under ElectionTimeout. Zero means a sixth of ElectionTimeout.
	Heartbeat time.Duration
	// SnapshotEvery is how many entries applied make a member take a
	// snapshot of its machine, in their place: zero means
	// DefaultSnapshotEvery.
	SnapshotEvery uint64
	// SessionTimeout is how long, by the time of the log, a session of a
	// Client lasts without a command: zero means DefaultSessionTimeout.
	SessionTimeout time.Duration
	// Storage keeps who the member is, its term, its vote and its log
	// across restarts, each member its own. Without one, the member keeps
	// them in memory only: restarted, it is another member, which the
	// group must add.
	Storage raft.Storage
}

// DefaultElectionTimeout is Spec.ElectionTimeout when zero.
const DefaultElectionTimeout = raft.DefaultElectionTimeout

// DefaultSnapshotEvery is Spec.SnapshotEvery when zero.
const DefaultSnapshotEvery = raft.DefaultSnapshotEvery

// DefaultSessionTimeout is Spec.SessionTimeout when zero.
const DefaultSessionTimeout = raft.DefaultSessionTimeout

// raft returns spec as the member takes it.
func (spec Spec[S, Cmd, Rep]) raft() raft.Spec[S, Cmd, Rep] {
	return raft.Spec[S, Cmd, Rep]{
		Group:           spec.Group,
		Machine:         spec.Machine,
		ElectionTimeout: spec.ElectionTimeout,
		Heartbeat:       spec.Heartbeat,
		SnapshotEvery:   spec.SnapshotEvery,
		SessionTimeout:  spec.SessionTimeout,
		Storage:         spec.Storage,
	}
}

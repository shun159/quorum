package quorum

import (
	"cmp"
	"time"

	"github.com/shun159/quorum/internal/kvsstorage"
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
	// Dir is the directory of the Storage the member keeps who it is and
	// its log in, across restarts. Empty, the member keeps them in memory
	// only: restarted, it is another member, which the group must add.
	Dir string
}

// DefaultElectionTimeout is Spec.ElectionTimeout when zero.
const DefaultElectionTimeout = 300 * time.Millisecond

// DefaultSnapshotEvery is Spec.SnapshotEvery when zero.
const DefaultSnapshotEvery = 10000

// DefaultSessionTimeout is Spec.SessionTimeout when zero.
const DefaultSessionTimeout = time.Minute

// raft returns spec as the member takes it, its zeros the defaults.
func (spec Spec[S, Cmd, Rep]) raft() raft.Spec[S, Cmd, Rep] {
	return raft.Spec[S, Cmd, Rep]{
		Group:           spec.Group,
		Machine:         spec.Machine,
		ElectionTimeout: cmp.Or(spec.ElectionTimeout, DefaultElectionTimeout),
		Heartbeat:       spec.Heartbeat, // a sixth of ElectionTimeout, if zero
		SnapshotEvery:   cmp.Or(spec.SnapshotEvery, DefaultSnapshotEvery),
		SessionTimeout:  cmp.Or(spec.SessionTimeout, DefaultSessionTimeout),
		Storage:         spec.storage(),
	}
}

func (spec Spec[S, Cmd, Rep]) storage() raft.Storage {
	if spec.Dir == "" {
		return nil
	}
	return kvsstorage.In(spec.Dir, spec.Group)
}

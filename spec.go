package quorum

import (
	"errors"
	"fmt"
	"time"
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
	// Storage keeps who the member is, its term, its vote and its log
	// across restarts, each member its own. Without one, the member keeps
	// them in memory only: restarted, it is another member, which the
	// group must add.
	Storage Storage
}

// DefaultElectionTimeout is Spec.ElectionTimeout when zero.
const DefaultElectionTimeout = 300 * time.Millisecond

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

package quorum

import (
	"errors"
	"fmt"
	"slices"
	"time"
)

// Spec says what a group is: its name, its members, and the machine it
// replicates.
type Spec[S, Cmd, Rep any] struct {
	// Group names the group. Its member on each node is registered under
	// "quorum/" followed by it.
	Group string
	// Members are the names of the nodes of the group, one member on each.
	Members []string
	Machine Machine[S, Cmd, Rep]
	// ElectionTimeout is how long a follower waits to hear from a leader
	// before it stands for election, plus up to as much again, which
	// differs between the members so that one of them stands first.
	// Zero means DefaultElectionTimeout.
	ElectionTimeout time.Duration
	// Heartbeat is how often a leader tells the others it lives, well
	// under ElectionTimeout. Zero means a sixth of ElectionTimeout.
	Heartbeat time.Duration
}

// DefaultElectionTimeout is Spec.ElectionTimeout when zero.
const DefaultElectionTimeout = 300 * time.Millisecond

// replicaName is the name of the member of group on each node.
func replicaName(group string) string { return "quorum/" + group }

// majority is how many members make a majority of the group.
func (spec Spec[S, Cmd, Rep]) majority() int { return len(spec.Members)/2 + 1 }

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
	case len(spec.Members) == 0:
		return fmt.Errorf("quorum: group %s has no members", spec.Group)
	case hasDuplicate(spec.Members):
		return fmt.Errorf("quorum: group %s has a member twice: %v", spec.Group, spec.Members)
	case spec.Machine == nil:
		return fmt.Errorf("quorum: group %s has no machine", spec.Group)
	case spec.Heartbeat >= spec.ElectionTimeout:
		return fmt.Errorf("quorum: group %s beats every %v, not under its election timeout %v",
			spec.Group, spec.Heartbeat, spec.ElectionTimeout)
	}
	return nil
}

func hasDuplicate(names []string) bool {
	sorted := slices.Sorted(slices.Values(names))
	return len(slices.Compact(sorted)) != len(names)
}

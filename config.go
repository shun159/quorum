package quorum

import (
	"slices"

	"github.com/shun159/molecule/proc"
)

// Config is who the members of a group are: the voters, a majority of
// which elects the leader and commits, and the learners, which follow the
// log without voting, catching up before they vote.
//
// A member is known by the PID it first had on its storage, blank: it
// keeps it restarted with its storage, and gets another on a new disk, a
// member anew, which the group must add.
type Config struct {
	Voters   []proc.PID
	Learners []proc.PID
}

func (c Config) isVoter(id proc.PID) bool { return slices.Contains(c.Voters, id) }

func (c Config) has(id proc.PID) bool {
	return c.isVoter(id) || slices.Contains(c.Learners, id)
}

// majority is how many voters make a majority.
func (c Config) majority() int { return len(c.Voters)/2 + 1 }

// members returns the voters, then the learners.
func (c Config) members() []proc.PID {
	return append(slices.Clone(c.Voters), c.Learners...)
}

// on returns the member on node, if any.
func (c Config) on(node string) (proc.PID, bool) {
	for _, id := range c.members() {
		if id.Node() == node {
			return id, true
		}
	}
	return proc.PID{}, false
}

// withLearner returns c with id as a learner.
func (c Config) withLearner(id proc.PID) Config {
	return Config{Voters: slices.Clone(c.Voters), Learners: append(slices.Clone(c.Learners), id)}
}

// promoted returns c with the learner id a voter.
func (c Config) promoted(id proc.PID) Config {
	return Config{
		Voters:   append(slices.Clone(c.Voters), id),
		Learners: slices.DeleteFunc(slices.Clone(c.Learners), func(x proc.PID) bool { return x == id }),
	}
}

// without returns c without id.
func (c Config) without(id proc.PID) Config {
	other := func(x proc.PID) bool { return x == id }
	return Config{
		Voters:   slices.DeleteFunc(slices.Clone(c.Voters), other),
		Learners: slices.DeleteFunc(slices.Clone(c.Learners), other),
	}
}

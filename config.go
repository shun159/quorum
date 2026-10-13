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

// config returns the latest configuration in the log, which is in force
// once appended, committed or not; the empty one, before the group is
// founded.
func (l raftLog) config() Config {
	for i := len(l) - 1; i >= 0; i-- {
		if l[i].Config != nil {
			return *l[i].Config
		}
	}
	return Config{}
}

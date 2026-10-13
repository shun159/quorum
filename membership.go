package quorum

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genstatem"
)

// Found founds group with its members on nodes, all voters, each started
// on a blank storage: their configuration becomes the first entry of their
// logs, the same in all. A group is founded once: a member that is in
// another group, or in this one founded otherwise, refuses, and Found
// fails. Should Found fail midway, it can be run again with the same
// nodes, the members started anew if any was.
func Found(ctx context.Context, caller actor.Caller, group string, nodes []string) error {
	var config Config
	for _, n := range nodes {
		v, err := actor.Call(ctx, caller, actor.Remote{Node: n, Name: replicaName(group)}, identify{})
		if err != nil {
			return fmt.Errorf("quorum: founding group %s, on %s: %w", group, n, err)
		}
		config.Voters = append(config.Voters, v.(identity).ID)
	}
	for _, n := range nodes {
		v, err := actor.Call(ctx, caller, actor.Remote{Node: n, Name: replicaName(group)}, found{config})
		if err != nil {
			return fmt.Errorf("quorum: founding group %s, on %s: %w", group, n, err)
		}
		if r, ok := v.(rejected); ok {
			return errors.New(r.Reason)
		}
	}
	return nil
}

// found founds the group with config, which the member is in: config is
// the first entry of its log, of term 0. A member with another log
// refuses; one founded with config already says so again.
func (b behaviour[S, Cmd, Rep]) found(r role, d replica[S], c genstatem.Call, m found) (role, replica[S], []actor.Effect) {
	switch {
	case d.foundedWith(m.Config):
		return r, d, d.after(c.Reply(applied{}))
	case d.log.lastIndex() != 0:
		return r, d, actor.Do(c.Reply(rejected{fmt.Sprintf("quorum: group %s is founded already", b.spec.Group)}))
	case !m.Config.has(d.id):
		return r, d, actor.Do(c.Reply(rejected{fmt.Sprintf("quorum: %v is not in the configuration of group %s", d.id, b.spec.Group)}))
	}
	config := Config{Voters: slices.Clone(m.Config.Voters), Learners: slices.Clone(m.Config.Learners)}
	d.log, d.config = raftLog{{Term: 0, Config: &config}}, config
	effs := b.saveLog(&d, 1)
	return r, d, append(effs, d.after(c.Reply(applied{}))...)
}

// foundedWith reports whether the log is only the founding of the group
// with config.
func (d *replica[S]) foundedWith(config Config) bool {
	if d.log.lastIndex() != 1 || d.log[0].Config == nil {
		return false
	}
	first := *d.log[0].Config
	return slices.Equal(first.Voters, config.Voters) && slices.Equal(first.Learners, config.Learners)
}

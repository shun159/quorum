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
	d.log = logOf(Entry{Term: 0, Config: &config})
	d.logChanged()
	effs := b.saveLog(&d, 1)
	return r, d, append(effs, d.after(c.Reply(applied{}))...)
}

// foundedWith reports whether the log is only the founding of the group
// with config.
func (d *replica[S]) foundedWith(config Config) bool {
	if d.log.lastIndex() != 1 || d.log.at(1).Config == nil {
		return false
	}
	return sameConfig(*d.log.at(1).Config, config)
}

// AddMember adds the member on node to group, asking the members on nodes
// for the leader. The member joins as a learner, following the log without
// voting; once it has caught up, the leader makes it a voter. AddMember
// returns then, once the member counts toward a majority, which one cut
// off may take long to: ctx bounds the wait. Should the leader change, it
// asks the next, and waits on.
//
// The node must have no member in the group: one on a new disk, which
// replaces another on the node, is added once that one is removed. Disks
// swapped one node after another, each AddMember returning before the next
// node is taken down, the group keeps its majority throughout.
func AddMember(ctx context.Context, caller actor.Caller, group string, nodes []string, node string) error {
	v, err := actor.Call(ctx, caller, actor.Remote{Node: node, Name: replicaName(group)}, identify{})
	if err != nil {
		return fmt.Errorf("quorum: adding to group %s the member on %s: %w", group, node, err)
	}
	_, err = ask(ctx, caller, group, nodes, addMember{v.(identity).ID})
	return err
}

// RemoveMember removes the member on node from group, whoever it is,
// asking the members on nodes for the leader, and returns once the removal
// is committed. A leader removing itself steps down then.
func RemoveMember(ctx context.Context, caller actor.Caller, group string, nodes []string, node string) error {
	_, err := ask(ctx, caller, group, nodes, removeMember{node})
	return err
}

// change handles a change of the members. The leader appends the
// configuration it makes, one change at a time. It replies to a removal
// once committed, to an addition once the member is a voter.
func (b behaviour[S, Cmd, Rep]) change(r role, d replica[S], c genstatem.Call, m any) (role, replica[S], []actor.Effect) {
	if r != leader {
		return r, d, actor.Do(c.Reply(redirect{Leader: d.leader.Node()}))
	}
	if !d.mayChange() {
		return r, d, actor.Do(c.Reply(busy{}))
	}
	config, err := b.changed(d.config, m)
	add, adding := m.(addMember)
	switch {
	case err != nil:
		return r, d, actor.Do(c.Reply(rejected{err.Error()}))
	case adding:
		d.joining = append(d.joining, joining{id: add.ID, from: c.From})
	case sameConfig(config, d.config):
		return r, d, actor.Do(c.Reply(applied{}))
	default:
		d.pending = append(d.pending, pending{index: d.log.lastIndex() + 1, from: c.From})
	}
	if sameConfig(config, d.config) {
		return r, d, d.joined() // added already
	}
	return r, d, b.appendConfig(r, &d, config)
}

// joined replies to the additions of members now voters by the
// configuration committed, and of members removed since.
func (d *replica[S]) joined() []actor.Effect {
	committed := d.log.configUpTo(d.commit)
	var effs []actor.Effect
	var waiting []joining
	for _, j := range d.joining {
		switch {
		case committed.isVoter(j.id):
			effs = append(effs, actor.Reply{To: j.from, Value: applied{}})
		case !d.config.has(j.id):
			reason := fmt.Sprintf("quorum: %v was removed before it could vote", j.id)
			effs = append(effs, actor.Reply{To: j.from, Value: rejected{reason}})
		default:
			waiting = append(waiting, j)
		}
	}
	d.joining = waiting
	return effs
}

// mayChange reports whether the leader may change the members: once the
// latest configuration is committed, for one change at a time, and an
// entry of its own term, for it to know that configuration is the latest.
func (d *replica[S]) mayChange() bool {
	return d.configAt <= d.commit && d.log.termAt(d.commit) == d.term
}

// changed returns config changed as m asks.
func (b behaviour[S, Cmd, Rep]) changed(config Config, m any) (Config, error) {
	switch m := m.(type) {
	case addMember:
		if old, ok := config.on(m.ID.Node()); ok && old != m.ID {
			return config, fmt.Errorf("quorum: node %s has a member of group %s already; remove it first", m.ID.Node(), b.spec.Group)
		}
		if config.has(m.ID) {
			return config, nil
		}
		return config.withLearner(m.ID), nil
	case removeMember:
		id, ok := config.on(m.Node)
		if !ok {
			return config, nil
		}
		if config.isVoter(id) && len(config.Voters) == 1 {
			return config, fmt.Errorf("quorum: %s is the last voter of group %s", m.Node, b.spec.Group)
		}
		return config.without(id), nil
	}
	return config, nil
}

// promote makes a learner a voter, once it holds what is committed.
func (b behaviour[S, Cmd, Rep]) promote(r role, d *replica[S], p peer) []actor.Effect {
	if r != leader || !slices.Contains(d.config.Learners, p.id) || p.match < d.commit || !d.mayChange() {
		return nil
	}
	return b.appendConfig(r, d, d.config.promoted(p.id))
}

// appendConfig appends config, in force at once, and sends it to the
// members, those it adds included. Those it removes still get it, to
// learn they are out, until it is committed.
func (b behaviour[S, Cmd, Rep]) appendConfig(r role, d *replica[S], config Config) []actor.Effect {
	d.log = d.log.add(b.entry(d, Entry{Config: &config}))
	d.logChanged()
	for _, id := range d.others() {
		if d.peer(id) == nil {
			d.peers = append(d.peers, peer{id: id, next: d.log.lastIndex(), heardIn: d.round})
		}
	}
	effs := b.saveLog(d, d.log.lastIndex())
	effs = append(effs, b.replicate(*d)...)
	return append(effs, b.commit(r, d)...)
}

// prunePeers drops the peers out of the configuration, once committed.
func (d *replica[S]) prunePeers() {
	if d.configAt > d.commit {
		return
	}
	d.peers = slices.DeleteFunc(slices.Clone(d.peers), func(p peer) bool { return !d.config.has(p.id) })
}

func sameConfig(a, b Config) bool {
	return slices.Equal(a.Voters, b.Voters) && slices.Equal(a.Learners, b.Learners)
}

package quorum

import (
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"slices"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genstatem"
	"github.com/shun159/molecule/proc"
)

// role is the state of a member, as Raft names them.
type role int

const (
	follower role = iota
	candidate
	leader
)

func (r role) String() string {
	return [...]string{"follower", "candidate", "leader"}[r]
}

// replica is the data of a member of a group.
type replica[S any] struct {
	id  proc.PID // who the member is, see Config
	rng rand.PCG // draws its election timeouts

	term     uint64   // the latest term it knows
	votedFor proc.PID // in term, if it voted
	leader   proc.PID // of term, once known

	log      raftLog
	config   Config // the latest in the log
	configAt uint64 // its index
	commit   uint64 // the last index known committed
	applied  uint64 // the last index applied to state
	state    S      // of the machine

	votes   []proc.PID // granted to it, as a candidate
	peers   []peer     // the other members, as the leader
	pending []pending  // commands to reply to, as the leader, by index
	round   uint64     // of the checks of the quorum, as the leader

	seq    uint64 // of the last write to the storage
	saved  uint64 // seq of the last write known durable
	stable uint64 // the last index of the log known durable
	held   []held // what waits on writes to be told, by seq
}

// peer is what the leader knows of the log of another member.
type peer struct {
	id    proc.PID
	next  uint64 // the index of the next entry to send it
	match uint64 // the last index known to match the leader's

	heardIn uint64 // the round of the check the leader last heard from it in
}

// pending is a command the leader appended, to reply to once applied.
type pending struct {
	index uint64
	from  actor.From
}

// behaviour is a member of a group, a genstatem with state enter calls.
type behaviour[S, Cmd, Rep any] struct {
	spec Spec[S, Cmd, Rep]
}

func (behaviour[S, Cmd, Rep]) StateEnter() bool { return true }

// Init loads what the member saved. On a blank storage, the member is a
// new one, known by its PID, which it saves first.
func (b behaviour[S, Cmd, Rep]) Init(self proc.PID) (role, replica[S], []actor.Effect, error) {
	if err := b.spec.validate(); err != nil {
		return follower, replica[S]{}, nil, err
	}
	saved, err := b.load()
	if err != nil {
		return follower, replica[S]{}, nil, fmt.Errorf("quorum: group %s cannot load what it saved: %w", b.spec.Group, err)
	}
	d := replica[S]{
		id:       saved.State.Self,
		rng:      *rand.NewPCG(seed(b.spec.Group, self.String()), 0),
		state:    b.spec.Machine.Init(),
		term:     saved.State.Term,
		votedFor: saved.State.VotedFor,
		log:      raftLog(saved.Log),
	}
	d.logChanged()
	d.stable = d.log.lastIndex()
	if !d.id.IsZero() {
		return follower, d, nil, nil
	}
	d.id = self
	return follower, d, b.saveState(&d), nil
}

// seed seeds the election timeouts of the member of group in process
// self: the same in every run, different between the members.
func seed(group, self string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(group + "/" + self))
	return h.Sum64()
}

func (b behaviour[S, Cmd, Rep]) HandleEvent(r role, d replica[S], ev genstatem.Event) (role, replica[S], []actor.Effect) {
	r, d, effs := b.handleEvent(r, d, ev)
	if _, entering := ev.(genstatem.Enter[role]); !entering && r == leader && d.left() {
		return follower, d, effs
	}
	return r, d, effs
}

// left reports whether the member is out of the group, and that is
// committed: a leader that removed itself steps down then.
func (d *replica[S]) left() bool {
	return !d.config.has(d.id) && d.configAt <= d.commit
}

// logChanged keeps the configuration of the member that of its log.
func (d *replica[S]) logChanged() {
	d.config, d.configAt = d.log.config(), d.log.configAt()
}

func (b behaviour[S, Cmd, Rep]) handleEvent(r role, d replica[S], ev genstatem.Event) (role, replica[S], []actor.Effect) {
	switch e := ev.(type) {
	case genstatem.Enter[role]:
		return b.enter(r, e.Old, d)
	case genstatem.StateTimeout:
		return b.timeout(r, d)
	case genstatem.Timeout:
		if e.Name == (checkQuorum{}) {
			return b.checkQuorum(r, d)
		}
	case genstatem.Call:
		return b.call(r, d, e)
	case genstatem.Info:
		if tag, err, ok := b.isDurable(e.Msg); ok {
			return b.durable(r, d, tag, err)
		}
		return b.message(r, d, e.Msg)
	}
	return r, d, nil
}

// enter runs on every change of role, and on starting, as a follower.
func (b behaviour[S, Cmd, Rep]) enter(r, old role, d replica[S]) (role, replica[S], []actor.Effect) {
	var effs []actor.Effect
	if old == leader && r != leader {
		effs = b.surrender(&d)
	}
	switch r {
	case follower:
		effs = append(effs, d.electionTimer(b.spec.ElectionTimeout))
	case leader:
		effs = append(effs, b.lead(&d)...)
	}
	return r, d, effs
}

// timeout is the state timeout: a voter that heard from no leader stands
// for election; the leader beats. A member not voting waits on.
func (b behaviour[S, Cmd, Rep]) timeout(r role, d replica[S]) (role, replica[S], []actor.Effect) {
	switch {
	case r == leader:
		return r, d, b.heartbeat(d)
	case !d.config.isVoter(d.id):
		return r, d, actor.Do(d.electionTimer(b.spec.ElectionTimeout))
	}
	return b.stand(d)
}

// message handles a message from another member. One of a later term
// makes the member a follower in that term first, whatever it was, which
// it saves.
func (b behaviour[S, Cmd, Rep]) message(r role, d replica[S], msg any) (role, replica[S], []actor.Effect) {
	if m, ok := msg.(appendEntries); ok && m.To != d.id {
		return r, d, nil // for an earlier member on this node
	}
	var saving []actor.Effect
	if m, ok := msg.(termed); ok && m.term() > d.term {
		r = follower
		d.newTerm(m.term())
		saving = b.saveState(&d)
	}
	r, d, effs := b.handle(r, d, msg)
	return r, d, append(saving, effs...)
}

func (b behaviour[S, Cmd, Rep]) handle(r role, d replica[S], msg any) (role, replica[S], []actor.Effect) {
	switch m := msg.(type) {
	case requestVote:
		return b.requestVote(r, d, m)
	case voteReply:
		return b.voteReply(r, d, m)
	case appendEntries:
		return b.appendEntries(r, d, m)
	case appendReply:
		return b.appendReply(r, d, m)
	}
	return r, d, nil
}

// newTerm moves the member to a later term, in which it has not voted,
// and knows no leader yet.
func (d *replica[S]) newTerm(term uint64) {
	d.term, d.votedFor, d.leader = term, proc.PID{}, proc.PID{}
}

// others returns the members other than this one.
func (d *replica[S]) others() []proc.PID {
	return slices.DeleteFunc(d.config.members(), func(id proc.PID) bool { return id == d.id })
}

// send sends msg to the member id.
func (b behaviour[S, Cmd, Rep]) send(id proc.PID, msg any) actor.Effect {
	return actor.Send{To: actor.Remote{Node: id.Node(), Name: replicaName(b.spec.Group)}, Msg: msg}
}

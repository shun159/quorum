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
	self string   // the node of the member
	rng  rand.PCG // draws its election timeouts

	term     uint64 // the latest term it knows
	votedFor string // in term, if it voted
	leader   string // of term, once known

	log     raftLog
	commit  uint64 // the last index known committed
	applied uint64 // the last index applied to state
	state   S      // of the machine

	votes   []string  // granted to it, as a candidate
	peers   []peer    // the others, as the leader
	pending []pending // commands to reply to, as the leader, by index
	round   uint64    // of the checks of the quorum, as the leader

	seq    uint64 // of the last write to the storage
	saved  uint64 // seq of the last write known durable
	stable uint64 // the last index of the log known durable
	held   []held // what waits on writes to be told, by seq
}

// peer is what the leader knows of the log of another member.
type peer struct {
	name  string
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

func (b behaviour[S, Cmd, Rep]) Init(self proc.PID) (role, replica[S], []actor.Effect, error) {
	if err := b.spec.validate(); err != nil {
		return follower, replica[S]{}, nil, err
	}
	node := self.Node()
	if !slices.Contains(b.spec.Members, node) {
		return follower, replica[S]{}, nil, fmt.Errorf("quorum: node %s is not a member of group %s", node, b.spec.Group)
	}
	saved, err := b.load()
	if err != nil {
		return follower, replica[S]{}, nil, fmt.Errorf("quorum: group %s cannot load what it saved: %w", b.spec.Group, err)
	}
	d := replica[S]{
		self:     node,
		rng:      *rand.NewPCG(seed(b.spec.Group, node), 0),
		state:    b.spec.Machine.Init(),
		term:     saved.State.Term,
		votedFor: saved.State.VotedFor,
		log:      raftLog(saved.Log),
	}
	d.stable = d.log.lastIndex()
	return follower, d, nil, nil
}

// seed seeds the election timeouts of the member of group on node: the
// same in every run, different between the members.
func seed(group, node string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(group + "/" + node))
	return h.Sum64()
}

func (b behaviour[S, Cmd, Rep]) HandleEvent(r role, d replica[S], ev genstatem.Event) (role, replica[S], []actor.Effect) {
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

// timeout is the state timeout: a follower or a candidate that heard from
// no leader stands for election; the leader beats.
func (b behaviour[S, Cmd, Rep]) timeout(r role, d replica[S]) (role, replica[S], []actor.Effect) {
	if r == leader {
		return r, d, b.heartbeat(d)
	}
	return b.stand(d)
}

// message handles a message from another member. One of a later term
// makes the member a follower in that term first, whatever it was, which
// it saves.
func (b behaviour[S, Cmd, Rep]) message(r role, d replica[S], msg any) (role, replica[S], []actor.Effect) {
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
	d.term, d.votedFor, d.leader = term, "", ""
}

// others returns the members other than this one.
func (b behaviour[S, Cmd, Rep]) others(d replica[S]) []string {
	var others []string
	for _, m := range b.spec.Members {
		if m != d.self {
			others = append(others, m)
		}
	}
	return others
}

// send sends msg to the member of the group on node.
func (b behaviour[S, Cmd, Rep]) send(node string, msg any) actor.Effect {
	return actor.Send{To: actor.Remote{Node: node, Name: replicaName(b.spec.Group)}, Msg: msg}
}

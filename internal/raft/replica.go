package raft

import (
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"slices"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genstatem"
	"github.com/shun159/molecule/proc"
)

// role is the state of a member, as Raft names them.
type role int

const (
	follower     role = iota
	precandidate      // asking whether it would be voted for, see preStand
	candidate
	leader
)

func (r role) String() string {
	return [...]string{"follower", "precandidate", "candidate", "leader"}[r]
}

// replica is the data of a member of a group.
type replica[S any] struct {
	id  proc.PID // who the member is, see Config
	rng rand.PCG // draws its election timeouts

	*rare       // what it changes rarely, apart
	owned *rare // rare, if copied in this event: the member's to change

	term   uint64   // the latest term it knows
	leader proc.PID // of term, once known

	log      raftLog
	commit   uint64   // the last index known committed
	applied  uint64   // the last index applied to state
	state    S        // of the machine
	sessions Sessions // of the clients, as of applied

	peers   []peer    // the other members, as the leader
	pending []pending // commands to reply to, as the leader, by index
	reads   []reading // reads to answer, as the leader
	readSeq uint64    // its last round of confirming reads, as the leader
	timeAt  uint64    // the entry bringing the time of the log to its clock, for reads, as the leader

	compacting bool   // a snapshot taken to compact the log is being saved
	round      uint64 // of the checks of the quorum, as the leader

	transfer *transferring // the leader handing over, if it is

	quiet bool // the group is quiet, see Quiet
	idle  int  // the beats in a row with nothing to do, as the leader

	seq    uint64 // of the last write to the storage
	saved  uint64 // seq of the last write known durable
	stable uint64 // the last index of the log known durable
	held   []held // what waits on writes to be told, by seq
}

// peer is what the leader knows of the log of another member.
type peer struct {
	id   proc.PID
	next uint64 // the index of the next entry to send it
	// matched tells that its log matched the leader's at the last reply:
	// the leader sends it entries on from those on their way, not waiting
	// for its replies. Until then, or failing, it sends from next again.
	matched bool
	sent    bool   // entries since the last beat: it knows the leader lives
	match   uint64 // the last index known to match the leader's

	heardIn uint64  // the round of the check the leader last heard from it in
	read    uint64  // the last round of confirming reads it answered
	sending uint64  // the index of the snapshot it is sent, if not 0
	data    Encoded // that snapshot
	offset  int     // how much of it the member has
	chunkIn uint64  // the round of the check the leader last sent it a chunk in
}

// pending is a command the leader appended, to reply to once applied.
type pending struct {
	index uint64
	from  actor.From
}

// joining is a member added, by a client to reply to once it is a voter.
type joining struct {
	id   proc.PID
	from actor.From
}

// behaviour is a member of a group, a genstatem with state enter calls.
type behaviour[S, Cmd, Rep any] struct {
	spec Spec[S, Cmd, Rep]
	now  func() time.Time // the clock of the process, for the leader to stamp entries
}

func (behaviour[S, Cmd, Rep]) StateEnter() bool { return true }

// WithClock gives the member the clock of its process: see actor.Clocked.
func (b behaviour[S, Cmd, Rep]) WithClock(now func() time.Time) any {
	b.now = now
	return b
}

// entry returns e as the leader appends it, of its term, at its time.
func (b behaviour[S, Cmd, Rep]) entry(d *replica[S], e Entry) Entry {
	e.Term = d.term
	if b.now != nil {
		e.Time = b.now().UnixNano()
	}
	return e
}

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
		id:    saved.State.Self,
		rng:   *rand.NewPCG(seed(b.spec.Group, self.String()), 0),
		state: b.spec.Machine.Init(),
		term:  saved.State.Term,
		log:   logOf(saved.Log...),
		rare:  &rare{votedFor: saved.State.VotedFor},
	}
	if saved.Snapshot.Index > 0 {
		snap, err := b.decode(saved.Snapshot.Encoded)
		if err != nil {
			return follower, replica[S]{}, nil, fmt.Errorf("quorum: group %s cannot decode its snapshot: %w", b.spec.Group, err)
		}
		state, ok := snap.State.(S)
		if !ok {
			return follower, replica[S]{}, nil, fmt.Errorf("quorum: group %s saved a snapshot of a %T", b.spec.Group, snap.State)
		}
		d.rarely().snapshot = snap
		d.state, d.sessions = state, snap.Sessions
		d.log = raftLog{start: snap.Index, startTerm: snap.Term, startConfig: snap.Config, entries: saved.Log}
		d.commit, d.applied = snap.Index, snap.Index
	}
	d.logChanged(0)
	d.stable = d.log.lastIndex()
	effs := actor.Do(actor.MonitorNodes{On: true})
	if !d.id.IsZero() {
		return follower, d, effs, nil
	}
	d.id = self
	return follower, d, append(effs, b.saveState(&d)...), nil
}

// seed seeds the election timeouts of the member of group in process
// self: the same in every run, different between the members.
func seed(group, self string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(group + "/" + self))
	return h.Sum64()
}

// HandleEvent handles ev on d, its own copy of the replica: the handlers
// change it through a pointer, as copying it at every call costs.
func (b behaviour[S, Cmd, Rep]) HandleEvent(r role, d replica[S], ev genstatem.Event) (role, replica[S], []actor.Effect) {
	d.owned = nil // of the event before, shared with the replica given
	r, effs := b.handleEvent(r, &d, ev)
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

// logChanged keeps the configuration of the member that of its log,
// which changed from index from on: the latest of those entries, if one
// is a configuration, or the one in force still, unless it changed too.
// Not looking back through the log, a member gets an entry at the cost of
// it alone, however long its log.
func (d *replica[S]) logChanged(from uint64) {
	if from <= d.configAt {
		d.rarely().config, d.rarely().configAt = d.log.config() // the one in force changed: look back for it
		return
	}
	for i := d.log.lastIndex(); i >= from; i-- {
		if c := d.log.at(i).Config; c != nil {
			d.rarely().config, d.rarely().configAt = *c, i
			return
		}
	}
}

//abc:hot BenchmarkLeader,BenchmarkFollower
func (b behaviour[S, Cmd, Rep]) handleEvent(r role, d *replica[S], ev genstatem.Event) (role, []actor.Effect) {
	switch e := ev.(type) {
	case genstatem.Enter[role]:
		return b.enter(r, e.Old, d)
	case genstatem.StateTimeout:
		return b.timeout(r, d)
	case genstatem.Timeout:
		switch e.Name {
		case checkQuorum{}:
			return b.checkQuorum(r, d)
		case transferTimeout{}:
			return b.transferTimedOut(r, d)
		}
	case genstatem.Call:
		return b.call(r, d, e)
	case genstatem.Info:
		if tag, err, ok := b.isDurable(e.Msg); ok {
			return b.durable(r, d, tag, err)
		}
		if node, lost, ok := nodeLost(e.Msg); ok {
			return b.nodeEvent(r, d, node, lost)
		}
		if m, ok := e.Msg.(actor.Down); ok && m.Tag == (watchLeader{}) {
			return b.leaderDown(r, d, m)
		}
		return b.message(r, d, e.Msg)
	}
	return r, nil
}

// enter runs on every change of role, and on starting, as a follower.
func (b behaviour[S, Cmd, Rep]) enter(r, old role, d *replica[S]) (role, []actor.Effect) {
	var effs []actor.Effect
	if old == leader && r != leader {
		effs = b.surrender(d)
		d.quiet = false
	}
	switch r {
	case follower:
		effs = append(effs, d.electionTimer(b.spec.ElectionTimeout))
	case leader:
		effs = append(effs, b.lead(d)...)
	}
	return r, effs
}

// timeout is the state timeout: a voter that heard from no leader asks
// whether it would be elected; the leader beats. A member not voting
// waits on.
func (b behaviour[S, Cmd, Rep]) timeout(r role, d *replica[S]) (role, []actor.Effect) {
	switch {
	case r == leader:
		return r, b.beat(d)
	case !d.config.isVoter(d.id):
		d.leader = proc.PID{} // heard from none: it would vote, for those counting it a voter
		return r, actor.Do(d.electionTimer(b.spec.ElectionTimeout))
	}
	return b.preStand(d)
}

// message handles a message from another member. One of a later term
// makes the member a follower in that term first, whatever it was, which
// it saves.
func (b behaviour[S, Cmd, Rep]) message(r role, d *replica[S], msg any) (role, []actor.Effect) {
	if m, ok := msg.(addressed); ok && m.to() != d.id {
		return r, nil // for an earlier member on this node
	}
	var saving []actor.Effect
	if m, ok := msg.(termed); ok && m.term() > d.term {
		if r == follower && d.quiet {
			saving = d.unhush(d.electionTimer(b.spec.ElectionTimeout))
		}
		r = follower
		d.newTerm(m.term())
		saving = append(saving, b.saveState(d)...)
	}
	r, effs := b.handle(r, d, msg)
	return r, append(saving, effs...)
}

func (b behaviour[S, Cmd, Rep]) handle(r role, d *replica[S], msg any) (role, []actor.Effect) {
	switch m := msg.(type) {
	case preVote:
		return b.preVote(r, d, m)
	case preVoteReply:
		return b.preVoteReply(r, d, m)
	case requestVote:
		return b.requestVote(r, d, m)
	case voteReply:
		return b.voteReply(r, d, m)
	case appendEntries:
		return b.appendEntries(r, d, m)
	case appendReply:
		return b.appendReply(r, d, m)
	case installSnapshot:
		return b.installSnapshot(r, d, m)
	case snapshotAck:
		return b.snapshotAck(r, d, m)
	case timeoutNow:
		return b.timeoutNow(r, d, m)
	}
	return r, nil
}

// newTerm moves the member to a later term, in which it has not voted,
// and knows no leader yet.
func (d *replica[S]) newTerm(term uint64) {
	d.term, d.leader = term, proc.PID{}
	d.rarely().votedFor = proc.PID{}
	d.rarely().incoming = receiving{}
}

// others returns the members other than this one.
func (d *replica[S]) others() []proc.PID {
	return slices.DeleteFunc(d.config.members(), func(id proc.PID) bool { return id == d.id })
}

// send sends msg to the member id.
func (b behaviour[S, Cmd, Rep]) send(id proc.PID, msg any) actor.Effect {
	return actor.Send{To: b.at(id), Msg: msg}
}

// at is where the member id runs: on its node, under the name of the
// group, whatever process it is now.
func (b behaviour[S, Cmd, Rep]) at(id proc.PID) actor.Remote {
	return actor.Remote{Node: id.Node(), Name: replicaName(b.spec.Group)}
}

// rare is what a member changes rarely, as it votes, its members change,
// or it takes or gets a snapshot. Apart from the replica, behind a pointer,
// it is not copied with it at every event, as genstatem copies the data
// of a machine. It is a value too, shared with the replicas before: the
// member changes it through rarely, which copies it first.
type rare struct {
	votedFor proc.PID   // in term, if it voted
	snapshot Snapshot   // of the entries up to log.start
	config   Config     // the latest in the log
	configAt uint64     // its index
	votes    []proc.PID // granted to it, as a candidate or precandidate
	joining  []joining  // members added, to reply to once voters, as the leader
	unheard  []string   // the nodes silent or down, as dist tells
	incoming receiving  // the snapshot coming from the leader, as a follower
}

// rarely returns the rare part of the member to change: a copy of it, made
// once an event, which the member reads from then on too.
func (d *replica[S]) rarely() *rare {
	if d.owned != d.rare {
		c := *d.rare
		d.rare, d.owned = &c, &c
	}
	return d.rare
}

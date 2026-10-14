package quorum

import (
	"slices"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genstatem"
	"github.com/shun159/molecule/proc"
)

// maxEntries is the most entries a message carries.
const maxEntries = 64

// lead makes the member the leader of its term. It appends an entry of
// its own, which commits the entries of earlier terms with it, tells the
// others, and starts beating.
func (b behaviour[S, Cmd, Rep]) lead(d *replica[S]) []actor.Effect {
	d.leader, d.votes = d.id, nil
	d.log = d.log.add(b.entry(d, Entry{}))
	d.peers, d.round = nil, 0
	for _, id := range d.others() {
		d.peers = append(d.peers, peer{id: id, next: d.log.lastIndex()})
	}
	effs := b.saveLog(d, d.log.lastIndex())
	effs = append(effs, genstatem.StartStateTimeout{After: b.spec.Heartbeat}, b.quorumTimer())
	if l, ok := b.spec.Machine.(Leader[S]); ok {
		effs = append(effs, l.Elected(d.state, d.term)...)
	}
	effs = append(effs, b.replicate(*d)...)
	return append(effs, b.commit(leader, d)...)
}

// surrender ends the leadership of the member. The commands it waits on
// may be applied or not, which it cannot know: it tells their clients so.
func (b behaviour[S, Cmd, Rep]) surrender(d *replica[S]) []actor.Effect {
	effs := []actor.Effect{genstatem.CancelTimeout{Name: checkQuorum{}}}
	for _, p := range d.pending {
		effs = append(effs, actor.Reply{To: p.from, Value: deposed{}})
	}
	for _, j := range d.joining {
		effs = append(effs, actor.Reply{To: j.from, Value: deposed{}})
	}
	for _, rd := range d.reads {
		effs = append(effs, actor.Reply{To: rd.from, Value: redirect{}}) // to read again
	}
	d.pending, d.joining, d.reads, d.peers = nil, nil, nil, nil
	if l, ok := b.spec.Machine.(Leader[S]); ok {
		effs = append(effs, l.Surrendered(d.state)...)
	}
	return effs
}

// heartbeat sends every other member what it misses, nothing if it is up
// to date, for it to know the leader lives.
func (b behaviour[S, Cmd, Rep]) heartbeat(d replica[S]) []actor.Effect {
	return append(b.replicate(d), genstatem.StartStateTimeout{After: b.spec.Heartbeat})
}

// replicate sends every other member the entries it misses.
func (b behaviour[S, Cmd, Rep]) replicate(d replica[S]) []actor.Effect {
	var effs []actor.Effect
	for _, p := range d.peers {
		effs = append(effs, b.appendTo(d, p))
	}
	return effs
}

// appendTo sends p the entries from its next one on, after the one before,
// or the snapshot, if the leader has them no more.
func (b behaviour[S, Cmd, Rep]) appendTo(d replica[S], p peer) actor.Effect {
	if p.next <= d.log.start {
		return b.send(p.id, installSnapshot{Term: d.term, Leader: d.id, To: p.id, Snapshot: d.snapshot})
	}
	prev := p.next - 1
	return b.send(p.id, appendEntries{
		Term:      d.term,
		Leader:    d.id,
		To:        p.id,
		PrevIndex: prev,
		PrevTerm:  d.log.termAt(prev),
		Entries:   d.log.from(p.next, maxEntries),
		Commit:    d.commit,
		Read:      d.readSeq,
	})
}

// appendEntries takes the entries of the leader, if the log matches the
// leader's up to them, and replies how far it now matches, once they are
// saved.
func (b behaviour[S, Cmd, Rep]) appendEntries(r role, d replica[S], m appendEntries) (role, replica[S], []actor.Effect) {
	if m.Term < d.term {
		// From a leader deposed, which learns of the later term.
		return r, d, d.after(b.send(m.Leader, appendReply{Term: d.term, From: d.id}))
	}
	// From the leader of the term: a candidate in it has lost.
	r, d.leader = follower, m.Leader
	effs := []actor.Effect{d.electionTimer(b.spec.ElectionTimeout)}
	if !d.log.has(m.PrevIndex, m.PrevTerm) {
		reply := appendReply{Term: d.term, From: d.id, LastIndex: d.log.lastIndex(), Read: m.Read}
		return r, d, append(effs, d.after(b.send(m.Leader, reply))...)
	}
	log, changed := d.log.merge(m.PrevIndex, m.Entries)
	if changed > 0 {
		d.log = log
		d.logChanged()
		effs = append(effs, b.saveLog(&d, changed)...)
	}
	match := m.PrevIndex + uint64(len(m.Entries))
	d.commit = max(d.commit, min(m.Commit, match))
	effs = append(effs, b.apply(r, &d)...)
	reply := appendReply{Term: d.term, From: d.id, Success: true, Match: match, Read: m.Read}
	return r, d, append(effs, d.after(b.send(m.Leader, reply))...)
}

// appendReply learns how far the log of a member matches the leader's, and
// sends it what it still misses: from further back, if it failed.
func (b behaviour[S, Cmd, Rep]) appendReply(r role, d replica[S], m appendReply) (role, replica[S], []actor.Effect) {
	if r != leader || m.Term != d.term {
		return r, d, nil
	}
	p := d.peer(m.From)
	if p == nil {
		return r, d, nil
	}
	d.heard(p)
	p.read = max(p.read, m.Read)
	if !m.Success {
		p.next = max(1, min(p.next-1, m.LastIndex+1))
		return r, d, append(b.serveReads(&d), b.appendTo(d, *p))
	}
	p.match = max(p.match, m.Match)
	p.next = max(p.next, p.match+1)
	progress := *p
	effs := append(b.serveReads(&d), b.commit(r, &d)...)
	if progress.next <= d.log.lastIndex() {
		effs = append(effs, b.appendTo(d, progress))
	}
	return r, d, append(effs, b.promote(r, &d, progress)...)
}

// peer returns the progress of the member id, in a copy of the peers the
// data owns, to change; nil if it is not one of them.
func (d *replica[S]) peer(id proc.PID) *peer {
	i := slices.IndexFunc(d.peers, func(p peer) bool { return p.id == id })
	if i < 0 {
		return nil
	}
	d.peers = slices.Clone(d.peers)
	return &d.peers[i]
}

// commit commits the latest entry of the term that a majority of the
// voters holds, with those before it, and applies them. An entry of an
// earlier term is never committed by counting, only by one of the term
// after it: a majority may hold it, yet a later leader may not.
func (b behaviour[S, Cmd, Rep]) commit(r role, d *replica[S]) []actor.Effect {
	for n := d.log.lastIndex(); n > d.commit && d.log.termAt(n) == d.term; n-- {
		if d.holders(n) >= d.config.majority() {
			d.commit = n
			d.prunePeers()
			return b.apply(r, d)
		}
	}
	return nil
}

// holders counts the voters known to hold the entry at index n: the
// leader, once it is durable there, and the others matching it that far.
func (d *replica[S]) holders(n uint64) int {
	count := 0
	if d.config.isVoter(d.id) && d.stable >= n {
		count++
	}
	for _, p := range d.peers {
		if d.config.isVoter(p.id) && p.match >= n {
			count++
		}
	}
	return count
}

// apply applies the entries committed, not applied yet. The leader also
// performs their effects, and replies to the commands; the others drop
// both.
func (b behaviour[S, Cmd, Rep]) apply(r role, d *replica[S]) []actor.Effect {
	var effs []actor.Effect
	for d.applied < d.commit {
		d.applied++
		reply, more := b.applyEntry(d, d.applied, d.log.at(d.applied))
		if r == leader {
			effs = append(effs, more...)
			effs = append(effs, d.reply(d.applied, reply)...)
		}
	}
	if r == leader {
		effs = append(effs, d.joined()...)
		effs = append(effs, b.serveReads(d)...)
	}
	return append(effs, b.takeSnapshot(d)...)
}

// applyEntry applies e, at index, and returns the reply to its client,
// and the effects of the machine. A command of a session applied before
// is not applied again: its reply is the first's. One of a session
// expired is not applied.
func (b behaviour[S, Cmd, Rep]) applyEntry(d *replica[S], index uint64, e Entry) (any, []actor.Effect) {
	d.sessions = d.sessions.passed(e.Time, b.spec.SessionTimeout)
	switch {
	case e.Open:
		d.sessions = d.sessions.opened(index)
		return applied{Rep: index}, nil
	case e.Cmd == nil: // the entry of an election, or a configuration
		return applied{}, nil
	case e.From.Session == 0:
		return b.applyCmd(d, e.Cmd)
	}
	open, done, rep := d.sessions.seen(e.From)
	switch {
	case !open:
		return expired{}, nil
	case done:
		d.sessions = d.sessions.applied(e.From, rep)
		return applied{rep}, nil
	}
	reply, effs := b.applyCmd(d, e.Cmd)
	d.sessions = d.sessions.applied(e.From, reply.Rep)
	return reply, effs
}

// applyCmd applies cmd to the machine.
func (b behaviour[S, Cmd, Rep]) applyCmd(d *replica[S], cmd any) (applied, []actor.Effect) {
	state, rep, effs := b.spec.Machine.Apply(d.state, cmd.(Cmd))
	d.state = state
	return applied{rep}, effs
}

// reply answers the command or the change at index with v, if the leader
// appended it.
func (d *replica[S]) reply(index uint64, v any) []actor.Effect {
	if len(d.pending) == 0 || d.pending[0].index != index {
		return nil
	}
	p := d.pending[0]
	d.pending = d.pending[1:]
	return actor.Do(actor.Reply{To: p.from, Value: v})
}

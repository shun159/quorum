package quorum

import (
	"slices"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genstatem"
)

// maxEntries is the most entries a message carries.
const maxEntries = 64

// lead makes the member the leader of its term. It appends an entry of
// its own, which commits the entries of earlier terms with it, tells the
// others, and starts beating.
func (b behaviour[S, Cmd, Rep]) lead(d *replica[S]) []actor.Effect {
	d.leader, d.votes = d.self, nil
	d.log = append(d.log, entry{Term: d.term})
	d.peers = nil
	for _, m := range b.others(*d) {
		d.peers = append(d.peers, peer{name: m, next: d.log.lastIndex()})
	}
	effs := []actor.Effect{genstatem.StartStateTimeout{After: b.spec.Heartbeat}}
	if l, ok := b.spec.Machine.(Leader[S]); ok {
		effs = append(effs, l.Elected(d.state, d.term)...)
	}
	effs = append(effs, b.replicate(*d)...)
	return append(effs, b.commit(leader, d)...)
}

// surrender ends the leadership of the member. The commands it waits on
// may be applied or not, which it cannot know: it tells their clients so.
func (b behaviour[S, Cmd, Rep]) surrender(d *replica[S]) []actor.Effect {
	var effs []actor.Effect
	for _, p := range d.pending {
		effs = append(effs, actor.Reply{To: p.from, Value: deposed{}})
	}
	d.pending, d.peers = nil, nil
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

// appendTo sends p the entries from its next one on, after the one before.
func (b behaviour[S, Cmd, Rep]) appendTo(d replica[S], p peer) actor.Effect {
	prev := p.next - 1
	return b.send(p.name, appendEntries{
		Term:      d.term,
		Leader:    d.self,
		PrevIndex: prev,
		PrevTerm:  d.log.termAt(prev),
		Entries:   d.log.from(p.next, maxEntries),
		Commit:    d.commit,
	})
}

// appendEntries takes the entries of the leader, if the log matches the
// leader's up to them, and replies how far it now matches.
func (b behaviour[S, Cmd, Rep]) appendEntries(r role, d replica[S], m appendEntries) (role, replica[S], []actor.Effect) {
	if m.Term < d.term {
		// From a leader deposed, which learns of the later term.
		return r, d, actor.Do(b.send(m.Leader, appendReply{Term: d.term, From: d.self}))
	}
	// From the leader of the term: a candidate in it has lost.
	r, d.leader = follower, m.Leader
	effs := []actor.Effect{d.electionTimer(b.spec.ElectionTimeout)}
	if !d.log.has(m.PrevIndex, m.PrevTerm) {
		reply := appendReply{Term: d.term, From: d.self, LastIndex: d.log.lastIndex()}
		return r, d, append(effs, b.send(m.Leader, reply))
	}
	d.log = d.log.merge(m.PrevIndex, m.Entries)
	match := m.PrevIndex + uint64(len(m.Entries))
	d.commit = max(d.commit, min(m.Commit, match))
	effs = append(effs, b.apply(r, &d)...)
	reply := appendReply{Term: d.term, From: d.self, Success: true, Match: match}
	return r, d, append(effs, b.send(m.Leader, reply))
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
	if !m.Success {
		p.next = max(1, min(p.next-1, m.LastIndex+1))
		return r, d, actor.Do(b.appendTo(d, *p))
	}
	p.match = max(p.match, m.Match)
	p.next = max(p.next, p.match+1)
	effs := b.commit(r, &d)
	if p.next <= d.log.lastIndex() {
		effs = append(effs, b.appendTo(d, *p))
	}
	return r, d, effs
}

// peer returns the progress of the member name, in a copy of the peers the
// data owns, to change; nil if it is not one of them.
func (d *replica[S]) peer(name string) *peer {
	i := slices.IndexFunc(d.peers, func(p peer) bool { return p.name == name })
	if i < 0 {
		return nil
	}
	d.peers = slices.Clone(d.peers)
	return &d.peers[i]
}

// commit commits the latest entry of the term that a majority holds, with
// those before it, and applies them. An entry of an earlier term is never
// committed by counting, only by one of the term after it: a majority may
// hold it, yet a later leader may not.
func (b behaviour[S, Cmd, Rep]) commit(r role, d *replica[S]) []actor.Effect {
	for n := d.log.lastIndex(); n > d.commit && d.log.termAt(n) == d.term; n-- {
		if d.holders(n) >= b.spec.majority() {
			d.commit = n
			return b.apply(r, d)
		}
	}
	return nil
}

// holders counts the members known to hold the entry at index n: the
// leader, and the others matching it that far.
func (d *replica[S]) holders(n uint64) int {
	count := 1
	for _, p := range d.peers {
		if p.match >= n {
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
		e := d.log.at(d.applied)
		if e.Cmd == nil {
			continue // the entry of an election
		}
		state, rep, more := b.spec.Machine.Apply(d.state, e.Cmd.(Cmd))
		d.state = state
		if r == leader {
			effs = append(effs, more...)
			effs = append(effs, d.reply(d.applied, rep)...)
		}
	}
	return effs
}

// reply answers the command at index, if the leader appended it.
func (d *replica[S]) reply(index uint64, rep any) []actor.Effect {
	if len(d.pending) == 0 || d.pending[0].index != index {
		return nil
	}
	p := d.pending[0]
	d.pending = d.pending[1:]
	return actor.Do(actor.Reply{To: p.from, Value: applied{rep}})
}

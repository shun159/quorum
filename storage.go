package quorum

import (
	"fmt"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/proc"
)

// Storage keeps what a member must not forget across restarts: its term,
// its vote, and its log. Raft is safe only if what a member tells others
// rests on what it saved: it grants a vote, or acknowledges entries, once
// they are durable.
//
// Writes are effects, for the member to stay pure: its runtime performs
// them, and a message tells when they are durable, which Durable
// recognizes. Once a write is durable, those before it are too.
type Storage interface {
	// Load returns what was saved, as the member starts.
	Load() (Saved, error)
	// SaveState returns the effect saving st.
	SaveState(st State, tag any) actor.Effect
	// Append returns the effect writing entries from index on, in place
	// of the entries saved from index on.
	Append(index uint64, entries []Entry, tag any) actor.Effect
	// SaveSnapshot returns the effect saving snap, in place of the
	// entries up to its index; those after it stay.
	SaveSnapshot(snap Snapshot, tag any) actor.Effect
	// Durable reports whether msg tells that the write of tag is durable,
	// or that it failed, with err.
	Durable(msg any) (tag any, err error, ok bool)
}

// State is what a member saves besides its log: who it is, the latest
// term it knows, and whom it voted for in it, if anyone.
type State struct {
	Self     proc.PID
	Term     uint64
	VotedFor proc.PID
}

// Snapshot is the state of the machine once the entries up to Index are
// applied, with the term of the last of them, the configuration in force
// then, and the sessions of the clients: it takes the place of those
// entries.
type Snapshot struct {
	Index    uint64
	Term     uint64
	Config   Config
	State    any
	Sessions Sessions
}

// Saved is what a member saved: its state, its snapshot, if Index is not
// 0, and its log after the snapshot.
type Saved struct {
	State    State
	Snapshot Snapshot
	Log      []Entry
}

// writeTag tags a write: Seq numbers the writes of the member, and Index
// is the last index of its log as it wrote, durable with it.
type writeTag struct {
	Seq   uint64
	Index uint64
}

// held is what a member waits to tell until its write seq is durable.
type held struct {
	seq  uint64
	effs []actor.Effect
}

// load returns what the member saved, nothing without a storage.
func (b behaviour[S, Cmd, Rep]) load() (Saved, error) {
	if b.spec.Storage == nil {
		return Saved{}, nil
	}
	return b.spec.Storage.Load()
}

// saveState saves the term and the vote of the member.
func (b behaviour[S, Cmd, Rep]) saveState(d *replica[S]) []actor.Effect {
	st := State{Self: d.id, Term: d.term, VotedFor: d.votedFor}
	return b.write(d, func(tag writeTag) actor.Effect { return b.spec.Storage.SaveState(st, tag) })
}

// saveLog saves the log of the member from index on, which changed.
func (b behaviour[S, Cmd, Rep]) saveLog(d *replica[S], index uint64) []actor.Effect {
	d.stable = min(d.stable, index-1)
	entries := d.log.from(index, len(d.log.entries))
	return b.write(d, func(tag writeTag) actor.Effect { return b.spec.Storage.Append(index, entries, tag) })
}

// saveSnapshot saves the snapshot of the member, in place of the entries
// it holds.
func (b behaviour[S, Cmd, Rep]) saveSnapshot(d *replica[S]) []actor.Effect {
	snap := d.snapshot
	return b.write(d, func(tag writeTag) actor.Effect { return b.spec.Storage.SaveSnapshot(snap, tag) })
}

// write returns the effect of a write, tagged as the next. Without a
// storage, there is none, and the log is as durable as it gets at once.
func (b behaviour[S, Cmd, Rep]) write(d *replica[S], effect func(writeTag) actor.Effect) []actor.Effect {
	if b.spec.Storage == nil {
		d.stable = d.log.lastIndex()
		return nil
	}
	d.seq++
	return actor.Do(effect(writeTag{Seq: d.seq, Index: d.log.lastIndex()}))
}

// after returns effs to perform now if every write so far is durable, or
// holds them until it is: what they tell rests on what was written.
func (d *replica[S]) after(effs ...actor.Effect) []actor.Effect {
	if d.saved == d.seq {
		return effs
	}
	d.held = append(d.held, held{seq: d.seq, effs: effs})
	return nil
}

// isDurable reports whether msg tells of a write of the member.
func (b behaviour[S, Cmd, Rep]) isDurable(msg any) (tag any, err error, ok bool) {
	if b.spec.Storage == nil {
		return nil, nil, false
	}
	return b.spec.Storage.Durable(msg)
}

// durable learns that the writes up to the one of tag are durable: the
// member tells what waited on them, and the leader counts itself as
// holding the entries written. A member that cannot save stops.
func (b behaviour[S, Cmd, Rep]) durable(r role, d replica[S], tag any, err error) (role, replica[S], []actor.Effect) {
	if err != nil {
		return r, d, actor.Do(actor.Stop{Reason: fmt.Errorf("quorum: group %s cannot save: %w", b.spec.Group, err)})
	}
	t, ok := tag.(writeTag)
	if !ok || t.Seq <= d.saved {
		return r, d, nil // known durable already, with a later one
	}
	d.saved, d.stable = t.Seq, t.Index
	var effs []actor.Effect
	for len(d.held) > 0 && d.held[0].seq <= d.saved {
		effs = append(effs, d.held[0].effs...)
		d.held = d.held[1:]
	}
	if r == leader {
		effs = append(effs, b.commit(r, &d)...)
	}
	return r, d, effs
}

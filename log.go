package quorum

import "slices"

// Entry is a command in the log, with the term of the leader that
// appended it, or a configuration of the group. The entry a leader appends
// on being elected has neither, and the first of the log, founding the
// group, has the term 0.
type Entry struct {
	Term   uint64
	Cmd    any
	Config *Config
}

// raftLog is the log of a member: its entries after start, those up to
// start replaced by a snapshot, which records the term of the last of them
// and the configuration in force then. Without a snapshot, start is 0, and
// the first entry is at index 1.
//
// It is a value: appending past its end leaves the entries of an older
// copy as they are, and a change within it makes a new slice.
type raftLog struct {
	start       uint64
	startTerm   uint64
	startConfig Config
	entries     []Entry
}

// logOf is the log of entries from index 1 on.
func logOf(entries ...Entry) raftLog { return raftLog{entries: entries} }

func (l raftLog) lastIndex() uint64 { return l.start + uint64(len(l.entries)) }

func (l raftLog) lastTerm() uint64 { return l.termAt(l.lastIndex()) }

// termAt returns the term of the entry at index i, or 0 if the log has no
// entry there, nor the snapshot the last one.
func (l raftLog) termAt(i uint64) uint64 {
	switch {
	case i == l.start:
		return l.startTerm
	case i < l.start || i > l.lastIndex():
		return 0
	}
	return l.entries[i-l.start-1].Term
}

// at returns the entry at index i, which must be in the log, past start.
func (l raftLog) at(i uint64) Entry { return l.entries[i-l.start-1] }

// add returns the log with e appended.
func (l raftLog) add(e Entry) raftLog {
	l.entries = append(l.entries, e)
	return l
}

// has reports whether the log holds the entry of term at index, as the
// leader's does: the log matches the leader's up to there. Up to start,
// the entries are committed, the same in every log that has them.
func (l raftLog) has(index, term uint64) bool {
	return index < l.start || (index <= l.lastIndex() && l.termAt(index) == term)
}

// from returns a copy of the entries from index i on, past start, at most
// max, for a message: what is sent must not change after.
func (l raftLog) from(i uint64, max int) []Entry {
	if i > l.lastIndex() {
		return nil
	}
	first := i - l.start - 1
	return slices.Clone(l.entries[first:min(uint64(len(l.entries)), first+uint64(max))])
}

// upTo returns the entries after start up to index i.
func (l raftLog) upTo(i uint64) []Entry {
	return l.entries[:min(uint64(len(l.entries)), i-min(i, l.start))]
}

// merge returns the log with the leader's entries after index prev, which
// the log has, and the index from which it changed, 0 if it did not. An
// entry it holds already is kept; from the first that differs in term, the
// log takes the leader's instead of its own. Those up to start, it has.
func (l raftLog) merge(prev uint64, entries []Entry) (raftLog, uint64) {
	if prev < l.start {
		skip := min(l.start-prev, uint64(len(entries)))
		prev, entries = prev+skip, entries[skip:]
	}
	for k, e := range entries {
		i := prev + 1 + uint64(k)
		if i > l.lastIndex() {
			l.entries = append(l.entries, entries[k:]...)
			return l, i
		}
		if l.termAt(i) != e.Term {
			l.entries = append(slices.Clip(l.entries[:i-l.start-1]), entries[k:]...)
			return l, i
		}
	}
	return l, 0
}

// notAheadOf reports whether the log is no more up to date than one
// ending with an entry of lastTerm at lastIndex: the later last term is
// ahead, or with the same, the longer log. A member votes only for a
// candidate whose log its own is not ahead of.
func (l raftLog) notAheadOf(lastIndex, lastTerm uint64) bool {
	if lastTerm != l.lastTerm() {
		return l.lastTerm() < lastTerm
	}
	return l.lastIndex() <= lastIndex
}

// configUpTo returns the configuration in force at index i: the latest
// up to there.
func (l raftLog) configUpTo(i uint64) Config {
	for j := int(min(i, l.lastIndex())-l.start) - 1; j >= 0; j-- {
		if c := l.entries[j].Config; c != nil {
			return *c
		}
	}
	return l.startConfig
}

// compact returns the log after snap, which takes the place of the
// entries up to its index: those after it stay if keep, none else.
func (l raftLog) compact(snap Snapshot, keep bool) raftLog {
	c := raftLog{start: snap.Index, startTerm: snap.Term, startConfig: snap.Config}
	if keep && snap.Index < l.lastIndex() {
		c.entries = slices.Clone(l.entries[snap.Index-l.start:])
	}
	return c
}

// config returns the latest configuration in the log, which is in force
// once appended, committed or not, and its index; the empty one, at 0,
// before the group is founded.
func (l raftLog) config() (Config, uint64) {
	for i := len(l.entries) - 1; i >= 0; i-- {
		if c := l.entries[i].Config; c != nil {
			return *c, l.start + uint64(i) + 1
		}
	}
	return l.startConfig, l.start
}

package quorum

import "slices"

// entry is a command in the log, with the term of the leader that
// appended it. A nil Cmd is the entry a leader appends on being elected,
// which applies nothing.
type entry struct {
	Term uint64
	Cmd  any
}

// raftLog is the entries of a member, the first at index 1. It is a value:
// appending past its end leaves the entries of an older copy as they are,
// and a change within it makes a new slice.
type raftLog []entry

func (l raftLog) lastIndex() uint64 { return uint64(len(l)) }

func (l raftLog) lastTerm() uint64 { return l.termAt(l.lastIndex()) }

// termAt returns the term of the entry at index i, or 0 if there is none.
func (l raftLog) termAt(i uint64) uint64 {
	if i == 0 || i > l.lastIndex() {
		return 0
	}
	return l[i-1].Term
}

// at returns the entry at index i, which must be in the log.
func (l raftLog) at(i uint64) entry { return l[i-1] }

// has reports whether the log holds the entry of term at index, as the
// leader's does: the log matches the leader's up to there. Every log has
// the empty entry 0.
func (l raftLog) has(index, term uint64) bool {
	return index == 0 || (index <= l.lastIndex() && l.termAt(index) == term)
}

// from returns a copy of the entries from index i on, at most max, for a
// message: what is sent must not change after.
func (l raftLog) from(i uint64, max int) []entry {
	if i > l.lastIndex() {
		return nil
	}
	return slices.Clone(l[i-1 : min(len(l), int(i-1)+max)])
}

// merge returns the log with the leader's entries after index prev, which
// the log has. An entry it holds already is kept; from the first that
// differs in term, the log takes the leader's instead of its own.
func (l raftLog) merge(prev uint64, entries []entry) raftLog {
	for k, e := range entries {
		i := prev + 1 + uint64(k)
		if i > l.lastIndex() {
			return append(l, entries[k:]...)
		}
		if l.termAt(i) != e.Term {
			return append(slices.Clip(l[:i-1]), entries[k:]...)
		}
	}
	return l
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

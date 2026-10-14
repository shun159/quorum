package raft

import (
	"slices"
	"testing"
)

// terms makes a log of entries of these terms.
func terms(ts ...uint64) raftLog {
	var l raftLog
	for _, t := range ts {
		l = l.add(Entry{Term: t})
	}
	return l
}

func termsOf(l raftLog) []uint64 {
	var ts []uint64
	for _, e := range l.entries {
		ts = append(ts, e.Term)
	}
	return ts
}

func TestLogHas(t *testing.T) {
	l := terms(1, 1, 2)
	for _, c := range []struct {
		index, term uint64
		want        bool
	}{
		{0, 0, true}, {2, 1, true}, {3, 2, true},
		{3, 1, false}, {4, 2, false},
	} {
		if got := l.has(c.index, c.term); got != c.want {
			t.Errorf("has(%d, %d) = %v", c.index, c.term, got)
		}
	}
}

func TestLogMerge(t *testing.T) {
	for _, c := range []struct {
		name    string
		log     raftLog
		prev    uint64
		entries raftLog
		want    []uint64
	}{
		{"appended", terms(1, 1), 2, terms(2), []uint64{1, 1, 2}},
		{"held already", terms(1, 1, 2), 1, terms(1), []uint64{1, 1, 2}},
		{"conflict taken over", terms(1, 1, 2, 2), 2, terms(3), []uint64{1, 1, 3}},
		{"heartbeat", terms(1), 1, raftLog{}, []uint64{1}},
	} {
		old := slices.Clone(c.log.entries)
		got, _ := c.log.merge(c.prev, c.entries.entries)
		if !slices.Equal(termsOf(got), c.want) {
			t.Errorf("%s: %v, want %v", c.name, termsOf(got), c.want)
		}
		if !slices.Equal(termsOf(c.log), termsOf(logOf(old...))) {
			t.Errorf("%s: the old log changed to %v", c.name, termsOf(c.log))
		}
	}
}

func TestLogNotAheadOf(t *testing.T) {
	l := terms(1, 2, 2)
	for _, c := range []struct {
		lastIndex, lastTerm uint64
		want                bool
	}{
		{3, 2, true},  // as up to date
		{5, 2, true},  // longer
		{1, 3, true},  // a later term
		{2, 2, false}, // shorter
		{9, 1, false}, // an earlier term
	} {
		if got := l.notAheadOf(c.lastIndex, c.lastTerm); got != c.want {
			t.Errorf("notAheadOf(%d, %d) = %v", c.lastIndex, c.lastTerm, got)
		}
	}
}

// A log after a snapshot of the entries up to 3, of term 2, has entries 4
// and 5.
func snapshotted() raftLog {
	return raftLog{start: 3, startTerm: 2, entries: []Entry{{Term: 2}, {Term: 3}}}
}

func TestLogAfterSnapshot(t *testing.T) {
	l := snapshotted()
	if l.lastIndex() != 5 || l.lastTerm() != 3 || l.termAt(3) != 2 || l.termAt(4) != 2 || l.termAt(2) != 0 {
		t.Errorf("last %d of term %d; terms at 2, 3, 4: %d %d %d", l.lastIndex(), l.lastTerm(), l.termAt(2), l.termAt(3), l.termAt(4))
	}
	// The entries up to the snapshot are committed: every log has them.
	if !l.has(1, 1) || !l.has(3, 2) || l.has(3, 1) || !l.has(5, 3) {
		t.Error("has")
	}
	if got := termsOf(raftLog{entries: l.from(5, 10)}); !slices.Equal(got, []uint64{3}) {
		t.Errorf("from 5: %v", got)
	}
	if got := len(l.upTo(4)); got != 1 {
		t.Errorf("up to 4: %d entries", got)
	}
}

// The leader's entries overlapping the snapshot are skipped.
func TestLogMergeAfterSnapshot(t *testing.T) {
	got, changed := snapshotted().merge(2, terms(2, 2, 3, 4).entries)
	if changed != 6 || !slices.Equal(termsOf(got), []uint64{2, 3, 4}) {
		t.Errorf("merged %v, changed from %d", termsOf(got), changed)
	}
	got, changed = snapshotted().merge(1, terms(1, 2).entries)
	if changed != 0 || got.lastIndex() != 5 {
		t.Errorf("merging what the snapshot has: %v, changed from %d", termsOf(got), changed)
	}
}

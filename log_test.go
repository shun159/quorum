package quorum

import (
	"slices"
	"testing"
)

// terms makes a log of entries of these terms.
func terms(ts ...uint64) raftLog {
	var l raftLog
	for _, t := range ts {
		l = append(l, Entry{Term: t})
	}
	return l
}

func termsOf(l raftLog) []uint64 {
	var ts []uint64
	for _, e := range l {
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
		{"heartbeat", terms(1), 1, nil, []uint64{1}},
	} {
		old := slices.Clone(c.log)
		got, _ := c.log.merge(c.prev, c.entries)
		if !slices.Equal(termsOf(got), c.want) {
			t.Errorf("%s: %v, want %v", c.name, termsOf(got), c.want)
		}
		if !slices.Equal(termsOf(c.log), termsOf(old)) {
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

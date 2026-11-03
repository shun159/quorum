package kvsstorage

import (
	"testing"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/gensim"
	"github.com/shun159/quorum/internal/raft"
)

// A repeater performs, at each call, the write of make for the count of
// calls so far, and replies once it is durable: a write at a time, as a
// member waits for each before it tells of it.
type repeater struct {
	genserver.Default[repeating]
	st   raft.Storage
	make func(n uint64) actor.Effect
}

type repeating struct {
	n       uint64
	waiting *genserver.From[error]
}

func (w repeater) HandleCall(s repeating, _ perform, from genserver.From[error]) (repeating, []actor.Effect) {
	s.n++
	return repeating{n: s.n, waiting: &from}, []actor.Effect{w.make(s.n)}
}

func (w repeater) HandleInfo(s repeating, msg any) (repeating, []actor.Effect) {
	if _, err, ok := w.st.Durable(msg); ok && s.waiting != nil {
		reply := s.waiting.Reply(err)
		return repeating{n: s.n}, actor.Do(reply)
	}
	return s, nil
}

// benchWrites measures the writes of make to a table on disk, each made
// durable before the next.
func benchWrites(b *testing.B, make func(st raft.Storage, n uint64) actor.Effect) {
	table, err := Open(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer table.Close()
	st := For(table, "g")
	s := gensim.New(1)
	w := repeater{st: st, make: func(n uint64) actor.Effect { return make(st, n) }}
	pid, err := gensim.Spawn(s, genserver.Gen(w), nil)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		v, err := s.Call(pid, perform{})
		if err != nil {
			b.Fatal(err)
		}
		if err, _ := v.(error); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkAppend appends an entry after the last, as a follower does.
func BenchmarkAppend(b *testing.B) {
	benchWrites(b, func(st raft.Storage, n uint64) actor.Effect {
		return st.Append(n, []raft.Entry{{Term: 1, Cmd: int(n)}}, n)
	})
}

// BenchmarkSaveState saves a term, as a member does at each election.
func BenchmarkSaveState(b *testing.B) {
	benchWrites(b, func(st raft.Storage, n uint64) actor.Effect {
		return st.SaveState(raft.State{Term: n, VotedFor: voted}, n)
	})
}

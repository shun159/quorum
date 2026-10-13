package kvsstorage

import (
	"slices"
	"testing"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/gensim"
	"github.com/shun159/molecule/proc"
	"github.com/shun159/quorum"
)

// writer performs its writes when called, as a member does, and replies
// once the last is durable. They are its own, not in the call: a message
// is fingerprinted, the table within it too, while kvs syncs it.
type writer struct {
	genserver.Default[*genserver.From[error]]
	st   quorum.Storage
	effs []actor.Effect
}

type perform struct{}

func (w writer) HandleCall(_ *genserver.From[error], _ perform, from genserver.From[error]) (*genserver.From[error], []actor.Effect) {
	return &from, w.effs
}

func (w writer) HandleInfo(waiting *genserver.From[error], msg any) (*genserver.From[error], []actor.Effect) {
	if _, err, ok := w.st.Durable(msg); ok && waiting != nil {
		return nil, actor.Do(waiting.Reply(err))
	}
	return waiting, nil
}

// write performs effs on st, in a simulation, and waits for them.
func write(t *testing.T, st quorum.Storage, effs ...actor.Effect) {
	t.Helper()
	s := gensim.New(1)
	pid, err := gensim.Spawn(s, genserver.Gen(writer{st: st, effs: effs}), nil)
	if err != nil {
		t.Fatal(err)
	}
	if v, err := s.Call(pid, perform{}); err != nil || v != nil {
		t.Fatalf("write: %v, %v", v, err)
	}
}

// voted is the member voted for, a PID.
var voted = proc.NewNode("a").NewPID()

func entries(terms ...uint64) []quorum.Entry {
	var es []quorum.Entry
	for i, term := range terms {
		es = append(es, quorum.Entry{Term: term, Cmd: i})
	}
	return es
}

// What is written is loaded back once the table is opened again, the
// entries replaced by a later append gone, and the groups apart.
func TestStorage(t *testing.T) {
	dir := t.TempDir()
	table, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	g, gx := For(table, "g"), For(table, "gx")
	write(t, g, g.SaveState(quorum.State{Term: 3, VotedFor: voted}, 1), g.Append(1, entries(1, 1, 2, 2), 2))
	write(t, gx, gx.Append(1, entries(7), 3))
	// A leader of term 3 replaces the entries from index 3 on.
	write(t, g, g.Append(3, []quorum.Entry{{Term: 3, Cmd: "new"}}, 4))
	if err := table.Close(); err != nil {
		t.Fatal(err)
	}

	table, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer table.Close()
	saved, err := For(table, "g").Load()
	if err != nil {
		t.Fatal(err)
	}
	want := quorum.Saved{
		State: quorum.State{Term: 3, VotedFor: voted},
		Log:   append(entries(1, 1), quorum.Entry{Term: 3, Cmd: "new"}),
	}
	if saved.State != want.State || !slices.Equal(saved.Log, want.Log) {
		t.Errorf("g loaded %+v, want %+v", saved, want)
	}
	if saved, _ := For(table, "gx").Load(); !slices.Equal(saved.Log, entries(7)) {
		t.Errorf("gx loaded %+v", saved)
	}
	if saved, _ := For(table, "none").Load(); saved.State != (quorum.State{}) || len(saved.Log) != 0 {
		t.Errorf("a group never written loaded %+v", saved)
	}
}

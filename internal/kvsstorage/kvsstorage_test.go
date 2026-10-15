package kvsstorage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/behaviours/supervisor"
	"github.com/shun159/molecule/gensim"
	"github.com/shun159/molecule/kvs"
	"github.com/shun159/molecule/proc"
	"github.com/shun159/quorum/internal/raft"
)

// writer performs its writes when called, as a member does, and replies
// once the last is durable. They are its own, not in the call: a message
// is fingerprinted, the table within it too, while kvs syncs it.
type writer struct {
	genserver.Default[*genserver.From[error]]
	st   raft.Storage
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

// save performs effs on st, in a simulation, and waits for them.
func save(t *testing.T, st raft.Storage, effs ...actor.Effect) {
	t.Helper()
	if err := saving(t, st, effs...); err != nil {
		t.Fatalf("write: %v", err)
	}
}

// saving is save, returning the error of the writes.
func saving(t *testing.T, st raft.Storage, effs ...actor.Effect) error {
	t.Helper()
	s := gensim.New(1)
	pid, err := gensim.Spawn(s, genserver.Gen(writer{st: st, effs: effs}), nil)
	if err != nil {
		t.Fatal(err)
	}
	v, err := s.Call(pid, perform{})
	if err != nil {
		t.Fatal(err)
	}
	err, _ = v.(error)
	return err
}

// voted is the member voted for, a PID.
var voted = proc.NewNode("a").NewPID()

func entries(terms ...uint64) []raft.Entry {
	var es []raft.Entry
	for i, term := range terms {
		es = append(es, raft.Entry{Term: term, Cmd: i})
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
	save(t, g, g.SaveState(raft.State{Term: 3, VotedFor: voted}, 1), g.Append(1, entries(1, 1, 2, 2), 2))
	save(t, gx, gx.Append(1, entries(7), 3))
	// A leader of term 3 replaces the entries from index 3 on.
	save(t, g, g.Append(3, []raft.Entry{{Term: 3, Cmd: "new"}}, 4))
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
	want := raft.Saved{
		State: raft.State{Term: 3, VotedFor: voted},
		Log:   append(entries(1, 1), raft.Entry{Term: 3, Cmd: "new"}),
	}
	if saved.State != want.State || !slices.Equal(saved.Log, want.Log) {
		t.Errorf("g loaded %+v, want %+v", saved, want)
	}
	if saved, _ := For(table, "gx").Load(); !slices.Equal(saved.Log, entries(7)) {
		t.Errorf("gx loaded %+v", saved)
	}
	if saved, _ := For(table, "none").Load(); saved.State != (raft.State{}) || len(saved.Log) != 0 {
		t.Errorf("a group never written loaded %+v", saved)
	}
}

// A snapshot takes the place of the entries up to it; those after it stay.
func TestSnapshot(t *testing.T) {
	dir := t.TempDir()
	table, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	g := For(table, "g")
	enc := raft.Encoded{Head: []byte("head"), State: []byte("state")}
	encode := func() (raft.Encoded, error) { return enc, nil }
	save(t, g, g.Append(1, entries(1, 1, 1, 2), 1), g.SaveSnapshot(2, encode, 2))
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
	if saved.Snapshot.Index != 2 || string(saved.Snapshot.Head) != "head" || string(saved.Snapshot.State) != "state" {
		t.Errorf("snapshot loaded %+v", saved.Snapshot)
	}
	if want := entries(1, 1, 1, 2)[2:]; !slices.Equal(saved.Log, want) {
		t.Errorf("log loaded %+v, want %+v", saved.Log, want)
	}
	// What is left of the log is that after the snapshot.
	for k := range table.Range(Key{Group: "g", Kind: KindEntry}, Key{Group: "g", Kind: KindEntry, Index: 3}) {
		t.Errorf("entry %d left", k.Index)
	}
}

// A snapshot failing to encode is not saved, nor are the entries it was
// to replace deleted: the member hears of the error.
func TestSnapshotFailing(t *testing.T) {
	table, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer table.Close()
	g := For(table, "g")
	save(t, g, g.Append(1, entries(1, 1, 1), 1))
	bad := func() (raft.Encoded, error) { return raft.Encoded{}, errors.New("no encoding") }
	if err := saving(t, g, g.SaveSnapshot(2, bad, 2)); err == nil {
		t.Fatal("saved a snapshot failing to encode")
	}
	saved, err := g.Load()
	if err != nil || saved.Snapshot.Index != 0 || !slices.Equal(saved.Log, entries(1, 1, 1)) {
		t.Errorf("loaded snapshot %d, log %+v, %v; want none, the 3 entries", saved.Snapshot.Index, saved.Log, err)
	}
}

// The table of Child is open while it runs, for the members In it, and
// closed once it stops.
func TestChild(t *testing.T) {
	dir := t.TempDir()
	g := In(dir, "g")
	if _, err := g.Load(); err == nil {
		t.Fatal("loaded before the table is open")
	}

	n := proc.NewNode("a")
	ctx := context.Background()
	sup, err := supervisor.Start(ctx, n, supervisor.Spec{
		Children: []supervisor.ChildSpec{{ID: "storage", Start: Child(dir)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	save(t, g, g.Append(1, entries(1, 1), 1))
	if saved, err := g.Load(); err != nil || !slices.Equal(saved.Log, entries(1, 1)) {
		t.Fatalf("loaded %+v, %v", saved, err)
	}
	if err := supervisor.Stop(ctx, n, sup); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Load(); err == nil {
		t.Error("loaded once the table is closed")
	}

	table, err := Open(dir) // only once closed
	if err != nil {
		t.Fatal(err)
	}
	defer table.Close()
	if saved, _ := For(table, "g").Load(); !slices.Equal(saved.Log, entries(1, 1)) {
		t.Errorf("loaded %+v from the table opened again", saved)
	}
}

// The groups started on a node are kept with their members; forgotten, a
// group's records are all gone, those of the others kept. One a crash
// left being forgotten is told apart.
func TestRegistered(t *testing.T) {
	dir := t.TempDir()
	n := proc.NewNode("a")
	ctx := context.Background()
	sup, err := supervisor.Start(ctx, n, supervisor.Spec{
		Children: []supervisor.ChildSpec{{ID: "storage", Start: Child(dir)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer supervisor.Stop(ctx, n, sup)
	for _, g := range []string{"x", "y", "z"} {
		if err := Register(dir, g); err != nil {
			t.Fatal(err)
		}
		st := In(dir, g)
		encode := func() (raft.Encoded, error) { return raft.Encoded{Head: []byte(g)}, nil }
		save(t, st, st.SaveState(raft.State{Term: 1}, 1), st.Append(1, entries(1, 1, 1), 2), st.SaveSnapshot(1, encode, 3))
	}
	if err := Forget(dir, "y"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "snapshots", "y")); !os.IsNotExist(err) {
		t.Errorf("the snapshots of y forgotten stay: %v", err)
	}
	started, deleting, err := Registered(dir)
	if err != nil || !slices.Equal(started, []string{"x", "z"}) || len(deleting) != 0 {
		t.Fatalf("registered %v, deleting %v, %v", started, deleting, err)
	}
	if saved, _ := In(dir, "y").Load(); saved.State.Term != 0 || len(saved.Log) != 0 {
		t.Errorf("y forgotten, loaded %+v", saved)
	}
	if saved, _ := In(dir, "x").Load(); len(saved.Log) != 2 || string(saved.Snapshot.Head) != "x" {
		t.Errorf("x loaded %+v", saved)
	}

	table, _ := kvs.Opened[Key, Record](dir)
	if err := table.Put(registration("z"), Record{Deleting: true}); err != nil { // as a Forget cut short
		t.Fatal(err)
	}
	if started, deleting, _ := Registered(dir); !slices.Equal(started, []string{"x"}) || !slices.Equal(deleting, []string{"z"}) {
		t.Errorf("registered %v, deleting %v", started, deleting)
	}
}

// A snapshot is kept in a file of its own, the latest few kept; a file
// changed fails Load.
func TestSnapshotFiles(t *testing.T) {
	dir := t.TempDir()
	table, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer table.Close()
	g := For(table, "a/b")
	save(t, g, g.Append(1, entries(1, 1, 1, 1, 1, 1, 1, 1), 1))
	for i := range uint64(4) {
		index := 2 * (i + 1)
		enc := raft.Encoded{Head: []byte("head"), State: []byte(fmt.Sprint("state ", index))}
		save(t, g, g.SaveSnapshot(index, func() (raft.Encoded, error) { return enc, nil }, 2+i))
	}
	files, _ := filepath.Glob(filepath.Join(dir, "snapshots", "a%2Fb", "*"))
	if len(files) != 3 {
		t.Errorf("files kept: %v; want the 3 latest", files)
	}
	saved, err := g.Load()
	if err != nil || saved.Snapshot.Index != 8 || string(saved.Snapshot.State) != "state 8" || len(saved.Log) != 0 {
		t.Fatalf("loaded %+v, %v; want the snapshot of 8", saved.Snapshot, err)
	}

	last := files[len(files)-1]
	b, _ := os.ReadFile(last)
	b[len(b)-5] ^= 1
	os.WriteFile(last, b, 0o644)
	if _, err := g.Load(); err == nil {
		t.Error("loaded a snapshot changed on disk")
	}
}

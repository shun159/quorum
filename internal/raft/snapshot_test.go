package raft

import (
	"testing"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genstatem"
	"github.com/shun159/molecule/proc"
)

// withCommands returns d with commands of term 1 after its founding, up
// to index last.
func withCommands(d replica[int], last uint64) replica[int] {
	for d.log.lastIndex() < last {
		d.log = d.log.add(Entry{Term: 1, Cmd: 1})
	}
	d.term = 1
	return d
}

// A snapshot has the configuration in force at its index, not one
// appended after it, not applied yet.
func TestSnapshotConfig(t *testing.T) {
	b, d := member(t)
	b.spec.SnapshotEvery = 4
	founding := d.config
	d = withCommands(d, 5)
	d.log = d.log.add(Entry{Term: 1, Config: &Config{Voters: founding.Voters, Learners: []proc.PID{proc.NewNode("d").NewPID()}}})
	d.logChanged(0)
	d.commit, d.applied = 5, 5

	b.takeSnapshot(&d)
	if d.snapshot.Index != 5 || !sameConfig(d.snapshot.Config, founding) {
		t.Errorf("snapshot at %d with %v, want at 5 with %v", d.snapshot.Index, d.snapshot.Config, founding)
	}
	// The configuration after it stays in the log, in force.
	if d.log.start != 5 || d.log.lastIndex() != 6 || len(d.config.Learners) != 1 {
		t.Errorf("log from %d to %d, in force %v", d.log.start, d.log.lastIndex(), d.config)
	}
}

// A snapshot installed replaces the log up to it; the entries after it
// stay if the log goes on with the leader's there, and go if not. Its
// state is encoded by gob, or by the machine.
func TestInstallSnapshot(t *testing.T) {
	for _, c := range []struct {
		name    string
		term    uint64 // of the snapshot's last entry
		wantEnd uint64 // the last index of the log after
		machine Machine[int, int, int]
	}{
		{"matching", 1, 4, counter{}},
		{"diverging", 2, 3, counter{}},
		{"encoded by the machine", 1, 4, encodingCounter{}},
	} {
		b, d := member(t)
		b.spec.Machine = c.machine
		d = withCommands(d, 4)
		snap := Snapshot{Index: 3, Term: c.term, Config: d.config, State: 7}
		data, err := b.encodedSnapshot(&replica[int]{rare: &rare{snapshot: snap}})
		if err != nil {
			t.Fatal(err)
		}
		m := installSnapshot{Term: 2, Leader: ida, To: idb, Index: 3, Size: data.len(), Head: len(data.Head), Data: data.slice(0, data.len())}
		_, d, _ = b.HandleEvent(follower, d, genstatem.Info{Msg: m})
		if d.log.start != 3 || d.log.lastIndex() != c.wantEnd || d.state != 7 || d.applied != 3 {
			t.Errorf("%s: log from %d to %d, state %d applied to %d; want to %d, 7 applied to 3",
				c.name, d.log.start, d.log.lastIndex(), d.state, d.applied, c.wantEnd)
		}
	}
}

// The leader sends a chunk once, not each beat until it is answered, nor
// for a reply sent before; again if unanswered for an election timeout.
func TestSnapshotOnItsWay(t *testing.T) {
	b, d := leading(t)
	b.spec.SnapshotEvery = 1
	b.takeSnapshot(&d)
	d.peers[0] = peer{id: ida, next: 1}
	beat := genstatem.StateTimeout{}
	for i, c := range []struct {
		name   string
		rounds uint64 // of the check of the quorum, before
		ev     genstatem.Event
		want   int
	}{
		{"first beat", 0, beat, 1},
		{"next beat", 0, beat, 0},
		{"stale reply", 0, genstatem.Info{Msg: appendReply{Term: 1, From: ida}}, 0},
		{"beat within the timeout", checksPerTimeout - 1, beat, 0},
		{"beat after", 1, beat, 1},
		{"arrived", 0, genstatem.Info{Msg: appendReply{Term: 1, From: ida, Success: true, Match: 2}}, 0},
	} {
		d.round += c.rounds
		var effs []actor.Effect
		_, d, effs = b.HandleEvent(leader, d, c.ev)
		n := 0
		for _, m := range sent(effs) {
			if _, ok := m.(installSnapshot); ok {
				n++
			}
		}
		if n != c.want {
			t.Errorf("%d %s: %d chunks sent, want %d", i, c.name, n, c.want)
		}
	}
	if p := d.peer(ida); p.sending != 0 || p.match != 2 {
		t.Errorf("after it arrived: sending %d, match %d; want 0, 2", p.sending, p.match)
	}
}

// A snapshot goes in chunks, each once the member has the one before;
// the member installs it with the last.
func TestSnapshotInChunks(t *testing.T) {
	bl, ld := leading(t)
	bl.spec.chunk, bl.spec.SnapshotEvery = 16, 1
	ld.id, ld.leader = idc, idc
	ld.sessions = Sessions{Open: map[uint64]Session{1: {}}}
	bl.takeSnapshot(&ld)
	ld.peers = []peer{{id: idb, next: 1}} // a, up to date, left out
	bf, fd := member(t)

	var effs []actor.Effect
	_, ld, effs = bl.HandleEvent(leader, ld, genstatem.StateTimeout{})
	chunks := 0
	for {
		msgs := sent(effs)
		if len(msgs) != 1 {
			t.Fatalf("after %d chunks, sent %v", chunks, msgs)
		}
		m, ok := msgs[0].(installSnapshot)
		if !ok {
			t.Fatalf("after %d chunks, the leader sent %T", chunks, msgs[0])
		}
		chunks++
		_, fd, effs = bf.HandleEvent(follower, fd, genstatem.Info{Msg: m})
		if len(fd.held) > 0 { // the snapshot saved
			effs, fd.held, fd.saved = fd.held[0].effs, nil, fd.seq
		}
		reply := sent(effs)
		if len(reply) != 1 {
			t.Fatalf("chunk %d: the member sent %v", chunks, reply)
		}
		_, ld, effs = bl.HandleEvent(leader, ld, genstatem.Info{Msg: reply[0]})
		if _, ok := reply[0].(appendReply); ok {
			break
		}
	}
	data, _ := bl.encodedSnapshot(&ld)
	if want := (data.len() + 15) / 16; chunks != want || want < 2 {
		t.Errorf("%d chunks, want %d of the %d bytes", chunks, want, data.len())
	}
	if fd.state != 3 || fd.applied != 2 || len(fd.sessions.Open) != 1 || fd.incoming.data != nil {
		t.Errorf("member: state %d applied to %d, %d sessions, %d bytes kept; want 3 to 2, 1, none",
			fd.state, fd.applied, len(fd.sessions.Open), len(fd.incoming.data))
	}
	if p := ld.peer(idb); p.match != 2 || p.sending != 0 || p.data.len() != 0 {
		t.Errorf("leader: match %d, sending %d, %d bytes kept; want 2, 0, none", p.match, p.sending, p.data.len())
	}
}

// A snapshot encoded is sent in slices of its head and state as if they
// were one.
func TestEncodedSlice(t *testing.T) {
	e := Encoded{Head: []byte("abc"), State: []byte("defg")}
	whole := "abcdefg"
	for from := range len(whole) {
		for to := from; to <= len(whole); to++ {
			if got := string(e.slice(from, to)); got != whole[from:to] {
				t.Errorf("slice(%d, %d) = %q, want %q", from, to, got, whole[from:to])
			}
		}
	}
	if string(e.Head) != "abc" {
		t.Errorf("slicing changed the head: %q", e.Head)
	}
}

// A member tells how much of a snapshot it has, whatever chunk comes: one
// sent again keeps what it has; one of another, it has none of, unless the
// first.
func TestReceiveChunks(t *testing.T) {
	_, d := member(t)
	chunk := func(leader proc.PID, index uint64, offset int) installSnapshot {
		return installSnapshot{Leader: leader, Index: index, Offset: offset, Size: 8, Data: []byte{1, 2}}
	}
	for i, c := range []struct {
		name string
		m    installSnapshot
		want int
	}{
		{"first", chunk(ida, 5, 0), 2},
		{"next", chunk(ida, 5, 2), 4},
		{"first again", chunk(ida, 5, 0), 4},
		{"skipping one", chunk(ida, 5, 6), 4},
		{"of another snapshot", chunk(ida, 6, 4), 0},
		{"of another leader", chunk(idc, 5, 4), 0},
		{"first of another", chunk(idc, 7, 0), 2},
	} {
		if has := d.receive(c.m); has != c.want {
			t.Errorf("%d %s: has %d, want %d", i, c.name, has, c.want)
		}
	}
}

// The snapshot of an Encoder machine is saved as the machine encodes its
// state, apart from the rest, and comes back decoded as the member starts
// again.
func TestSnapshotSavedEncoded(t *testing.T) {
	b, d := member(t)
	b.spec.Machine, b.spec.SnapshotEvery = encodingCounter{}, 1
	d.state, d.applied = 7, 1
	var w *memWrite
	for _, e := range b.takeSnapshot(&d) {
		if e, ok := e.(memEffect); ok && e.w.encode != nil {
			w = &e.w
		}
	}
	if w == nil {
		t.Fatal("no snapshot saved")
	}
	enc, err := w.encode()
	if err != nil || string(enc.State) != "7" {
		t.Fatalf("saved the state as %q, %v; want 7", enc.State, err)
	}
	m := b.spec.Storage.(*memStorage)
	m.saved.Snapshot = SavedSnapshot{Index: w.snapshot.Index, Encoded: enc}
	_, d, _, err = b.Init(idb)
	if err != nil || d.state != 7 || d.snapshot.State != 7 || d.applied != 1 {
		t.Errorf("started again: state %d, snapshot of %#v, applied %d, %v; want 7 to 1", d.state, d.snapshot.State, d.applied, err)
	}
}

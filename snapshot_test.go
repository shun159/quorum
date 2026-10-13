package quorum

import (
	"testing"

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
	d.logChanged()
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
// stay if the log goes on with the leader's there, and go if not.
func TestInstallSnapshot(t *testing.T) {
	for _, c := range []struct {
		name    string
		term    uint64 // of the snapshot's last entry
		wantEnd uint64 // the last index of the log after
	}{
		{"matching", 1, 4},
		{"diverging", 2, 3},
	} {
		b, d := member(t)
		d = withCommands(d, 4)
		snap := Snapshot{Index: 3, Term: c.term, Config: d.config, State: 7}
		_, d, _ = b.HandleEvent(follower, d, genstatem.Info{Msg: installSnapshot{Term: 2, Leader: ida, To: idb, Snapshot: snap}})
		if d.log.start != 3 || d.log.lastIndex() != c.wantEnd || d.state != 7 || d.applied != 3 {
			t.Errorf("%s: log from %d to %d, state %d applied to %d; want to %d, 7 applied to 3",
				c.name, d.log.start, d.log.lastIndex(), d.state, d.applied, c.wantEnd)
		}
	}
}

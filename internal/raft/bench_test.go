package raft

import (
	"testing"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genstatem"
)

// BenchmarkLeader has a leader take a command and the replies of its two
// followers, 64 commands out at once: what a command costs it, without
// the messages, with SnapshotEvery entries in its log at most.
func BenchmarkLeader(b *testing.B) {
	bl, d := leading(b)
	bl.spec.Storage = nil // durable at once
	for i := range d.peers {
		d.peers[i].matched = true
	}
	const out = 64
	var effs []actor.Effect
	for i := range b.N + out {
		_, d, effs = bl.HandleEvent(leader, d, genstatem.Call{From: reader, Req: command{Cmd: 1}})
		_ = effs
		if i < out {
			continue
		}
		match := d.log.lastIndex() - out
		for _, p := range d.peers {
			reply := appendReply{Term: d.term, From: p.id, Success: true, Match: match}
			_, d, _ = bl.HandleEvent(leader, d, genstatem.Info{Msg: reply})
		}
	}
}

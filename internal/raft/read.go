package raft

import (
	"context"
	"fmt"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genstatem"
)

// Read is quorum.Read.
func Read[Rep, Cmd any](ctx context.Context, caller actor.Caller, group string, nodes []string, cmd Cmd) (Rep, error) {
	var zero Rep
	v, err := ask(ctx, caller, group, nodes, read{cmd})
	if err != nil {
		return zero, err
	}
	rep, _ := v.(applied).Rep.(Rep)
	return rep, nil
}

// reading is a read the leader is to answer, once a majority has answered
// its round seq of confirming reads, begun after the read came: it led
// then, so no later leader had committed more than it had. Applying what
// it commits at once, it holds all that was committed when the read came.
type reading struct {
	seq   uint64
	after uint64 // the entry of the time it reads at, applied first, if not 0
	cmd   any
	from  actor.From
}

// read handles a read. The leader begins a round of heartbeats to learn
// whether it still leads; another member tells where the leader is. A
// leader just elected has the read wait until its first entry is
// committed, before which it does not know what is.
func (b behaviour[S, Cmd, Rep]) read(r role, d *replica[S], c genstatem.Call, m read) (role, []actor.Effect) {
	if reason := b.unreadable(m.Cmd); reason != "" {
		return r, actor.Do(c.Reply(rejected{reason}))
	}
	switch {
	case r != leader:
		return r, actor.Do(c.Reply(redirect{Leader: d.leader.Node()}))
	case d.log.termAt(d.commit) != d.term:
		return r, actor.Do(c.Reply(busy{}))
	}
	d.readSeq++
	after, effs := b.timeForRead(d)
	d.reads = append(d.reads, reading{seq: d.readSeq, after: after, cmd: m.Cmd, from: c.From})
	effs = append(effs, b.replicate(d)...)
	effs = append(effs, b.commit(r, d)...) // the entry of its time, held alone and durable at once
	return r, append(effs, b.serveReads(d)...)
}

// timeForRead brings the time of the log to the leader's clock, for a
// read of a TimedReader, if behind it by more than Heartbeat: it appends
// an entry of its time, and returns its index, for the read to wait until
// it is applied; or the one on its way. A read at the time of the log,
// every command after it is applied at a time no earlier, whatever the
// clock of the leader then; at the clock of a leader ahead of the next,
// commands would go back in time from it.
func (b behaviour[S, Cmd, Rep]) timeForRead(d *replica[S]) (uint64, []actor.Effect) {
	if _, ok := b.spec.Machine.(TimedReader[S, Cmd, Rep]); !ok || b.now == nil {
		return 0, nil
	}
	if d.timeAt > d.applied {
		return d.timeAt, nil
	}
	if b.now().UnixNano()-d.sessions.Time <= int64(b.spec.Heartbeat) {
		return 0, nil
	}
	d.log = d.log.add(b.entry(d, Entry{}))
	d.timeAt = d.log.lastIndex()
	return d.timeAt, b.saveLog(d, d.timeAt)
}

// unreadable tells why the group cannot read cmd, if it cannot.
func (b behaviour[S, Cmd, Rep]) unreadable(cmd any) string {
	if !b.reads() {
		return fmt.Sprintf("quorum: the machine of group %s does not read", b.spec.Group)
	}
	if _, ok := cmd.(Cmd); !ok {
		var want Cmd
		return fmt.Sprintf("quorum: group %s takes commands of type %T, not %T", b.spec.Group, want, cmd)
	}
	return ""
}

// serveReads answers the reads the leader may now.
func (b behaviour[S, Cmd, Rep]) serveReads(d *replica[S]) []actor.Effect {
	if len(d.reads) == 0 {
		return nil
	}
	var effs []actor.Effect
	var waiting []reading
	for _, rd := range d.reads {
		if !d.confirmed(rd.seq) || rd.after > d.applied {
			waiting = append(waiting, rd)
			continue
		}
		rep := b.readMachine(d, rd.cmd.(Cmd))
		effs = append(effs, actor.Reply{To: rd.from, Value: applied{Rep: rep}})
	}
	d.reads = waiting
	return effs
}

// confirmed reports whether a majority of the voters answered the round
// seq of confirming reads, or a later one, the leader itself included.
func (d *replica[S]) confirmed(seq uint64) bool {
	count := 0
	if d.config.isVoter(d.id) {
		count++
	}
	for _, p := range d.peers {
		if d.config.isVoter(p.id) && p.read >= seq {
			count++
		}
	}
	return count >= d.config.majority()
}

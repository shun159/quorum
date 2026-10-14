package quorum

import (
	"context"
	"fmt"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genstatem"
)

// Read has group answer cmd from its state, as its Machine's Read does,
// without cmd going through the log: nothing is appended nor written. The
// answer is as fresh as a command's: it has every command applied before
// Read was called, as Raft's ReadIndex has it. The Machine must be a
// Reader.
//
// A read changes nothing: on an error, it may be tried again.
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
	seq  uint64
	cmd  any
	from actor.From
}

// read handles a read. The leader begins a round of heartbeats to learn
// whether it still leads; another member tells where the leader is. A
// leader just elected has the read wait until its first entry is
// committed, before which it does not know what is.
func (b behaviour[S, Cmd, Rep]) read(r role, d replica[S], c genstatem.Call, m read) (role, replica[S], []actor.Effect) {
	if reason := b.unreadable(m.Cmd); reason != "" {
		return r, d, actor.Do(c.Reply(rejected{reason}))
	}
	switch {
	case r != leader:
		return r, d, actor.Do(c.Reply(redirect{Leader: d.leader.Node()}))
	case d.log.termAt(d.commit) != d.term:
		return r, d, actor.Do(c.Reply(busy{}))
	}
	d.readSeq++
	d.reads = append(d.reads, reading{seq: d.readSeq, cmd: m.Cmd, from: c.From})
	effs := b.replicate(d)
	return r, d, append(effs, b.serveReads(&d)...)
}

// unreadable tells why the group cannot read cmd, if it cannot.
func (b behaviour[S, Cmd, Rep]) unreadable(cmd any) string {
	if _, ok := b.spec.Machine.(Reader[S, Cmd, Rep]); !ok {
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
	reader := b.spec.Machine.(Reader[S, Cmd, Rep])
	var effs []actor.Effect
	var waiting []reading
	for _, rd := range d.reads {
		if !d.confirmed(rd.seq) {
			waiting = append(waiting, rd)
			continue
		}
		rep := reader.Read(d.state, rd.cmd.(Cmd))
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

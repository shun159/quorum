package raft

import (
	"slices"
	"testing"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genstatem"
	"github.com/shun159/molecule/proc"
)

// A read answers what the commands before it made, and appends nothing.
func TestRead(t *testing.T) {
	g := newGroup(t, 1, three)
	for range 3 {
		if _, err := g.command(1); err != nil {
			t.Fatal(err)
		}
	}
	_, before, _ := g.member(g.leader())
	for range 5 {
		if v, err := g.ask(read{0}); err != nil || v.(applied).Rep != 3 {
			t.Fatalf("read: %#v, %v", v, err)
		}
	}
	if _, after, _ := g.member(g.leader()); after.log.lastIndex() != before.log.lastIndex() {
		t.Errorf("reads appended: %d entries, from %d", after.log.lastIndex(), before.log.lastIndex())
	}
}

// writeOnly is a counter that does not read.
type writeOnly struct{}

func (writeOnly) Init() int { return 0 }

func (writeOnly) Apply(sum, n int) (int, int, []actor.Effect) { return sum + n, sum + n, nil }

// A machine that does not read refuses reads.
func TestReadNotReader(t *testing.T) {
	g := newGroupOf(t, 1, three, func(string) Machine[int, int, int] { return writeOnly{} })
	g.run(time.Second)
	v, err := g.s.Call(g.at(g.leader()), read{0})
	if _, ok := v.(rejected); err != nil || !ok {
		t.Errorf("read: %#v, %v", v, err)
	}
}

// leading returns the member b as the leader of term 1, its entry of the
// term committed, holding 3, with a and c its peers.
func leading(t *testing.T) (behaviour[int, int, int], replica[int]) {
	t.Helper()
	b, d := member(t)
	d.term, d.leader = 1, idb
	d.log = d.log.add(Entry{Term: 1})
	d.logChanged()
	d.commit, d.applied, d.stable, d.state = 2, 2, 2, 3
	d.peers = []peer{{id: ida, next: 3, match: 2}, {id: idc, next: 3, match: 2}}
	return b, d
}

var reader = actor.From{PID: proc.NewNode("client").NewPID()}

// The leader answers a read once a majority answered a heartbeat sent
// after it came: before, another may lead, which committed more.
func TestReadConfirmed(t *testing.T) {
	b, d := leading(t)
	r, d, effs := b.HandleEvent(leader, d, genstatem.Call{From: reader, Req: read{0}})
	beats := sent(effs)
	if len(effs) != 2 || len(beats) != 2 || beats[0].(appendEntries).Read != 1 {
		t.Fatalf("read: %v", effs)
	}
	before := appendReply{Term: 1, From: ida, Success: true, Match: 2, Read: 0}
	if _, _, effs := b.HandleEvent(r, d, genstatem.Info{Msg: before}); len(effs) != 0 {
		t.Fatalf("answered on a heartbeat sent before the read: %v", effs)
	}
	after := appendReply{Term: 1, From: ida, Success: true, Match: 2, Read: 1}
	_, _, effs = b.HandleEvent(r, d, genstatem.Info{Msg: after})
	if len(effs) != 1 || effs[0] != (actor.Reply{To: reader, Value: applied{Rep: 3}}) {
		t.Errorf("confirmed: %v", effs)
	}
}

// A leader just elected has reads wait until its own entry is committed;
// one stepping down has them read again elsewhere.
func TestReadNewLeader(t *testing.T) {
	b, d := leading(t)
	d.log = d.log.add(Entry{Term: 2})
	d.logChanged()
	d.term = 2
	_, _, effs := b.HandleEvent(leader, d, genstatem.Call{From: reader, Req: read{0}})
	if len(effs) != 1 || effs[0] != (actor.Reply{To: reader, Value: busy{}}) {
		t.Errorf("before its entry is committed: %v", effs)
	}

	b, d = leading(t)
	_, d, _ = b.HandleEvent(leader, d, genstatem.Call{From: reader, Req: read{0}})
	if effs := b.surrender(&d); !slices.Contains(effs, actor.Effect(actor.Reply{To: reader, Value: redirect{}})) {
		t.Errorf("stepping down: %v", effs)
	}
}

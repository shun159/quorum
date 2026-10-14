package quorum

import (
	"slices"
	"testing"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genstatem"
	"github.com/shun159/molecule/proc"
)

// Members started blank, never founded, elect no one, and take no command.
func TestUnfounded(t *testing.T) {
	g := unfounded(t, 1, three, func(string) Machine[int, int, int] { return counter{} })
	g.run(2 * time.Second)
	if l, ok := g.anyLeader(); ok {
		t.Fatalf("%s leads a group never founded", l)
	}
	v, err := g.s.Call(g.at("a"), command{1})
	if err != nil || v != (redirect{}) {
		t.Errorf("command: %#v, %v", v, err)
	}
}

// A group is founded once; founding it again with the same configuration
// says so again, as when Found runs again after failing midway.
func TestFoundOnce(t *testing.T) {
	g := newGroup(t, 1, three)
	_, d, _ := g.member("a")
	if v, err := g.s.Call(g.at("a"), found{d.config}); err != nil || v != (applied{}) {
		t.Errorf("founding again alike: %#v, %v", v, err)
	}
	other := Config{Voters: []proc.PID{d.id}}
	if v, err := g.s.Call(g.at("a"), found{other}); err != nil || v == (applied{}) {
		t.Errorf("founding again otherwise: %#v, %v", v, err)
	}
	if _, err := g.command(1); err != nil {
		t.Fatal(err)
	}
	if v, err := g.s.Call(g.at("a"), found{d.config}); err != nil || v == (applied{}) {
		t.Errorf("founding a group with commands: %#v, %v", v, err)
	}
}

// A member restarted on a new disk is another member, not in the group:
// it neither votes nor stands, nor takes what is sent to the one before
// it, while the group goes on without it.
func TestNewDisk(t *testing.T) {
	g := newGroup(t, 1, three)
	for range 3 {
		if _, err := g.command(1); err != nil {
			t.Fatal(err)
		}
	}
	_, before, _ := g.member("c")
	g.loseDisk("c")
	g.restart("c")
	g.run(3 * time.Second)

	r, d, _ := g.member("c")
	if d.id == before.id || d.id.IsZero() {
		t.Fatalf("c is %v, as before: %v", d.id, before.id)
	}
	if r != follower || d.log.lastIndex() != 0 || !d.votedFor.IsZero() {
		t.Errorf("c, new: %v, log of %d, voted for %v", r, d.log.lastIndex(), d.votedFor)
	}
	if sum, err := g.command(1); err != nil || sum != 4 {
		t.Errorf("command without c: %d, %v", sum, err)
	}
}

// A member without a storage, restarted, is another member as well.
func TestRestartWithoutStorage(t *testing.T) {
	g := newGroup(t, 1, three)
	for _, m := range three {
		spec := g.specs[m]
		spec.Storage = nil
		g.specs[m] = spec
	}
	g.s.Crash("c")
	g.restart("c")
	g.run(2 * time.Second)
	if _, d, _ := g.member("c"); d.log.lastIndex() != 0 || d.config.has(d.id) {
		t.Errorf("c restarted without storage: log of %d, in %v", d.log.lastIndex(), d.config)
	}
}

// A member not a voter in its configuration refuses its vote, and does
// not stand.
func TestNonVoterRefuses(t *testing.T) {
	b, d := member(t)
	config := Config{Voters: []proc.PID{ida, idc}, Learners: []proc.PID{idb}}
	d.log, d.config = logOf(Entry{Config: &config}), config

	_, d2, effs := b.HandleEvent(follower, d, genstatem.Info{Msg: requestVote{Term: 1, Candidate: ida, LastIndex: 1}})
	_, _, effs = b.HandleEvent(follower, d2, genstatem.Info{Msg: memDurable{written(t, effs)}})
	if msgs := sent(effs); len(msgs) != 1 || msgs[0] != (voteReply{Term: 1, From: idb}) {
		t.Errorf("a learner told %v", msgs)
	}
	if r, _, _ := b.HandleEvent(follower, d, genstatem.StateTimeout{}); r != follower {
		t.Errorf("a learner stood: %v", r)
	}
}

// The swap of a node: its member lost with its disk, the node comes back
// blank; removed, then added again under the same name, the member catches
// up as a learner and votes again, as the one before it did, by the time
// it is added.
func TestNodeSwap(t *testing.T) {
	g := newGroup(t, 1, three)
	for range 3 {
		if _, err := g.command(1); err != nil {
			t.Fatal(err)
		}
	}
	g.loseDisk("c")
	g.restart("c")
	if _, err := g.ask(removeMember{"c"}); err != nil {
		t.Fatalf("remove c: %v", err)
	}
	newc := g.identify("c")
	if _, err := g.ask(addMember{newc}); err != nil {
		t.Fatalf("add c: %v", err)
	}
	// Added, c is a voter, by what the leader committed.
	_, ld, _ := g.member(g.leader())
	if !ld.log.configUpTo(ld.commit).isVoter(newc) {
		t.Fatalf("c added, but not a voter: %+v", ld.log.configUpTo(ld.commit))
	}
	g.run(2 * time.Second)
	if _, d, _ := g.member("c"); d.state != 3 {
		t.Fatalf("c holds %d, want 3", d.state)
	}
	// c counts again: with a gone, b and c are a majority.
	g.crash("a", 0)
	g.run(2 * time.Second)
	if sum, err := g.command(1); err != nil || sum != 4 {
		t.Errorf("command without a: %d, %v", sum, err)
	}
}

// A node with a member keeps it until it is removed: the member of a new
// disk is not added in its place.
func TestAddOverMember(t *testing.T) {
	g := newGroup(t, 1, three)
	g.loseDisk("c")
	g.restart("c")
	if _, err := g.ask(addMember{g.identify("c")}); err == nil {
		t.Error("added c over the member of c")
	}
}

// The leader removed steps down once the removal is committed, and the
// others go on.
func TestRemoveLeader(t *testing.T) {
	g := newGroup(t, 1, three)
	g.run(2 * time.Second)
	old := g.leader()
	if _, err := g.ask(removeMember{old}); err != nil {
		t.Fatalf("remove %s: %v", old, err)
	}
	g.run(2 * time.Second)
	if r, d, _ := g.member(old); r == leader || d.config.has(d.id) {
		t.Errorf("%s removed: %v, in %v", old, r, d.config)
	}
	l := g.leader()
	if l == old {
		t.Fatalf("leader %q after removing %s", l, old)
	}
	if sum, err := g.command(1); err != nil || sum != 1 {
		t.Errorf("command after: %d, %v", sum, err)
	}
}

// The last voter cannot be removed.
func TestRemoveLastVoter(t *testing.T) {
	g := newGroup(t, 1, []string{"a"})
	if _, err := g.ask(removeMember{"a"}); err == nil {
		t.Error("removed the last voter")
	}
}

// A change waits for the one before it to be committed.
func TestOneChangeAtATime(t *testing.T) {
	b, d := member(t)
	d.term, d.commit = 1, 1
	d.log = d.log.add(Entry{Term: 1, Config: &Config{Voters: []proc.PID{ida, idb, idc, proc.NewNode("d").NewPID()}}})
	d.logChanged() // a change appended, not committed
	call := genstatem.Call{Req: removeMember{"c"}}
	if _, _, effs := b.HandleEvent(leader, d, call); len(effs) != 1 || effs[0] != call.Reply(busy{}) {
		t.Errorf("a second change: %v", effs)
	}
}

// A candidate asks the learners for their votes too: one may be a voter by
// a later configuration, which the candidate does not have yet.
func TestStandAsksLearners(t *testing.T) {
	b, d := member(t)
	config := Config{Voters: []proc.PID{ida, idb}, Learners: []proc.PID{idc}}
	d.log, d.config = logOf(Entry{Config: &config}), config
	r, d, effs := b.stand(d)
	_, _, effs = b.HandleEvent(r, d, genstatem.Info{Msg: memDurable{written(t, effs)}})
	asked := map[string]bool{}
	for _, e := range effs {
		if s, ok := e.(actor.Send); ok {
			asked[s.To.(actor.Remote).Node] = true
		}
	}
	if !asked["a"] || !asked["c"] {
		t.Errorf("asked %v, want a and c", asked)
	}
}

// The leader replies to an addition once the member is a voter by the
// configuration committed, not only appended; to one removed since, that
// it was; and, stepping down, that it may be added or not.
func TestAddRepliesOnceVoter(t *testing.T) {
	_, d := member(t)
	idd := proc.NewNode("d").NewPID()
	from := actor.From{PID: ida}
	learner := Config{Voters: []proc.PID{ida, idb, idc}, Learners: []proc.PID{idd}}
	voter := learner.promoted(idd)
	d.term, d.commit = 1, 2
	d.log = d.log.add(Entry{Term: 1, Config: &learner}).add(Entry{Term: 1, Config: &voter})
	d.logChanged()
	d.joining = []joining{{id: idd, from: from}}

	if effs := d.joined(); len(effs) != 0 {
		t.Fatalf("replied while promoted, not committed: %v", effs)
	}
	d.commit = 3
	if effs := d.joined(); len(effs) != 1 || effs[0] != (actor.Reply{To: from, Value: applied{}}) || len(d.joining) != 0 {
		t.Fatalf("committed a voter: %v, %d waiting", effs, len(d.joining))
	}

	d.joining = []joining{{id: idd, from: from}}
	removed := voter.without(idd)
	d.log = d.log.add(Entry{Term: 1, Config: &removed})
	d.logChanged()
	d.commit = 2 // as if the voter were not committed, only the learner
	if effs := d.joined(); len(effs) != 1 || effs[0].(actor.Reply).Value == (applied{}) {
		t.Fatalf("removed: %v", effs)
	}

	d.joining = []joining{{id: idd, from: from}}
	b, _ := member(t)
	if effs := b.surrender(&d); !slices.Contains(effs, actor.Effect(actor.Reply{To: from, Value: deposed{}})) {
		t.Errorf("surrendered: %v", effs)
	}
}

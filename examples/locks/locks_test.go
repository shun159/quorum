package main

import (
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/application"
	"github.com/shun159/molecule/dist"
	"github.com/shun159/molecule/proc"
)

// The machine gives a lock to one owner at a time, for a lease its owner
// renews, then to another, with a larger token, once it expired; the
// holders of the nodes are told.
func TestMachine(t *testing.T) {
	m := machine{"locks/0"}
	l := m.Init()
	t0 := time.Unix(1000, 0)
	at := func(s int) time.Time { return t0.Add(time.Duration(s) * time.Second) }
	exp := func(s int) int64 { return at(s).UnixNano() }
	const lease = 10 * time.Second
	steps := []struct {
		op   Op
		at   int
		want Reply
		effs []actor.Effect
	}{
		{Op{Acquire, "printer", "a", lease}, 0, Reply{Holder: Holder{"a", 1, exp(10)}, OK: true},
			actor.Do(actor.Send{To: holderAt("a"), Msg: Granted{"printer", 1}})},
		{Op{Acquire, "printer", "b", lease}, 5, Reply{Holder: Holder{"a", 1, exp(10)}}, nil},
		{Op{Acquire, "printer", "a", lease}, 8, Reply{Holder: Holder{"a", 1, exp(18)}, OK: true}, nil}, // renewed
		{Op{Acquire, "printer", "b", lease}, 15, Reply{Holder: Holder{"a", 1, exp(18)}}, nil},
		{Op{Acquire, "printer", "b", lease}, 19, Reply{Holder: Holder{"b", 2, exp(29)}, OK: true},
			actor.Do(actor.Send{To: holderAt("a"), Msg: Lost{"printer", 1}}, actor.Send{To: holderAt("b"), Msg: Granted{"printer", 2}})},
		{Op{Kind: Release, Name: "printer", Owner: "a"}, 20, Reply{Holder: Holder{"b", 2, exp(29)}}, nil},
	}
	for i, s := range steps {
		var got Reply
		var effs []actor.Effect
		l, got, effs = m.ApplyAt(l, s.op, at(s.at))
		if got != s.want || !slices.Equal(effs, s.effs) {
			t.Fatalf("step %d, %+v: %+v, %v; want %+v, %v", i, s.op, got, effs, s.want, s.effs)
		}
	}
	who := Op{Kind: Query, Name: "printer"}
	if got := m.ReadAt(l, who, at(28)); got != (Reply{Holder: Holder{"b", 2, exp(29)}, OK: true}) {
		t.Errorf("read before the lease expires: %+v", got)
	}
	if got := m.ReadAt(l, who, at(29)); got != (Reply{}) {
		t.Errorf("read once it expired: %+v", got)
	}
	if got := m.ReadAt(l, Op{Kind: Count}, at(28)); got.Count != 1 {
		t.Errorf("count: %+v", got)
	}
	_, got, effs := m.ApplyAt(l, Op{Kind: Release, Name: "printer", Owner: "b"}, at(21))
	if !got.OK || !slices.Equal(effs, actor.Do(actor.Send{To: holderAt("b"), Msg: Released{"printer", 2}})) {
		t.Errorf("release: %+v, %v", got, effs)
	}
}

// cluster is the service on three nodes of this process, over dist on
// localhost, each keeping its log in a directory of its own.
type cluster struct {
	t     *testing.T
	svc   Service
	dir   string
	mu    sync.Mutex
	addrs map[string]string // where each node listens
	nodes map[string]*node
}

type node struct {
	lines lines // what its display printed
	n     *proc.Node
	d     *dist.Dist
	app   *application.Running
}

var names = []string{"a@localhost", "b@localhost", "c@localhost"}

// svc is the service of the tests: its locks over eight groups, for a
// lease longer than the tests.
var svc = Service{Nodes: names, Groups: 8, Lease: time.Minute}

func newCluster(t *testing.T, s Service) *cluster {
	c := &cluster{t: t, svc: s, dir: t.TempDir(), addrs: map[string]string{}, nodes: map[string]*node{}}
	for _, name := range names {
		c.start(name)
	}
	t.Cleanup(func() {
		for _, name := range names {
			c.stop(name)
		}
	})
	return c
}

// start starts the node name, on what it saved, if anything.
func (c *cluster) start(name string) {
	c.t.Helper()
	n := proc.NewNode(name)
	d, err := dist.Start(n, dist.Config{Listen: "127.0.0.1:0", Cookie: "locks", Resolve: c.resolve})
	if err != nil {
		c.t.Fatal(err)
	}
	c.mu.Lock()
	c.addrs[name] = d.Addr().String()
	c.mu.Unlock()
	ls := make(lines, 100)
	app, err := startLocks(context.Background(), n, c.svc, filepath.Join(c.dir, name), startDisplay(n, ls))
	if err != nil {
		c.t.Fatal(err)
	}
	c.nodes[name] = &node{lines: ls, n: n, d: d, app: app}
}

// stop stops the node name, if it runs.
func (c *cluster) stop(name string) {
	nd := c.nodes[name]
	if nd == nil {
		return
	}
	nd.app.Stop(context.Background())
	nd.d.Stop()
	delete(c.nodes, name)
}

func (c *cluster) resolve(node string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	a, ok := c.addrs[node]
	return a, ok
}

// lines are the lines a display printed.
type lines chan string

func (ls lines) Write(p []byte) (int, error) {
	ls <- strings.TrimSuffix(string(p), "\n")
	return len(p), nil
}

// allLed waits for every group to be led, as the console on n tells,
// by a node other than down.
func (c *cluster) allLed(n *proc.Node, down string) {
	c.t.Helper()
	var out strings.Builder
	for range 50 {
		out.Reset()
		console{n: n, svc: c.svc, out: &out}.groups()
		if strings.Count(out.String(), "led by") == c.svc.Groups && !strings.Contains(out.String(), "led by "+down) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	c.t.Fatalf("%s down, groups:\n%s", down, out.String())
}

// see waits for the display of the node name to print want.
func (c *cluster) see(name, want string) {
	c.t.Helper()
	timeout := time.After(10 * time.Second)
	for {
		select {
		case line := <-c.nodes[name].lines:
			if line == want {
				return
			}
		case <-timeout:
			c.t.Fatalf("%s did not show %q", name, want)
		}
	}
}

func ctx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// While a node is down, any of them, the leader of some groups as well,
// in turn, the others still know who holds a lock, and give others; every
// group is led, by the nodes up. The node of the owner of a lock shows it,
// held and released.
func TestLocks(t *testing.T) {
	c := newCluster(t, svc)
	a := c.nodes[names[0]].n
	if err := svc.Found(ctx(t), a); err != nil {
		t.Fatal(err)
	}
	r, err := svc.Lock(ctx(t), a, "printer", a.Name())
	if err != nil || !r.OK || r.Holder.Token != 1 {
		t.Fatalf("lock printer: %+v, %v", r, err)
	}
	c.see(a.Name(), `*** this node holds the lock "printer", token 1`)

	for i, down := range names {
		c.stop(down)
		up := c.nodes[names[(i+1)%len(names)]].n
		if r, err := svc.Who(ctx(t), up, "printer"); err != nil || r.Holder.Owner != a.Name() || r.Holder.Token != 1 {
			t.Fatalf("%s down: who holds printer: %+v, %v", down, r, err)
		}
		if r, err := svc.Lock(ctx(t), up, "printer", "bob"); err != nil || r.OK {
			t.Fatalf("%s down: bob locked printer: %+v, %v", down, r, err)
		}
		r, err := svc.Lock(ctx(t), up, "door "+down, up.Name())
		if err != nil || !r.OK {
			t.Fatalf("%s down: lock door: %+v, %v", down, r, err)
		}
		c.see(up.Name(), fmt.Sprintf("*** this node holds the lock \"door %s\", token %d", down, r.Holder.Token))
		c.allLed(up, down)
		c.start(down)
	}

	a = c.nodes[names[0]].n // started again
	r, err = svc.Lock(ctx(t), a, "scanner", a.Name())
	if err != nil || !r.OK {
		t.Fatalf("lock scanner: %+v, %v", r, err)
	}
	c.see(a.Name(), fmt.Sprintf("*** this node holds the lock \"scanner\", token %d", r.Holder.Token))
	if r, err := svc.Unlock(ctx(t), a, "scanner", a.Name()); err != nil || !r.OK {
		t.Fatalf("unlock scanner: %+v, %v", r, err)
	}
	c.see(a.Name(), `*** this node freed the lock "scanner"`)
}

// A lock lasts while the node of its owner renews its lease: once the
// node is down, it expires, and another node takes it, with a later token.
func TestLease(t *testing.T) {
	s := svc
	s.Lease = 600 * time.Millisecond
	c := newCluster(t, s)
	a, b := c.nodes[names[0]].n, c.nodes[names[1]].n
	if err := s.Found(ctx(t), a); err != nil {
		t.Fatal(err)
	}
	r, err := s.Lock(ctx(t), a, "printer", a.Name())
	if err != nil || !r.OK {
		t.Fatalf("lock: %+v, %v", r, err)
	}
	time.Sleep(4 * s.Lease) // renewed meanwhile
	if r, err := s.Lock(ctx(t), b, "printer", b.Name()); err != nil || r.OK || r.Holder.Owner != a.Name() {
		t.Fatalf("renewed, b locked: %+v, %v", r, err)
	}
	c.stop(names[0])
	time.Sleep(2 * s.Lease)
	r, err = s.Lock(ctx(t), b, "printer", b.Name())
	if err != nil || !r.OK || r.Holder.Token != 2 {
		t.Fatalf("expired, b locked: %+v, %v", r, err)
	}
	c.see(b.Name(), `*** this node holds the lock "printer", token 2`)
}

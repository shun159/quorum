package main

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/application"
	"github.com/shun159/molecule/dist"
	"github.com/shun159/molecule/proc"
)

// The machine gives a lock to one owner at a time, each time with a
// larger token.
func TestMachine(t *testing.T) {
	m := machine{"locks/0"}
	l := m.Init()
	steps := []struct {
		op   Op
		want Reply
	}{
		{Op{Acquire, "printer", "a"}, Reply{Holder: Holder{"a", 1}, OK: true}},
		{Op{Acquire, "printer", "b"}, Reply{Holder: Holder{"a", 1}, OK: false}},
		{Op{Acquire, "printer", "a"}, Reply{Holder: Holder{"a", 1}, OK: true}}, // again, the same token
		{Op{Release, "printer", "b"}, Reply{Holder: Holder{"a", 1}, OK: false}},
		{Op{Release, "printer", "a"}, Reply{OK: true}},
		{Op{Acquire, "printer", "b"}, Reply{Holder: Holder{"b", 2}, OK: true}},
	}
	for i, s := range steps {
		var got Reply
		l, got, _ = m.Apply(l, s.op)
		if got != s.want {
			t.Fatalf("step %d, %+v: %+v, want %+v", i, s.op, got, s.want)
		}
	}
	// The holder of the node of the owner is told, once.
	_, _, effs := m.Apply(l, Op{Release, "printer", "b"})
	if len(effs) != 1 || effs[0] != (actor.Send{To: holderAt("b"), Msg: Released{"printer", 2}}) {
		t.Errorf("release: %v", effs)
	}
	if _, _, effs := m.Apply(l, Op{Acquire, "printer", "b"}); len(effs) != 0 {
		t.Errorf("acquire again: %v", effs)
	}
	if got := m.Read(l, Op{Kind: Query, Name: "printer"}); got != (Reply{Holder: Holder{"b", 2}, OK: true}) {
		t.Errorf("read: %+v", got)
	}
}

// cluster is the service on three nodes of this process, over dist on
// localhost, each keeping its log in a directory of its own.
type cluster struct {
	t     *testing.T
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

// svc is the service of the tests: its locks over eight groups.
var svc = Service{Nodes: names, Groups: 8}

func newCluster(t *testing.T) *cluster {
	c := &cluster{t: t, dir: t.TempDir(), addrs: map[string]string{}, nodes: map[string]*node{}}
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
	app, err := startLocks(context.Background(), n, svc, filepath.Join(c.dir, name), startDisplay(n, ls))
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
		console{n: n, svc: svc, out: &out}.groups()
		if strings.Count(out.String(), "led by") == svc.Groups && !strings.Contains(out.String(), "led by "+down) {
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
	c := newCluster(t)
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
		if r, err := svc.Who(ctx(t), up, "printer"); err != nil || r.Holder != (Holder{a.Name(), 1}) {
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

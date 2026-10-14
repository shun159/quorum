package main

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/shun159/molecule/application"
	"github.com/shun159/molecule/dist"
	"github.com/shun159/molecule/proc"
	"github.com/shun159/quorum"
)

// The machine gives a lock to one owner at a time, each time with a
// larger token.
func TestMachine(t *testing.T) {
	m := machine{}
	l := m.Init()
	steps := []struct {
		op   Op
		want Reply
	}{
		{Op{Acquire, "printer", "a"}, Reply{Holder{"a", 1}, true}},
		{Op{Acquire, "printer", "b"}, Reply{Holder{"a", 1}, false}},
		{Op{Acquire, "printer", "a"}, Reply{Holder{"a", 1}, true}}, // again, the same token
		{Op{Release, "printer", "b"}, Reply{Holder{"a", 1}, false}},
		{Op{Release, "printer", "a"}, Reply{OK: true}},
		{Op{Acquire, "printer", "b"}, Reply{Holder{"b", 2}, true}},
	}
	for i, s := range steps {
		var got Reply
		l, got, _ = m.Apply(l, s.op)
		if got != s.want {
			t.Fatalf("step %d, %+v: %+v, want %+v", i, s.op, got, s.want)
		}
	}
	if got := m.Read(l, Op{Kind: Query, Name: "printer"}); got != (Reply{Holder{"b", 2}, true}) {
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
	n   *proc.Node
	d   *dist.Dist
	app *application.Running
}

var names = []string{"a@localhost", "b@localhost", "c@localhost"}

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
	app, err := startLocks(context.Background(), n, filepath.Join(c.dir, name))
	if err != nil {
		c.t.Fatal(err)
	}
	c.nodes[name] = &node{n: n, d: d, app: app}
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

func ctx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// TestLocks runs the service on three nodes: with any one of them down,
// the leader as well, in turn, the others still know who holds a lock,
// and give others, with tokens that grow.
func TestLocks(t *testing.T) {
	c := newCluster(t)
	a := c.nodes[names[0]].n
	if err := quorum.Found(ctx(t), a, group, names); err != nil {
		t.Fatal(err)
	}
	r, err := Lock(ctx(t), a, names, "printer", "alice")
	if err != nil || !r.OK || r.Holder.Token != 1 {
		t.Fatalf("lock printer: %+v, %v", r, err)
	}

	last := r.Holder.Token
	for i, down := range names {
		c.stop(down)
		up := c.nodes[names[(i+1)%len(names)]].n
		if r, err := Who(ctx(t), up, names, "printer"); err != nil || r.Holder != (Holder{"alice", 1}) {
			t.Fatalf("%s down: who holds printer: %+v, %v", down, r, err)
		}
		if r, err := Lock(ctx(t), up, names, "printer", "bob"); err != nil || r.OK {
			t.Fatalf("%s down: bob locked printer: %+v, %v", down, r, err)
		}
		r, err := Lock(ctx(t), up, names, "door "+down, "bob")
		if err != nil || !r.OK || r.Holder.Token <= last {
			t.Fatalf("%s down: lock door: %+v, %v, after token %d", down, r, err, last)
		}
		last = r.Holder.Token
		c.start(down)
	}
}

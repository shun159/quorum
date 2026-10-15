package main

import (
	"context"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/shun159/molecule/application"
	"github.com/shun159/molecule/dist"
	"github.com/shun159/molecule/proc"
	"github.com/shun159/quorum"
)

// A topic keeps its latest messages, by offset; a fetch gives those from
// an offset on, from the oldest kept if that one is gone; a commit only
// moves a subscriber forward.
func TestMachine(t *testing.T) {
	m := machine{keep: 3}
	s := m.Init()
	var r Reply
	for i, text := range []string{"a", "b", "c", "d"} {
		if s, r, _ = m.Apply(s, Op{Kind: Publish, Topic: "news", Text: text, From: "n"}); r.Offset != uint64(i) {
			t.Fatalf("publish %s: offset %d, want %d", text, r.Offset, i)
		}
	}
	texts := func(r Reply) []string {
		var ts []string
		for _, msg := range r.Msgs {
			ts = append(ts, msg.Text)
		}
		return ts
	}
	if r := m.Read(s, Op{Kind: Fetch, Topic: "news", Offset: 0, Max: 10}); !slices.Equal(texts(r), []string{"b", "c", "d"}) || r.Offset != 4 {
		t.Errorf("fetch from 0, a gone: %q, next %d", texts(r), r.Offset)
	}
	if r := m.Read(s, Op{Kind: Fetch, Topic: "news", Offset: 2, Max: 1}); !slices.Equal(texts(r), []string{"c"}) || r.Offset != 3 {
		t.Errorf("fetch 1 from 2: %q, next %d", texts(r), r.Offset)
	}
	if r := m.Read(s, Op{Kind: Fetch, Topic: "none", Max: 10}); len(r.Msgs) != 0 || r.Offset != 0 {
		t.Errorf("fetch of no topic: %+v", r)
	}

	where := Op{Kind: Where, Sub: "x", Topic: "news"}
	s, _, _ = m.Apply(s, Op{Kind: Commit, Sub: "x", Topic: "news", Offset: 3})
	s, _, _ = m.Apply(s, Op{Kind: Commit, Sub: "x", Topic: "news", Offset: 2}) // late, from before
	if r := m.Read(s, where); r.Offset != 3 {
		t.Errorf("x read to %d, want 3", r.Offset)
	}
	s, _, _ = m.Apply(s, Op{Kind: Publish, Topic: "alerts", Text: "z"})
	if r := m.Read(s, Op{Kind: List}); !slices.Equal(r.Topics, []string{"alerts 0..1", "news 1..4"}) {
		t.Errorf("topics %q", r.Topics)
	}
}

var names = []string{"a@localhost", "b@localhost", "c@localhost"}

// cluster is the service on three nodes of this process, over dist on
// localhost, each keeping its log in a directory of its own.
type cluster struct {
	t     *testing.T
	svc   Service
	mu    sync.Mutex
	addrs map[string]string
	nodes map[string]*node
}

type node struct {
	n   *proc.Node
	d   *dist.Dist
	app *application.Running
}

func newCluster(t *testing.T) *cluster {
	c := &cluster{t: t, svc: Service{Nodes: names}, addrs: map[string]string{}, nodes: map[string]*node{}}
	dir := t.TempDir()
	for _, name := range names {
		n := proc.NewNode(name)
		d, err := dist.Start(n, dist.Config{Listen: "127.0.0.1:0", Cookie: "pubsub", Resolve: c.resolve})
		if err != nil {
			t.Fatal(err)
		}
		c.mu.Lock()
		c.addrs[name] = d.Addr().String()
		c.mu.Unlock()
		app, err := startPubsub(context.Background(), n, c.svc, filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		c.nodes[name] = &node{n: n, d: d, app: app}
	}
	t.Cleanup(func() {
		for name := range c.nodes {
			c.stop(name)
		}
	})
	return c
}

func (c *cluster) stop(name string) {
	nd := c.nodes[name]
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

// display returns a process of n passing what it gets to a channel.
func display(n *proc.Node) (proc.PID, chan string) {
	got := make(chan string, 100)
	pid := n.Spawn(func(s *proc.Self) error {
		for {
			msg, err := s.Receive(context.Background())
			if err != nil {
				return err
			}
			got <- msg.(Message).Text
		}
	})
	return pid, got
}

// A subscriber reads what is published, in order; stopped and started
// again by its name, it reads on from where it committed, what was
// published meanwhile included, nothing again; while a node is down, the
// others take what is published and the subscriber reads it.
func TestSubscriber(t *testing.T) {
	c := newCluster(t)
	a, b, cn := c.nodes[names[0]].n, c.nodes[names[1]].n, c.nodes[names[2]].n
	if err := quorum.Found(ctx(t), a, group, names); err != nil {
		t.Fatal(err)
	}
	out, got := display(b)
	want := func(texts ...string) {
		t.Helper()
		for _, text := range texts {
			select {
			case g := <-got:
				if g != text {
					t.Fatalf("read %q, want %q", g, text)
				}
			case <-time.After(10 * time.Second):
				t.Fatalf("did not read %q", text)
			}
		}
	}
	sub, err := Subscribe(ctx(t), b, c.svc, "reader", "news", out)
	if err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{"one", "two"} {
		if _, err := c.svc.Publish(ctx(t), a, a.Name(), "news", text); err != nil {
			t.Fatal(err)
		}
	}
	want("one", "two")

	// Its commit in, it stops; started again, it reads on.
	for {
		r, err := quorum.Read[Reply](ctx(t), b, group, names, Op{Kind: Where, Sub: "reader", Topic: "news"})
		if err == nil && r.Offset == 2 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err := Unsubscribe(ctx(t), b, sub); err != nil {
		t.Fatal(err)
	}
	if _, err := c.svc.Publish(ctx(t), a, a.Name(), "news", "while away"); err != nil {
		t.Fatal(err)
	}
	if _, err := Subscribe(ctx(t), b, c.svc, "reader", "news", out); err != nil {
		t.Fatal(err)
	}
	want("while away")

	// a gone, the others lead: a read, safe to ask again, tells, before
	// a publish, which would be applied twice if asked again.
	c.stop(names[0])
	for {
		rctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := c.svc.Topics(rctx, cn)
		cancel()
		if err == nil {
			break
		}
	}
	if _, err := c.svc.Publish(ctx(t), cn, cn.Name(), "news", "a gone"); err != nil {
		t.Fatal(err)
	}
	want("a gone")
	select {
	case g := <-got:
		t.Errorf("read %q again", g)
	case <-time.After(500 * time.Millisecond):
	}
}

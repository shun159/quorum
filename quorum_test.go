package quorum_test

import (
	"context"
	"fmt"
	"maps"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/behaviours/supervisor"
	"github.com/shun159/molecule/dist"
	"github.com/shun159/molecule/proc"
	"github.com/shun159/quorum"
)

// counter sums the numbers it is given, and reads the sum.
type counter struct{}

func (counter) Init() int { return 0 }

func (counter) Apply(sum, n int) (int, int, []actor.Effect) { return sum + n, sum + n, nil }

func (counter) Read(sum, _ int) int { return sum }

const group = "counter"

var names = []string{"a@localhost", "b@localhost", "c@localhost"}

// cluster is a node for each of names, in this process, connected by
// dist: on each, the storage and the members of the groups.
type cluster struct {
	t      *testing.T
	groups []string
	dir    string
	mu     sync.Mutex
	addrs  map[string]string
	nodes  map[string]*node
}

type node struct {
	n   *proc.Node
	d   *dist.Dist
	sup proc.PID
}

func newCluster(t *testing.T, groups ...string) *cluster {
	c := &cluster{t: t, groups: groups, dir: t.TempDir(), addrs: map[string]string{}, nodes: map[string]*node{}}
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
	d, err := dist.Start(n, dist.Config{Listen: "127.0.0.1:0", Cookie: "quorum", Resolve: c.resolve})
	if err != nil {
		c.t.Fatal(err)
	}
	c.mu.Lock()
	c.addrs[name] = d.Addr().String()
	c.mu.Unlock()
	dir := filepath.Join(c.dir, name)
	children := []supervisor.ChildSpec{{ID: "storage", Start: quorum.Storage(dir)}}
	for _, g := range c.groups {
		children = append(children, supervisor.ChildSpec{ID: g, Start: quorum.Child(quorum.Spec[int, int, int]{Group: g, Machine: counter{}, Dir: dir})})
	}
	sup, err := supervisor.Start(context.Background(), n, supervisor.Spec{Strategy: supervisor.OneForAll, Children: children})
	if err != nil {
		c.t.Fatal(err)
	}
	c.nodes[name] = &node{n: n, d: d, sup: sup}
}

// stop stops the node name, if it runs.
func (c *cluster) stop(name string) {
	nd := c.nodes[name]
	if nd == nil {
		return
	}
	supervisor.Stop(context.Background(), nd.n, nd.sup)
	nd.d.Stop()
	delete(c.nodes, name)
}

func (c *cluster) resolve(node string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	a, ok := c.addrs[node]
	return a, ok
}

// adder adds through a Client, and replies with what came of it.
type adder struct {
	genserver.Default[adding]
}

type adding struct {
	c       quorum.Client[int, int]
	tags    int
	waiting map[int]genserver.From[quorum.Result[int]]
}

// add is the number to add, or, if read, the sum to read.
type add struct {
	N    int
	Read bool
}

func (adder) Init(proc.PID) (adding, []actor.Effect, error) {
	return adding{c: quorum.NewClient[int, int](group, names, time.Second)}, nil, nil
}

func (adder) HandleCall(st adding, req add, from genserver.From[quorum.Result[int]]) (adding, []actor.Effect) {
	st.tags++
	var effs []actor.Effect
	if req.Read {
		st.c, effs = st.c.Read(st.tags, 0)
	} else {
		st.c, effs = st.c.Command(st.tags, req.N)
	}
	st.waiting = maps.Clone(st.waiting)
	if st.waiting == nil {
		st.waiting = map[int]genserver.From[quorum.Result[int]]{}
	}
	st.waiting[st.tags] = from
	return st, effs
}

func (adder) HandleInfo(st adding, msg any) (adding, []actor.Effect) {
	var effs []actor.Effect
	var res *quorum.Result[int]
	st.c, effs, res = st.c.Handle(msg)
	if res == nil {
		return st, effs
	}
	from := st.waiting[res.Tag.(int)]
	st.waiting = maps.Clone(st.waiting)
	delete(st.waiting, res.Tag.(int))
	return st, append(effs, from.Reply(*res))
}

func ctx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// Through the API only: a group founded on three nodes, each keeping its
// member in its storage, adds what a Client sends, while each node in
// turn is stopped and started again from its disk.
func TestQuorum(t *testing.T) {
	c := newCluster(t, group)
	a := c.nodes[names[0]].n
	if err := quorum.Found(ctx(t), a, group, names); err != nil {
		t.Fatal(err)
	}
	client, err := genserver.Start[adding, add, quorum.Result[int], genserver.None](ctx(t), a, adder{})
	if err != nil {
		t.Fatal(err)
	}
	sum := 0
	addOne := func(when string) {
		t.Helper()
		sum++
		if r, err := client.Call(ctx(t), a, add{N: 1}); err != nil || r.Err != nil || r.Rep != sum {
			t.Fatalf("%s: add: %+v, %v, want %d", when, r, err, sum)
		}
	}
	addOne("all up")

	for _, down := range names[1:] { // a hosts the client
		c.stop(down)
		addOne(down + " down")
		if r, err := client.Call(ctx(t), a, add{Read: true}); err != nil || r.Err != nil || r.Rep != sum {
			t.Fatalf("%s down: read: %+v, %v, want %d", down, r, err, sum)
		}
		c.start(down)
	}
	addOne("all back")
	for _, name := range names {
		if got, err := quorum.Read[int](ctx(t), c.nodes[name].n, group, names, 0); err != nil || got != sum {
			t.Errorf("read on %s: %d, %v, want %d", name, got, err, sum)
		}
	}
}

// Many groups keep their members in the one storage of each node: each
// node stopped and started again in turn, every group goes on, with all
// it applied.
func TestManyGroups(t *testing.T) {
	const n = 300
	var groups []string
	for i := range n {
		groups = append(groups, fmt.Sprintf("g%d", i))
	}
	c := newCluster(t, groups...)
	a := c.nodes[names[0]].n
	each(t, groups, func(g string) error { return quorum.Found(ctx(t), a, g, names) })
	add := func(g string) error {
		_, err := quorum.Command[int](ctx(t), a, g, names, 1)
		return err
	}
	for _, down := range names[1:] { // a asks
		each(t, groups, add)
		c.stop(down)
		each(t, groups, add)
		c.start(down)
	}
	for _, name := range names {
		each(t, groups, func(g string) error {
			if sum, err := quorum.Read[int](ctx(t), c.nodes[name].n, g, names, 0); err != nil || sum != 4 {
				return fmt.Errorf("read through %s: %d, %v", name, sum, err)
			}
			return nil
		})
	}
}

// each runs f on every group, a few at once, and fails the test on the
// first error.
func each(t *testing.T, groups []string, f func(g string) error) {
	t.Helper()
	errs := make(chan error, len(groups))
	sem := make(chan struct{}, 16)
	for _, g := range groups {
		sem <- struct{}{}
		go func() {
			defer func() { <-sem }()
			if err := f(g); err != nil {
				errs <- fmt.Errorf("%s: %w", g, err)
				return
			}
			errs <- nil
		}()
	}
	for range groups {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
}

// The lead moves to each node in turn, over dist, the group taking
// commands after each move.
func TestTransferLeader(t *testing.T) {
	c := newCluster(t, group)
	a := c.nodes[names[0]].n
	if err := quorum.Found(ctx(t), a, group, names); err != nil {
		t.Fatal(err)
	}
	for i, to := range append(slices.Clone(names), names...) {
		if err := quorum.TransferLeader(ctx(t), a, group, names, to); err != nil {
			t.Fatalf("to %s: %v", to, err)
		}
		if sum, err := quorum.Command[int](ctx(t), a, group, names, 1); err != nil || sum != i+1 {
			t.Fatalf("after moving to %s: %d, %v", to, sum, err)
		}
	}
}

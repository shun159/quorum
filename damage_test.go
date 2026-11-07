package quorum_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/shun159/quorum"
	"github.com/shun159/quorum/internal/kvsstorage"
	"github.com/shun159/quorum/internal/raft"
)

// damage stops the node name and changes a byte in the middle of the
// largest segment of its table, as a disk going bad may: started again,
// the node fails with ErrCorrupt.
func (c *cluster) damage(name string) {
	c.t.Helper()
	c.stop(name)
	segs, _ := filepath.Glob(filepath.Join(c.dir, name, "*.log"))
	slices.SortFunc(segs, func(a, b string) int { return int(size(a) - size(b)) })
	if len(segs) == 0 {
		c.t.Fatalf("%s has no segment", name)
	}
	b, err := os.ReadFile(segs[len(segs)-1])
	if err != nil {
		c.t.Fatal(err)
	}
	b[len(b)/2] ^= 0xff
	if err := os.WriteFile(segs[len(segs)-1], b, 0o644); err != nil {
		c.t.Fatal(err)
	}
	if err := c.tryStart(name); !errors.Is(err, quorum.ErrCorrupt) {
		c.t.Fatalf("%s started on a damaged table: %v", name, err)
	}
}

// awaitRuns waits for the member of group to run on node, as the Groups
// of the node start it after the node starts.
func (c *cluster) awaitRuns(group, node string) {
	c.t.Helper()
	for deadline := time.Now().Add(10 * time.Second); !slices.Contains(c.runs(group), node); {
		if time.Now().After(deadline) {
			c.t.Fatalf("%s runs on %v only", group, c.runs(group))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func size(name string) int64 {
	fi, err := os.Stat(name)
	if err != nil {
		return 0
	}
	return fi.Size()
}

// blank moves the storage of the node name aside, as the package doc
// says, and starts the node on a blank one.
func (c *cluster) blank(name string) {
	c.t.Helper()
	dir := filepath.Join(c.dir, name)
	if err := os.Rename(dir, dir+".damaged"); err != nil {
		c.t.Fatal(err)
	}
	c.start(name)
}

// A node whose table is damaged fails to start; on a blank one, its
// groups started and their members replaced, as the package doc says, it
// takes part in every group again, with all it applied.
func TestDamagedStorage(t *testing.T) {
	c := newDynamic(t, 0)
	a := c.nodes[names[0]].n
	groups := []string{"x", "y"}
	for _, g := range groups {
		if err := quorum.CreateGroup(ctx(t), a, g, names); err != nil {
			t.Fatal(err)
		}
		for n := range 20 {
			if _, err := raise(ctx(t), a, g, n); err != nil {
				t.Fatal(err)
			}
		}
	}

	cc := names[2]
	c.damage(cc)
	c.blank(cc)
	for _, g := range groups {
		if err := quorum.StartMember(ctx(t), a, g, cc); err != nil {
			t.Fatal(err)
		}
		if err := quorum.ReplaceMember(ctx(t), a, g, names, cc); err != nil {
			t.Fatal(err)
		}
	}

	// c counts: with a gone, b and c are a majority, and c holds 19.
	c.stop(names[0])
	b := c.nodes[names[1]].n
	for _, g := range groups {
		if h, err := raise(ctx(t), b, g, 20); err != nil || h != 20 {
			t.Fatalf("%s, a down: %d, %v", g, h, err)
		}
		if h, err := quorum.Read[int](ctx(t), c.nodes[cc].n, g, names[1:], 0); err != nil || h != 20 {
			t.Errorf("%s: read on c: %d, %v, want 20", g, h, err)
		}
	}
}

// With a majority damaged, ReplaceMember waits, and the group with it:
// what the one member left saved, it keeps.
func TestDamagedMajority(t *testing.T) {
	c := newCluster(t, highest{}, group)
	a := c.nodes[names[0]].n
	if err := quorum.Found(ctx(t), a, group, names); err != nil {
		t.Fatal(err)
	}
	for n := range 20 {
		if _, err := raise(ctx(t), a, group, n); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range names[1:] {
		c.damage(name)
		c.blank(name)
	}
	short, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := quorum.ReplaceMember(short, a, group, names, names[2]); err == nil {
		t.Fatal("replaced c, with b and c damaged")
	}

	c.stop(names[0])
	table, err := kvsstorage.Open(filepath.Join(c.dir, names[0]))
	if err != nil {
		t.Fatal(err)
	}
	defer table.Close()
	saved, err := kvsstorage.For(table, group).Load()
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(saved.Log, func(e raft.Entry) bool { return e.Cmd == 19 }) {
		t.Errorf("a lost 19: %d entries", len(saved.Log))
	}
}

// Under Groups, a node whose snapshot of x is damaged starts its member of
// y, not of x, which DeleteMember, StartMember and ReplaceMember put back
// alone, as the package doc says.
func TestDamagedSnapshot(t *testing.T) {
	c := newDynamic(t, 5)
	a := c.nodes[names[0]].n
	groups := []string{"x", "y"}
	for _, g := range groups {
		if err := quorum.CreateGroup(ctx(t), a, g, names); err != nil {
			t.Fatal(err)
		}
		for n := range 20 {
			if _, err := raise(ctx(t), a, g, n); err != nil {
				t.Fatal(err)
			}
		}
	}
	cc := names[2]
	c.stop(cc)
	snaps, _ := filepath.Glob(filepath.Join(c.dir, cc, "snapshots", "x", "*.snap"))
	if len(snaps) == 0 {
		t.Fatal("c has no snapshot of x")
	}
	for _, snap := range snaps {
		if err := os.WriteFile(snap, []byte("damaged"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	c.start(cc)
	c.awaitRuns("y", cc)
	if on := c.runs("x"); slices.Contains(on, cc) {
		t.Errorf("x runs on %v, its snapshot on c damaged", on)
	}
	if err := quorum.StartMember(ctx(t), a, "x", cc); !errors.Is(err, quorum.ErrCorrupt) {
		t.Errorf("start x on c: %v, want ErrCorrupt", err)
	}

	for _, step := range []func(context.Context) error{
		func(ctx context.Context) error { return quorum.DeleteMember(ctx, a, "x", cc) },
		func(ctx context.Context) error { return quorum.StartMember(ctx, a, "x", cc) },
		func(ctx context.Context) error { return quorum.ReplaceMember(ctx, a, "x", names, cc) },
	} {
		if err := step(ctx(t)); err != nil {
			t.Fatal(err)
		}
	}
	// c counts in both: with a gone, b and c are a majority.
	c.stop(names[0])
	b := c.nodes[names[1]].n
	for _, g := range groups {
		if h, err := raise(ctx(t), b, g, 20); err != nil || h != 20 {
			t.Errorf("%s, a down: %d, %v", g, h, err)
		}
	}
}

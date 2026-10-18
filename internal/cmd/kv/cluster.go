package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/shun159/molecule/behaviours/supervisor"
	"github.com/shun159/molecule/dist"
	"github.com/shun159/molecule/proc"
	"github.com/shun159/quorum"
	"github.com/shun159/quorum/internal/cmd/nodes"
)

var names = []string{"a@localhost", "b@localhost", "c@localhost"}

const (
	group   = "kv"
	cookie  = "kv"
	control = "measure@localhost"
)

// cluster is the three nodes, each a process, with its storage under dir.
type cluster struct {
	dir   string
	addrs map[string]string
	procs map[string]*exec.Cmd
}

func startCluster(dir string) (*cluster, error) {
	c := &cluster{dir: dir, addrs: map[string]string{control: nodes.FreeAddr()}, procs: map[string]*exec.Cmd{}}
	for _, name := range names {
		c.addrs[name] = nodes.FreeAddr()
	}
	return c, c.start()
}

// start starts the nodes, and returns once they are ready.
func (c *cluster) start() error {
	for _, name := range names {
		p, err := nodes.Start(name, c.addrs, "-dir", filepath.Join(c.dir, name))
		if err != nil {
			return err
		}
		c.procs[name] = p
	}
	return nil
}

// kill kills the nodes, as a power cut would.
func (c *cluster) kill() {
	for _, p := range c.procs {
		p.Process.Kill()
		p.Wait()
	}
}

// memory tells the resident memory of each node.
func (c *cluster) memory() string {
	var mem []string
	for _, name := range names {
		b, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", c.procs[name].Process.Pid))
		if err != nil {
			continue
		}
		for line := range strings.Lines(string(b)) {
			if v, ok := strings.CutPrefix(line, "VmRSS:"); ok {
				mem = append(mem, name[:1]+" "+strings.TrimSpace(v))
			}
		}
	}
	return strings.Join(mem, ", ")
}

// runNode runs the node name, with its storage in dir and its member of
// the group, until killed.
func runNode(name string, addrs map[string]string, dir string) {
	n := proc.NewNode(name)
	if _, err := dist.Start(n, dist.Config{Listen: addrs[name], Cookie: cookie, Resolve: nodes.Resolver(addrs)}); err != nil {
		log.Fatal(err)
	}
	spec := quorum.Spec[state, Put, int]{Group: group, Machine: kv{}, Dir: dir}
	_, err := supervisor.Start(context.Background(), n, supervisor.Spec{
		Strategy: supervisor.OneForAll,
		Children: []supervisor.ChildSpec{
			{ID: "storage", Start: quorum.Storage(dir)},
			{ID: "member", Start: quorum.Child(spec)},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	nodes.Ready()
	select {}
}

package main

import (
	"fmt"
	"github.com/shun159/quorum/internal/cmd/nodes"
	"os/exec"
	"path/filepath"
	"syscall"
)

// cluster is the nodes, each a process, and the links between them.
type cluster struct {
	names []string
	addrs map[string]string            // where each node listens, and the control node
	finds map[string]map[string]string // where each node finds the others: proxies
	dirs  map[string]string
	procs map[string]*exec.Cmd
	links map[[2]string]*link // by the names of the two nodes, in order
}

// startCluster starts count nodes, keeping their storage under dir.
func startCluster(count int, dir string) (*cluster, error) {
	c := &cluster{addrs: map[string]string{control: nodes.FreeAddr()}, finds: map[string]map[string]string{}, dirs: map[string]string{},
		procs: map[string]*exec.Cmd{}, links: map[[2]string]*link{}}
	for i := range count {
		name := fmt.Sprintf("%c@localhost", 'a'+i)
		c.names = append(c.names, name)
		c.addrs[name] = nodes.FreeAddr()
		c.dirs[name] = filepath.Join(dir, name)
	}
	for i, x := range c.names {
		for _, y := range c.names[i+1:] {
			c.links[[2]string{x, y}] = &link{}
		}
	}
	for _, x := range c.names {
		c.finds[x] = map[string]string{x: c.addrs[x], control: c.addrs[control]}
		for _, y := range c.names {
			if y == x {
				continue
			}
			addr, err := c.link(x, y).proxy(c.addrs[y])
			if err != nil {
				c.stop()
				return nil, err
			}
			c.finds[x][y] = addr
		}
	}
	for _, name := range c.names {
		if err := c.start(name); err != nil {
			c.stop()
			return nil, err
		}
	}
	return c, nil
}

// link returns the link between the nodes x and y.
func (c *cluster) link(x, y string) *link {
	if x > y {
		x, y = y, x
	}
	return c.links[[2]string{x, y}]
}

// start starts the node name, and returns once it is ready.
func (c *cluster) start(name string) error {
	p, err := nodes.Start(name, c.finds[name], "-dir", c.dirs[name])
	if err != nil {
		return err
	}
	c.procs[name] = p
	return nil
}

// kill kills the node name; start starts it again, on the same storage.
func (c *cluster) kill(name string) {
	p := c.procs[name]
	p.Process.Signal(syscall.SIGKILL)
	p.Wait()
	delete(c.procs, name)
}

// pause stops the process of the node name, or has it go on.
func (c *cluster) pause(name string, paused bool) {
	sig := syscall.SIGCONT
	if paused {
		sig = syscall.SIGSTOP
	}
	if p, ok := c.procs[name]; ok {
		p.Process.Signal(sig)
	}
}

// heal has every node go on, and every link carry.
func (c *cluster) heal() {
	for name := range c.procs {
		c.pause(name, false)
	}
	for _, l := range c.links {
		l.set(carrying)
	}
}

func (c *cluster) stop() {
	for _, p := range c.procs {
		p.Process.Signal(syscall.SIGCONT)
		p.Process.Kill()
		p.Wait()
	}
	for _, l := range c.links {
		l.close()
	}
}

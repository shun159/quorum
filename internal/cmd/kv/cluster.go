package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/shun159/molecule/behaviours/supervisor"
	"github.com/shun159/molecule/dist"
	"github.com/shun159/molecule/proc"
	"github.com/shun159/quorum"
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
	c := &cluster{dir: dir, addrs: map[string]string{control: freeAddr()}, procs: map[string]*exec.Cmd{}}
	for _, name := range names {
		c.addrs[name] = freeAddr()
	}
	return c, c.start()
}

// start starts the nodes, and returns once they are ready.
func (c *cluster) start() error {
	self, err := os.Executable()
	if err != nil {
		return err
	}
	for _, name := range names {
		p := exec.Command(self, "-node", name, "-addrs", formatAddrs(c.addrs), "-dir", filepath.Join(c.dir, name))
		p.Stderr = os.Stderr
		out, err := p.StdoutPipe()
		if err != nil {
			return err
		}
		if err := p.Start(); err != nil {
			return err
		}
		c.procs[name] = p
		if line, _ := bufio.NewReader(out).ReadString('\n'); line != "ready\n" {
			return fmt.Errorf("%s: %q", name, line)
		}
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
	if _, err := dist.Start(n, dist.Config{Listen: addrs[name], Cookie: cookie, Resolve: resolver(addrs)}); err != nil {
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
	fmt.Println("ready")
	select {}
}

func freeAddr() string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

func resolver(addrs map[string]string) func(string) (string, bool) {
	return func(node string) (string, bool) { a, ok := addrs[node]; return a, ok }
}

func parseAddrs(s string) map[string]string {
	addrs := map[string]string{}
	for _, pair := range strings.Split(s, ",") {
		name, addr, _ := strings.Cut(pair, "=")
		addrs[name] = addr
	}
	return addrs
}

func formatAddrs(addrs map[string]string) string {
	var pairs []string
	for name, addr := range addrs {
		pairs = append(pairs, name+"="+addr)
	}
	return strings.Join(pairs, ",")
}

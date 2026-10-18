// Package nodes runs the nodes of a measure, each a process of its own:
// the measure starts its own program again for each node, with -node and
// the addresses of all, and the node says it is ready once it runs.
package nodes

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"strings"
)

// Start starts the program running again as the node name, with -node
// name, -addrs and args, and returns once the node says it is ready.
func Start(name string, addrs map[string]string, args ...string) (*exec.Cmd, error) {
	self, err := os.Executable()
	if err != nil {
		return nil, err
	}
	p := exec.Command(self, append([]string{"-node", name, "-addrs", Format(addrs)}, args...)...)
	p.Stderr = os.Stderr
	out, err := p.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := p.Start(); err != nil {
		return nil, err
	}
	if line, _ := bufio.NewReader(out).ReadString('\n'); line != "ready\n" {
		p.Process.Kill()
		p.Wait()
		return nil, fmt.Errorf("%s: %q", name, line)
	}
	return p, nil
}

// Ready tells the measure that started the node that it runs.
func Ready() { fmt.Println("ready") }

// FreeAddr returns an address of the loopback no one listens on, now.
func FreeAddr() string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

// Resolver finds the nodes of addrs, for dist.
func Resolver(addrs map[string]string) func(string) (string, bool) {
	return func(node string) (string, bool) { a, ok := addrs[node]; return a, ok }
}

// Format writes addrs as name=address pairs, comma separated, for -addrs.
func Format(addrs map[string]string) string {
	var pairs []string
	for name, addr := range addrs {
		pairs = append(pairs, name+"="+addr)
	}
	return strings.Join(pairs, ",")
}

// Parse reads what Format wrote.
func Parse(s string) map[string]string {
	addrs := map[string]string{}
	for _, pair := range strings.Split(s, ",") {
		name, addr, _ := strings.Cut(pair, "=")
		addrs[name] = addr
	}
	return addrs
}

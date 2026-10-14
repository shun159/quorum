// Command locks is a lock service replicated over three nodes by quorum:
// the group agrees on who holds each lock, so a lock is not lost, nor
// given twice, while two of the three nodes live.
//
//	locks_sup (one_for_one)
//	├── locks_group (one_for_all)
//	│   ├── locks_storage  the storage of the node, in -dir
//	│   └── locks_member   the member of the group, its log in the storage
//	└── locks_holder       shows the locks the node holds, and works with them
//
// Each file holds one "module": locks_app.go is the application,
// locks_sup.go the supervisors, locks_machine.go the state machine and its
// API, locks_holder.go the holder, locks_console.go the console.
//
//	go run ./examples/locks -name a@localhost
//	go run ./examples/locks -name b@localhost
//	go run ./examples/locks -name c@localhost
//
// Once the three are up, found the group, once, on any of them; then take
// locks, by any name, for the node they are typed on:
//
//	found
//	founded on a@localhost, b@localhost, c@localhost
//	lock printer
//	*** this node holds the lock "printer", token 1
//	ok, token 1
//	*** working with the lock "printer", token 1
//
// The leader tells the node that took the lock, an effect of the machine,
// which only the leader performs; the holder of the node works with it
// every few seconds, until it is unlocked. On b:
//
//	lock printer
//	held by a@localhost, token 1
//
// Stop any one node, the leader too: the other two still know who holds
// printer, and give locks. Back, from its disk, the node catches up. A
// holder that dies keeps its locks: a real service would free them, after
// a lease the holder did not renew.
package main

import (
	"context"
	"flag"
	"log"
	"maps"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/shun159/molecule/dist"
	"github.com/shun159/molecule/proc"
)

func main() {
	name := flag.String("name", "a@localhost", "name of this node, one of -nodes")
	nodes := flag.String("nodes", "a@localhost=127.0.0.1:4390,b@localhost=127.0.0.1:4391,c@localhost=127.0.0.1:4392",
		"the nodes of the group, as name=address, comma separated")
	dir := flag.String("dir", "locks-data", "directory of the logs, one per node")
	flag.Parse()

	addrs := parseNodes(*nodes)
	if _, ok := addrs[*name]; !ok {
		log.Fatalf("%s is not in -nodes", *name)
	}

	n := proc.NewNode(*name)
	d, err := startDist(n, addrs)
	if err != nil {
		log.Fatal(err)
	}
	defer d.Stop()

	app, err := startLocks(context.Background(), n, filepath.Join(*dir, *name), startDisplay(n, os.Stdout))
	if err != nil {
		log.Fatal(err)
	}
	go console{n: n, nodes: slices.Sorted(maps.Keys(addrs)), out: os.Stdout}.run(os.Stdin)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	select {
	case <-ctx.Done():
		app.Stop(context.Background())
	case <-app.Done(): // the tree gave up
		log.Print(app.Err())
	}
}

// parseNodes parses name=address pairs, comma separated.
func parseNodes(s string) map[string]string {
	addrs := map[string]string{}
	for _, pair := range strings.Split(s, ",") {
		name, addr, _ := strings.Cut(pair, "=")
		addrs[name] = addr
	}
	return addrs
}

// startDist connects n to the other nodes, at addrs, listening at its own.
func startDist(n *proc.Node, addrs map[string]string) (*dist.Dist, error) {
	return dist.Start(n, dist.Config{
		Listen:   addrs[n.Name()],
		Cookie:   "locks",
		Resolve:  func(node string) (string, bool) { a, ok := addrs[node]; return a, ok },
		TickTime: 4 * time.Second,
	})
}

// Command locks is a lock service replicated over three nodes by quorum:
// groups agree on who holds each lock, so a lock is not lost, nor given
// twice, while two of the three nodes live. The locks spread over groups,
// eight by default, by their names: each group has its own leader, on any
// node, so the work spreads over the nodes, and a node lost moves only
// the groups it led.
//
//	locks_sup (one_for_one)
//	├── locks_groups (one_for_all)
//	│   ├── locks_storage      the storage of the node, in -dir, for all the groups
//	│   └── locks_members (one_for_one)
//	│       └── locks_member_0 … the member of each group, its log in the storage
//	└── locks_holder           shows the locks the node takes and frees, and the groups it leads
//
// Each file holds one "module": locks_app.go is the application,
// locks_sup.go the supervisors, locks_machine.go the state machine and its
// API, locks_holder.go the holder, locks_console.go the console.
//
//	go run ./examples/locks -name a@localhost
//	go run ./examples/locks -name b@localhost
//	go run ./examples/locks -name c@localhost
//
// Once the three are up, found the groups, once, on any of them; then take
// locks, by any name, for the node they are typed on. Each node tells the
// groups it comes to lead:
//
//	found
//	*** this node leads locks/1, term 1
//	founded 8 groups on a@localhost, b@localhost, c@localhost
//	lock printer
//	*** this node holds the lock "printer", token 1
//	ok, token 1
//
// The leader of the lock's group tells the node that took it, an effect
// of the machine, which only the leader performs, and tells it again when
// it frees it. On b:
//
//	lock printer
//	held by a@localhost, token 1
//
// groups tells where each group is led:
//
//	groups
//	locks/0   led by b@localhost, term 1, 2 locks held
//	locks/1   led by a@localhost, term 1, 1 locks held
//	…
//	groups led: a@localhost 2, b@localhost 3, c@localhost 3
//
// Stop any one node: the groups it led elect leaders on the other two, in
// a later term, which still know who holds each lock, and give locks.
// Back, from its disk, the node catches up.
//
// A lock lasts for a lease, ten seconds by default, which the node holding
// it renews: a node lost, its locks expire, by the time of the groups'
// logs, the same for all, and another node takes them, with a later
// token. Back, the node that held them is told:
//
//	*** this node lost the lock "printer", its lease expired
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
	groups := flag.Int("groups", 8, "how many groups the locks spread over, the same on every node")
	lease := flag.Duration("lease", 10*time.Second, "how long a lock lasts unless its node renews it")
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

	svc := Service{Nodes: slices.Sorted(maps.Keys(addrs)), Groups: *groups, Lease: *lease}
	app, err := startLocks(context.Background(), n, svc, filepath.Join(*dir, *name), startDisplay(n, os.Stdout))
	if err != nil {
		log.Fatal(err)
	}
	go console{n: n, svc: svc, out: os.Stdout}.run(os.Stdin)

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

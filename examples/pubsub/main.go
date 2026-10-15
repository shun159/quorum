// Command pubsub keeps topics of messages, replicated over three nodes by
// quorum: what is published is kept, in order, while two of the three
// live, and a subscriber reads on from where it left off, through any of
// them, however long it was gone.
//
//	pubsub_sup (one_for_one)
//	├── pubsub_group (one_for_all)
//	│   ├── pubsub_storage  the storage of the node, in -dir
//	│   └── pubsub_member   the member of the group, its log in the storage
//	└── pubsub_subs         dynamic supervisor of the subscribers of this node
//
// Each file holds one "module": pubsub_app.go is the application,
// pubsub_sup.go the supervisor, pubsub_machine.go the state machine and
// its API, pubsub_subscriber.go the subscriber, pubsub_console.go the
// console.
//
//	go run ./examples/pubsub -name a@localhost
//	go run ./examples/pubsub -name b@localhost
//	go run ./examples/pubsub -name c@localhost
//
// Once the three are up, found the group, once, on any of them; then
// subscribe on one, by a name, and publish on another:
//
//	found
//	sub reader news           (on b)
//	pub news hello            (on a)
//	published at 0
//	[news 0] hello (from a@localhost)   (on b)
//
// The machine keeps the latest thousand messages of each topic, by offset,
// and where each subscriber has read each topic to. A subscriber fetches
// from there, shows what it got, then commits it read it: stopped, by
// unsub or its node lost, then started again by the same name, it reads
// on from there, what was published meanwhile included. A message is
// shown at least once: twice if the subscriber stops between showing it
// and committing.
//
// Stop any one node: the other two keep the topics, and take what is
// published. Back, from its disk, the node catches up.
//
// The pubsub example of molecule is the other kind: messages handed to
// the subscribers there are, at most once, nothing kept. Here the topics
// are in one group; a load too much for one would spread the topics over
// groups by their names, as the locks example does.
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
	dir := flag.String("dir", "pubsub-data", "directory of the logs, one per node")
	flag.Parse()

	addrs := parseNodes(*nodes)
	if _, ok := addrs[*name]; !ok {
		log.Fatalf("%s is not in -nodes", *name)
	}
	n := proc.NewNode(*name)
	d, err := dist.Start(n, dist.Config{
		Listen:   addrs[*name],
		Cookie:   "pubsub",
		Resolve:  func(node string) (string, bool) { a, ok := addrs[node]; return a, ok },
		TickTime: 4 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer d.Stop()

	svc := Service{Nodes: slices.Sorted(maps.Keys(addrs))}
	app, err := startPubsub(context.Background(), n, svc, filepath.Join(*dir, *name))
	if err != nil {
		log.Fatal(err)
	}
	go newConsole(n, svc, os.Stdout).run(os.Stdin)

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

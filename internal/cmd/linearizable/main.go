// Command linearizable checks that a group is linearizable over dist, each
// node a process of its own, with its storage on disk, as Jepsen does. It
// starts the nodes, each connected to each other through a proxy of its
// own, founds a group of a register, and has users write and read it while
// a nemesis cuts nodes off, pauses them and kills them, the leader as
// often as not. Then it heals all, and checks the history of the users
// with porcupine.
//
//	go run ./internal/cmd/linearizable                   # 3 nodes, 30s
//	go run ./internal/cmd/linearizable -nodes 5 -time 2m -seed 7
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/shun159/molecule/dist"
	"github.com/shun159/molecule/proc"
	"github.com/shun159/quorum"
	"github.com/shun159/quorum/internal/cmd/nodes"
	"github.com/shun159/quorum/internal/register"
)

const group = "register"

func main() {
	node := flag.String("node", "", "run as this node, for the check")
	addrs := flag.String("addrs", "", "where the node finds the others, as name=address, comma separated, for -node")
	dir := flag.String("dir", "", "the storage of the node, for -node")
	count := flag.Int("nodes", 3, "how many nodes")
	users := flag.Int("users", 5, "how many users")
	timeout := flag.Duration("timeout", time.Second, "how long a user waits on a member before it asks another")
	length := flag.Duration("time", 30*time.Second, "how long the nemesis disturbs")
	seed := flag.Uint64("seed", uint64(time.Now().UnixNano()), "the seed of the nemesis' choices")
	flag.Parse()
	if *node != "" {
		runNode(*node, nodes.Parse(*addrs), *dir)
		return
	}
	if err := check(*count, *users, *timeout, *length, *seed); err != nil {
		log.Fatal(err)
	}
}

// check runs the nodes, the users and the nemesis, then checks what the
// users saw.
func check(count, users int, timeout, length time.Duration, seed uint64) error {
	dir, err := os.MkdirTemp("", "linearizable")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	c, err := startCluster(count, dir)
	if err != nil {
		return err
	}
	defer c.stop()

	n := proc.NewNode(control)
	d, err := dist.Start(n, dist.Config{Listen: c.addrs[control], Cookie: cookie, Resolve: nodes.Resolver(c.addrs)})
	if err != nil {
		return err
	}
	defer d.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	err = quorum.Found(ctx, n, group, c.names)
	cancel()
	if err != nil {
		return err
	}

	fmt.Printf("%d nodes, %d users waiting %v, %v, seed %d\n", count, users, timeout, length, seed)
	u, err := startUsers(n, c.names, users, timeout)
	if err != nil {
		return err
	}
	runNemesis(n, c, length, seed)
	c.heal()
	time.Sleep(5 * time.Second) // for the users' last operations to end
	history, err := u.stop(n)
	if err != nil {
		return err
	}
	return verdict(history, seed)
}

// verdict checks the history, and tells what came of it.
func verdict(history []register.Op, seed uint64) error {
	done, unknown := register.Count(history)
	res, info := register.Check(history, time.Minute)
	fmt.Printf("%d operations, %d done, %d unknown: %s\n", len(history), done, unknown, res)
	if res == porcupine.Ok {
		return nil
	}
	path := filepath.Join(os.TempDir(), fmt.Sprintf("linearizable-%d.html", seed))
	if err := register.Visualize(info, path); err == nil {
		fmt.Printf("see %s\n", path)
	}
	if res == porcupine.Unknown {
		return fmt.Errorf("the check timed out")
	}
	return fmt.Errorf("the history is not linearizable")
}

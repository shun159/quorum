// Command kv measures a group whose machine is a map of keys to values, a
// pmap.Map encoded by the machine, over three node processes keeping it
// on disk: how fast it takes keys, then writes among them, how much memory
// the nodes hold, and how long the group takes to take a write again
// once all its nodes were killed, as the number of keys grows.
//
//	go run ./internal/cmd/kv -keys 1000000 -dir ~/somewhere-on-disk
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"slices"
	"time"

	"github.com/shun159/molecule/dist"
	"github.com/shun159/molecule/proc"
	"github.com/shun159/quorum"
	"github.com/shun159/quorum/internal/cmd/nodes"
)

func main() {
	node := flag.String("node", "", "run as this node, for the measure")
	addrs := flag.String("addrs", "", "the nodes, as name=address, comma separated, for -node")
	dir := flag.String("dir", "", "the storage of the nodes, under it: on disk")
	keys := flag.Int("keys", 100000, "how many keys")
	value := flag.Int("value", 100, "the size of a value, in bytes")
	clients := flag.Int("clients", 64, "how many clients put at once")
	length := flag.Duration("time", 10*time.Second, "how long the clients write among the keys")
	flag.Parse()
	if *node != "" {
		runNode(*node, nodes.Parse(*addrs), *dir)
		return
	}
	if *dir == "" {
		log.Fatal("-dir: a directory on disk")
	}
	if err := measure(*dir, *keys, *value, *clients, *length); err != nil {
		log.Fatal(err)
	}
}

func measure(parent string, keys, value, clients int, length time.Duration) error {
	dir, err := os.MkdirTemp(parent, "kv")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	c, err := startCluster(dir)
	if err != nil {
		return err
	}
	defer c.kill()
	n := proc.NewNode(control)
	d, err := dist.Start(n, dist.Config{Listen: c.addrs[control], Cookie: cookie, Resolve: nodes.Resolver(c.addrs)})
	if err != nil {
		return err
	}
	defer d.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	err = quorum.Found(ctx, n, group, names)
	cancel()
	if err != nil {
		return err
	}
	val := make([]byte, value)
	fmt.Printf("%d keys of %d bytes, %d clients at once\n", keys, value, clients)

	start := time.Now()
	ss, err := startSenders(n, clients, sender{until: keys, val: val})
	if err != nil {
		return err
	}
	lat, failed, err := ss.wait(n)
	if err != nil {
		return err
	}
	took := time.Since(start)
	fmt.Printf("loaded in %v: %.0f puts/s, %d failed; %s\n", took.Round(time.Millisecond), float64(len(lat))/took.Seconds(), failed, latencies(lat))
	fmt.Printf("memory: %s\n", c.memory())

	ss, err = startSenders(n, clients, sender{until: keys, random: true, end: time.Now().Add(length), val: val})
	if err != nil {
		return err
	}
	lat, failed, err = ss.wait(n)
	if err != nil {
		return err
	}
	fmt.Printf("writes among them: %.0f puts/s, %d failed; %s\n", float64(len(lat))/length.Seconds(), failed, latencies(lat))
	fmt.Printf("memory: %s\n", c.memory())

	c.kill()
	start = time.Now()
	if err := c.start(); err != nil {
		return err
	}
	started := time.Since(start)
	if err := putAgain(n); err != nil {
		return err
	}
	fmt.Printf("all killed, started again in %v, writing in %v\n", started.Round(time.Millisecond), time.Since(start).Round(time.Millisecond))
	fmt.Printf("memory: %s\n", c.memory())
	return nil
}

// putAgain puts a key until the group takes it.
func putAgain(n *proc.Node) error {
	deadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := quorum.Command[int](ctx, n, group, names, Put{Key: "again"})
		cancel()
		if err == nil {
			return nil
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			time.Sleep(10 * time.Millisecond)
		}
	}
	return errors.New("the group takes no write")
}

func latencies(lat []time.Duration) string {
	if len(lat) == 0 {
		return "none"
	}
	slices.Sort(lat)
	us := func(d time.Duration) string { return d.Round(10 * time.Microsecond).String() }
	return fmt.Sprintf("median %v, p99 %v, max %v", us(lat[len(lat)/2]), us(lat[len(lat)*99/100]), us(lat[len(lat)-1]))
}

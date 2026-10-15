package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/proc"
)

// runNemesis disturbs the cluster for length: one fault at a time, for a
// second to four, then healed. Half the faults that take a node take the
// leader, whose reads are then tested.
func runNemesis(n *proc.Node, c *cluster, length time.Duration, seed uint64) {
	r := rand.New(rand.NewPCG(seed, 11))
	start := time.Now()
	for time.Since(start) < length {
		target, what := c.names[r.IntN(len(c.names))], ""
		if l, ok := leader(n, c); ok && r.IntN(2) == 0 {
			target, what = l, " (the leader)"
		}
		heal := disturb(c, r, target, what)
		time.Sleep(time.Duration(1000+r.IntN(3000)) * time.Millisecond)
		heal()
		c.heal()
	}
}

// disturb brings a fault on target, telling what it is, and returns what
// heals it, beyond cluster.heal.
func disturb(c *cluster, r *rand.Rand, target, what string) (heal func()) {
	others := slices.DeleteFunc(slices.Clone(c.names), func(s string) bool { return s == target })
	at := time.Now().Format("15:04:05.000")
	switch r.IntN(5) {
	case 0:
		fmt.Printf("%s all well\n", at)
	case 1:
		fmt.Printf("%s %s%s cut off, reset\n", at, target, what)
		cut(c, target, others, reset)
	case 2:
		fmt.Printf("%s %s%s cut off, silent\n", at, target, what)
		cut(c, target, others, silent)
	case 3:
		fmt.Printf("%s %s%s paused\n", at, target, what)
		c.pause(target, true)
	case 4:
		fmt.Printf("%s %s%s killed\n", at, target, what)
		c.kill(target)
		return func() {
			if err := c.start(target); err != nil {
				fmt.Printf("restarting %s: %v\n", target, err)
			}
		}
	}
	return func() {}
}

// cut cuts the node off from the others.
func cut(c *cluster, node string, others []string, s state) {
	for _, o := range others {
		c.link(node, o).set(s)
	}
}

// leader returns the node whose member leads, if one tells it does: of
// two that tell so, the one elected later is not known, so neither.
func leader(n *proc.Node, c *cluster) (string, bool) {
	var leaders []string
	for _, name := range c.names {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		ref := genserver.NewRef[leadsReq, bool, genserver.None](actor.Remote{Node: name, Name: "leading"})
		leads, err := ref.Call(ctx, n, leadsReq{})
		cancel()
		if err == nil && leads {
			leaders = append(leaders, name)
		}
	}
	if len(leaders) != 1 {
		return "", false
	}
	return leaders[0], true
}

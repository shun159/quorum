package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/proc"
)

// console reads commands from in, and runs them from n, the locks taken
// for n, writing what comes of them to out.
type console struct {
	n   *proc.Node
	svc Service
	out io.Writer
}

const usage = `found         found the groups: once all are up, on one of them, once
lock NAME     take the lock NAME, any name, as printer, for this node
unlock NAME   free it
who NAME      tell who holds it
groups        tell where each group is led, and how many locks it holds`

func (c console) run(in io.Reader) {
	fmt.Fprintf(c.out, "locks on %s, one of %s, in %d groups\n", c.n.Name(), strings.Join(c.svc.Nodes, ", "), c.svc.Groups)
	fmt.Fprintln(c.out, usage)
	sc := bufio.NewScanner(in)
	for sc.Scan() {
		switch f := strings.Fields(sc.Text()); {
		case len(f) == 0:
		case len(f) == 1 && f[0] == "found":
			c.found()
		case len(f) == 2 && f[0] == "lock":
			c.lock(f[1])
		case len(f) == 2 && f[0] == "unlock":
			c.unlock(f[1])
		case len(f) == 2 && f[0] == "who":
			c.who(f[1])
		case len(f) == 1 && f[0] == "groups":
			c.groups()
		default:
			fmt.Fprintln(c.out, usage)
		}
	}
}

func (c console) found() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := c.svc.Found(ctx, c.n); err != nil {
		fmt.Fprintf(c.out, "error: %v\n", err)
		return
	}
	fmt.Fprintf(c.out, "founded %d groups on %s\n", c.svc.Groups, strings.Join(c.svc.Nodes, ", "))
}

// groups tells, for each group, the node leading it, as the holders of
// the nodes up tell, and how many of its locks are held; then how many
// groups each node leads.
func (c console) groups() {
	type led struct {
		node string
		term uint64
	}
	leaders := map[string]led{}
	for _, node := range c.svc.Nodes {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		l, err := genserver.NewRef[leadsReq, Leads, genserver.None](holderAt(node)).Call(ctx, c.n, leadsReq{})
		cancel()
		if err != nil {
			continue // down
		}
		for g, term := range l.Groups {
			if term > leaders[g].term { // a leader deposed may not know yet
				leaders[g] = led{node, term}
			}
		}
	}
	count := map[string]int{}
	for i := range c.svc.Groups {
		g := group(i)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		n, err := c.svc.Count(ctx, c.n, g)
		cancel()
		switch l, ok := leaders[g]; {
		case !ok:
			fmt.Fprintf(c.out, "%-9s no leader\n", g)
		case err != nil:
			fmt.Fprintf(c.out, "%-9s led by %s, term %d: %v\n", g, l.node, l.term, err)
		default:
			fmt.Fprintf(c.out, "%-9s led by %s, term %d, %d locks held\n", g, l.node, l.term, n)
			count[l.node]++
		}
	}
	var each []string
	for _, node := range c.svc.Nodes {
		each = append(each, fmt.Sprintf("%s %d", node, count[node]))
	}
	fmt.Fprintf(c.out, "groups led: %s\n", strings.Join(each, ", "))
}

func (c console) lock(name string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r, err := c.svc.Lock(ctx, c.n, name, c.n.Name())
	switch {
	case err != nil:
		fmt.Fprintf(c.out, "error: %v\n", err)
	case r.OK:
		fmt.Fprintf(c.out, "ok, token %d\n", r.Holder.Token)
	default:
		fmt.Fprintf(c.out, "held by %s, token %d\n", r.Holder.Owner, r.Holder.Token)
	}
}

func (c console) unlock(name string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r, err := c.svc.Unlock(ctx, c.n, name, c.n.Name())
	switch {
	case err != nil:
		fmt.Fprintf(c.out, "error: %v\n", err)
	case r.OK:
		fmt.Fprintln(c.out, "ok")
	case r.Holder.Owner == "":
		fmt.Fprintln(c.out, "not held")
	default:
		fmt.Fprintf(c.out, "held by %s\n", r.Holder.Owner)
	}
}

func (c console) who(name string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r, err := c.svc.Who(ctx, c.n, name)
	switch {
	case err != nil:
		fmt.Fprintf(c.out, "error: %v\n", err)
	case r.OK:
		fmt.Fprintf(c.out, "%s, token %d\n", r.Holder.Owner, r.Holder.Token)
	default:
		fmt.Fprintln(c.out, "free")
	}
}

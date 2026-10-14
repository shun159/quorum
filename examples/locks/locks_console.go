package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/shun159/molecule/proc"
	"github.com/shun159/quorum"
)

// console reads commands from in, and runs them from n, the locks taken
// for n, writing what comes of them to out.
type console struct {
	n     *proc.Node
	nodes []string
	out   io.Writer
}

const usage = "found, lock NAME, unlock NAME, or who NAME"

func (c console) run(in io.Reader) {
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
		default:
			fmt.Fprintln(c.out, usage)
		}
	}
}

func (c console) found() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := quorum.Found(ctx, c.n, group, c.nodes); err != nil {
		fmt.Fprintf(c.out, "error: %v\n", err)
		return
	}
	fmt.Fprintf(c.out, "founded on %s\n", strings.Join(c.nodes, ", "))
}

func (c console) lock(name string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r, err := Lock(ctx, c.n, c.nodes, name, c.n.Name())
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
	r, err := Unlock(ctx, c.n, c.nodes, name, c.n.Name())
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
	r, err := Who(ctx, c.n, c.nodes, name)
	switch {
	case err != nil:
		fmt.Fprintf(c.out, "error: %v\n", err)
	case r.OK:
		fmt.Fprintf(c.out, "%s, token %d\n", r.Holder.Owner, r.Holder.Token)
	default:
		fmt.Fprintln(c.out, "free")
	}
}

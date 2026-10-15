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

// console reads commands from in and runs them from n, writing what comes
// of them, and what the subscribers of the node read, to out.
type console struct {
	n       *proc.Node
	svc     Service
	out     io.Writer
	display proc.PID            // writes what the subscribers read
	subs    map[string]proc.PID // the subscribers of the node, by name and topic
}

func newConsole(n *proc.Node, svc Service, out io.Writer) *console {
	display := n.Spawn(func(s *proc.Self) error {
		for {
			msg, err := s.Receive(context.Background())
			if err != nil {
				return err
			}
			fmt.Fprintln(out, msg)
		}
	})
	return &console{n: n, svc: svc, out: out, display: display, subs: map[string]proc.PID{}}
}

func (c *console) run(in io.Reader) {
	sc := bufio.NewScanner(in)
	for sc.Scan() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		switch f := strings.Fields(sc.Text()); {
		case len(f) == 0:
		case len(f) == 1 && f[0] == "found":
			c.report(quorum.Found(ctx, c.n, group, c.svc.Nodes), "founded")
		case len(f) >= 3 && f[0] == "pub":
			offset, err := c.svc.Publish(ctx, c.n, c.n.Name(), f[1], strings.Join(f[2:], " "))
			c.report(err, fmt.Sprintf("published at %d", offset))
		case len(f) == 3 && f[0] == "sub":
			c.subscribe(ctx, f[1], f[2])
		case len(f) == 3 && f[0] == "unsub":
			c.unsubscribe(ctx, f[1], f[2])
		case len(f) == 1 && f[0] == "topics":
			topics, err := c.svc.Topics(ctx, c.n)
			c.report(err, strings.Join(topics, "\n"))
		default:
			fmt.Fprintln(c.out, "found, pub TOPIC TEXT, sub NAME TOPIC, unsub NAME TOPIC, or topics")
		}
		cancel()
	}
}

func (c *console) subscribe(ctx context.Context, sub, topic string) {
	key := sub + " " + topic
	if _, ok := c.subs[key]; ok {
		return
	}
	pid, err := Subscribe(ctx, c.n, c.svc, sub, topic, c.display)
	if err == nil {
		c.subs[key] = pid
	}
	c.report(err, "")
}

func (c *console) unsubscribe(ctx context.Context, sub, topic string) {
	key := sub + " " + topic
	if pid, ok := c.subs[key]; ok {
		delete(c.subs, key)
		c.report(Unsubscribe(ctx, c.n, pid), "")
	}
}

func (c *console) report(err error, done string) {
	switch {
	case err != nil:
		fmt.Fprintln(c.out, "error:", err)
	case done != "":
		fmt.Fprintln(c.out, done)
	}
}

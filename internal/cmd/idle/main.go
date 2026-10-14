// Command idle measures what idle groups cost: groups founded on three
// nodes of this process, over dist on localhost, then left alone. For each
// number of groups, it tells the messages and the bytes the nodes send each
// other a second, the CPU the process takes, and the elections there were,
// while nothing is asked of the groups.
//
//	go run ./internal/cmd/idle -groups 1,10,100,1000
//	go run ./internal/cmd/idle -groups 1000 -quiet=-1s  # never quiet
package main

import (
	"bytes"
	"context"
	"encoding/gob"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/supervisor"
	"github.com/shun159/molecule/dist"
	"github.com/shun159/molecule/proc"
	"github.com/shun159/quorum"
)

var names = []string{"a@localhost", "b@localhost", "c@localhost"}

// quiet is Spec.Quiet of the groups.
var quiet time.Duration

// profiling is where the CPU profile goes, if anywhere: of the last
// number of groups measured.
var profiling *os.File

func main() {
	groups := flag.String("groups", "1,10,100,1000", "the numbers of groups to measure, comma separated")
	settle := flag.Duration("settle", 3*time.Second, "how long to wait, once founded, before measuring")
	window := flag.Duration("window", 5*time.Second, "how long to measure")
	profile := flag.String("cpuprofile", "", "write a CPU profile of the windows measured to this file")
	flag.DurationVar(&quiet, "quiet", 0, "Spec.Quiet of the groups: negative, as -1s, they never go quiet")
	flag.Parse()
	if *profile != "" {
		f, err := os.Create(*profile)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		profiling = f
	}

	fmt.Printf("%7s %10s %10s %8s %10s %10s\n", "groups", "msgs/s", "KB/s", "CPU%", "elections", "goroutines")
	for _, s := range strings.Split(*groups, ",") {
		n, err := strconv.Atoi(s)
		if err != nil {
			log.Fatal(err)
		}
		r, err := measure(n, *settle, *window)
		if err != nil {
			log.Fatalf("%d groups: %v", n, err)
		}
		fmt.Printf("%7d %10.0f %10.1f %8.1f %10d %10d\n", n, r.msgs, r.kbytes, r.cpu, r.elections, r.goroutines)
	}
}

// result is what n idle groups cost, a second.
type result struct {
	msgs, kbytes, cpu float64
	elections         int64
	goroutines        int
}

// measure founds n groups, waits settle, and measures them for window.
func measure(n int, settle, window time.Duration) (result, error) {
	c, err := start(n)
	if err != nil {
		return result{}, err
	}
	defer c.stop()
	if err := c.found(n); err != nil {
		return result{}, err
	}
	time.Sleep(settle)

	if profiling != nil {
		pprof.StartCPUProfile(profiling)
		defer pprof.StopCPUProfile()
	}
	msgs, bytes, elections, cpu := c.msgs.Load(), c.bytes.Load(), c.elections.Load(), cpuTime()
	begin := time.Now()
	time.Sleep(window)
	secs := time.Since(begin).Seconds()
	return result{
		msgs:       float64(c.msgs.Load()-msgs) / secs,
		kbytes:     float64(c.bytes.Load()-bytes) / secs / 1024,
		cpu:        (cpuTime() - cpu).Seconds() / secs * 100,
		elections:  c.elections.Load() - elections,
		goroutines: runtime.NumGoroutine(),
	}, nil
}

// cluster is the three nodes, each with the member of every group.
type cluster struct {
	mu    sync.Mutex
	addrs map[string]string
	nodes []*proc.Node
	dists []*dist.Dist
	sups  []proc.PID

	msgs, bytes, elections atomic.Int64
}

func start(n int) (*cluster, error) {
	c := &cluster{addrs: map[string]string{}}
	for _, name := range names {
		node := proc.NewNode(name)
		d, err := dist.Start(node, dist.Config{
			Listen:  "127.0.0.1:0",
			Cookie:  "idle",
			Resolve: c.resolve,
			Codec:   counting{&c.msgs, &c.bytes},
		})
		if err != nil {
			return nil, err
		}
		c.mu.Lock()
		c.addrs[name] = d.Addr().String()
		c.mu.Unlock()
		var children []supervisor.ChildSpec
		for g := range n {
			spec := quorum.Spec[int, int, int]{Group: group(g), Machine: counter{&c.elections}, Quiet: quiet}
			children = append(children, supervisor.ChildSpec{ID: group(g), Start: quorum.Child(spec)})
		}
		sup, err := supervisor.Start(context.Background(), node, supervisor.Spec{Strategy: supervisor.OneForOne, Children: children})
		if err != nil {
			return nil, err
		}
		c.nodes, c.dists, c.sups = append(c.nodes, node), append(c.dists, d), append(c.sups, sup)
	}
	return c, nil
}

func group(g int) string { return fmt.Sprintf("g%d", g) }

// found founds the n groups, a few at once.
func (c *cluster) found(n int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	errs := make(chan error, n)
	sem := make(chan struct{}, 16)
	for g := range n {
		sem <- struct{}{}
		go func() {
			defer func() { <-sem }()
			errs <- quorum.Found(ctx, c.nodes[g%len(c.nodes)], group(g), names)
		}()
	}
	for range n {
		if err := <-errs; err != nil {
			return err
		}
	}
	return nil
}

func (c *cluster) stop() {
	for i, node := range c.nodes {
		supervisor.Stop(context.Background(), node, c.sups[i])
		c.dists[i].Stop()
	}
}

func (c *cluster) resolve(node string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	a, ok := c.addrs[node]
	return a, ok
}

// counter is a machine that counts the elections of its group.
type counter struct{ elections *atomic.Int64 }

func (counter) Init() int { return 0 }

func (counter) Apply(sum, n int) (int, int, []actor.Effect) { return sum + n, sum + n, nil }

func (c counter) Elected(int, uint64) []actor.Effect {
	c.elections.Add(1)
	return nil
}

func (counter) Surrendered(int) []actor.Effect { return nil }

func cpuTime() time.Duration {
	var ru syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

// counting is a gob codec that counts the messages it encodes, and their
// bytes: those the nodes send each other.
type counting struct{ msgs, bytes *atomic.Int64 }

// envelope carries a message in an interface, so that gob tells its type.
type envelope struct{ Msg any }

func (c counting) NewEncoder() dist.Encoder {
	e := &encoder{c: c}
	e.enc = gob.NewEncoder(&e.buf)
	return e
}

func (counting) NewDecoder() dist.Decoder {
	d := &decoder{}
	d.dec = gob.NewDecoder(&d.buf)
	return d
}

type encoder struct {
	c   counting
	buf bytes.Buffer
	enc *gob.Encoder
}

func (e *encoder) Encode(msg any) ([]byte, error) {
	e.buf.Reset()
	if err := e.enc.Encode(envelope{msg}); err != nil {
		return nil, err
	}
	e.c.msgs.Add(1)
	e.c.bytes.Add(int64(e.buf.Len()))
	return bytes.Clone(e.buf.Bytes()), nil
}

type decoder struct {
	buf bytes.Buffer
	dec *gob.Decoder
}

func (d *decoder) Decode(b []byte) (any, error) {
	d.buf.Write(b)
	var env envelope
	if err := d.dec.Decode(&env); err != nil {
		return nil, err
	}
	return env.Msg, nil
}

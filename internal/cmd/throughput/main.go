// Command throughput measures how many commands groups apply a second,
// over dist, each node a process of its own. It starts three nodes, founds
// the groups, then has clients send commands to each group, each one after
// the other, in a session of its own, for a while, and tells the commands applied a second, their
// latency, the CPU the nodes took, and the elections meanwhile.
//
//	go run ./internal/cmd/throughput -groups 1 -clients 64          # one group, 64 at once
//	go run ./internal/cmd/throughput -groups 100 -clients 4 -dir ~/q  # on disk, kept under ~/q
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/behaviours/supervisor"
	"github.com/shun159/molecule/dist"
	"github.com/shun159/molecule/proc"
	"github.com/shun159/quorum"
	"github.com/shun159/quorum/internal/cmd/nodes"
)

var names = []string{"a@localhost", "b@localhost", "c@localhost"}

func main() {
	node := flag.String("node", "", "run as this node, for the measure")
	addrs := flag.String("addrs", "", "the nodes, as name=address, comma separated, for -node")
	groups := flag.Int("groups", 1, "how many groups")
	clients := flag.Int("clients", 16, "how many clients send to each group at once")
	length := flag.Duration("time", 10*time.Second, "how long the clients send")
	dir := flag.String("dir", "", "keep the storage of the nodes under dir, on disk; none if empty")
	flag.Parse()
	if *node != "" {
		runNode(*node, nodes.Parse(*addrs), *groups, *dir)
		return
	}
	if err := measure(*groups, *clients, *length, *dir); err != nil {
		log.Fatal(err)
	}
}

// measure runs the three nodes, has the clients send for length, and
// tells what came of it.
func measure(groups, clients int, length time.Duration, dir string) error {
	if dir != "" {
		var err error
		if dir, err = os.MkdirTemp(dir, "throughput"); err != nil {
			return err
		}
		defer os.RemoveAll(dir)
	}
	addrs := map[string]string{}
	for _, name := range append(slices.Clone(names), "measure@localhost") {
		addrs[name] = nodes.FreeAddr()
	}
	procs := map[string]*exec.Cmd{}
	defer func() {
		for _, p := range procs {
			p.Process.Kill()
			p.Wait()
		}
	}()
	for _, name := range names {
		nodeDir := ""
		if dir != "" {
			nodeDir = filepath.Join(dir, name)
		}
		p, err := nodes.Start(name, addrs, "-groups", fmt.Sprint(groups), "-dir", nodeDir)
		if err != nil {
			return err
		}
		procs[name] = p
	}

	n := proc.NewNode("measure@localhost")
	d, err := dist.Start(n, dist.Config{Listen: addrs[n.Name()], Cookie: "throughput", Resolve: nodes.Resolver(addrs)})
	if err != nil {
		return err
	}
	defer d.Stop()
	if err := found(n, groups); err != nil {
		return err
	}
	where := "in memory"
	if dir != "" {
		where = "on disk"
	}
	fmt.Printf("%d groups, %d clients each, %v, %s\n", groups, clients, length, where)

	time.Sleep(time.Second) // for the elections to settle
	elections := electionsOf(n)
	cpu := cpuOf(procs)
	start := time.Now()
	ss, err := startSenders(n, groups, clients, start.Add(length))
	if err != nil {
		return err
	}
	time.Sleep(time.Until(start.Add(length)))
	took := time.Since(start)
	used := cpuOf(procs) - cpu
	time.Sleep(time.Second) // for the last commands to end
	lat, failed, err := ss.results(n)
	if err != nil {
		return err
	}

	slices.Sort(lat)
	if len(lat) == 0 {
		return fmt.Errorf("no command applied, %d failed", failed)
	}
	rate := float64(len(lat)) / took.Seconds()
	fmt.Printf("%.0f commands/s (%.0f a group), %d failed\n", rate, rate/float64(groups), failed)
	fmt.Printf("latency: median %v, p99 %v, max %v\n", us(lat[len(lat)/2]), us(lat[len(lat)*99/100]), us(lat[len(lat)-1]))
	fmt.Printf("nodes' CPU: %.2f cores, %.1f µs a command\n", used.Seconds()/took.Seconds(), float64(used.Microseconds())/float64(len(lat)))
	fmt.Printf("elections meanwhile: %d\n", electionsOf(n)-elections)
	return nil
}

func found(n *proc.Node, groups int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	errs := make(chan error, groups)
	sem := make(chan struct{}, 16)
	for g := range groups {
		sem <- struct{}{}
		go func() {
			defer func() { <-sem }()
			errs <- quorum.Found(ctx, n, group(g), names)
		}()
	}
	for range groups {
		if err := <-errs; err != nil {
			return err
		}
	}
	return nil
}

// cpuOf returns the CPU time the processes took so far, user and system.
func cpuOf(procs map[string]*exec.Cmd) time.Duration {
	var sum time.Duration
	for _, p := range procs {
		b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", p.Process.Pid))
		if err != nil {
			continue
		}
		// The fields after the command, in parentheses: utime and stime are
		// the 12th and 13th, in clock ticks of 1/100 s.
		fields := strings.Fields(string(b[strings.LastIndexByte(string(b), ')')+2:]))
		for _, f := range fields[11:13] {
			ticks, _ := strconv.ParseInt(f, 10, 64)
			sum += time.Duration(ticks) * 10 * time.Millisecond
		}
	}
	return sum
}

// electionsOf returns the elections won so far on all the nodes.
func electionsOf(n *proc.Node) int64 {
	var sum int64
	for _, name := range names {
		ref := genserver.NewRef[electionsReq, int64, genserver.None](actor.Remote{Node: name, Name: "elections"})
		v, err := ref.Call(context.Background(), n, electionsReq{})
		if err != nil {
			log.Fatal(err)
		}
		sum += v
	}
	return sum
}

// runNode runs the node name, with the member of every group, until
// killed: its storage in dir, if not empty.
func runNode(name string, addrs map[string]string, groups int, dir string) {
	n := proc.NewNode(name)
	if _, err := dist.Start(n, dist.Config{Listen: addrs[name], Cookie: "throughput", Resolve: nodes.Resolver(addrs)}); err != nil {
		log.Fatal(err)
	}
	var members []supervisor.ChildSpec
	for g := range groups {
		spec := quorum.Spec[int, int, int]{Group: group(g), Machine: counter{}, Dir: dir}
		members = append(members, supervisor.ChildSpec{ID: group(g), Start: quorum.Child(spec)})
	}
	children := []supervisor.ChildSpec{{ID: "elections", Start: genserver.Child(counting{}, actor.WithName(actor.Local("elections")))}}
	if dir != "" {
		children = append(children, supervisor.ChildSpec{ID: "storage", Start: quorum.Storage(dir)})
	}
	children = append(children, supervisor.ChildSpec{ID: "members", Start: supervisor.Child(supervisor.Spec{Strategy: supervisor.OneForOne, Children: members})})
	if _, err := supervisor.Start(context.Background(), n, supervisor.Spec{Strategy: supervisor.OneForAll, Children: children}); err != nil {
		log.Fatal(err)
	}
	nodes.Ready()
	select {}
}

func group(g int) string { return fmt.Sprintf("g%d", g) }

// elections are the elections the groups won on this node.
var elections atomic.Int64

// counter is a machine adding up its commands, counting its elections.
type counter struct{}

func (counter) Init() int { return 0 }

func (counter) Apply(sum, n int) (int, int, []actor.Effect) { return sum + n, sum + n, nil }

func (counter) Elected(int, uint64) []actor.Effect {
	elections.Add(1)
	return nil
}

func (counter) Surrendered(int) []actor.Effect { return nil }

// counting answers how many elections the groups won on its node.
type counting struct{ genserver.Default[struct{}] }

type electionsReq struct{}

func init() { dist.Register(electionsReq{}) }

func (counting) HandleCall(s struct{}, _ electionsReq, from genserver.From[int64]) (struct{}, []actor.Effect) {
	return s, actor.Do(from.Reply(elections.Load()))
}

func us(d time.Duration) string { return d.Round(10 * time.Microsecond).String() }

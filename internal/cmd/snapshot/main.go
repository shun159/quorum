// Command snapshot measures what sending a large snapshot costs the other
// groups of the nodes, over dist, each node a process of its own. It
// starts three nodes, a group of a large state on two of them and many
// small groups on all three, then adds the third to the large group, its
// leader sending it the whole state, while a command goes to each small
// group every tick. It tells how long the state took to arrive, the
// latency of the small groups before and meanwhile, their elections, and
// the memory of the leader's process.
//
//	go run ./internal/cmd/snapshot -size 100   # a state of 100 MB
//	go run ./internal/cmd/snapshot -size 100 -maxframe 64  # dist.Config.MaxFrame of 64 MB
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"slices"
	"strings"
	"sync"
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

const (
	big  = "big"
	tick = 100 * time.Millisecond
)

func main() {
	node := flag.String("node", "", "run as this node, for the measure")
	addrs := flag.String("addrs", "", "the nodes, as name=address, comma separated, for -node")
	groups := flag.Int("groups", 100, "how many small groups")
	size := flag.Int("size", 100, "the size of the large state, in MB")
	maxFrame := flag.Int("maxframe", 0, "dist.Config.MaxFrame of the nodes, in MB: zero, none")
	flag.Parse()
	if *node != "" {
		runNode(*node, nodes.Parse(*addrs), *groups, *maxFrame<<20)
		return
	}
	if err := measure(*groups, *size, *maxFrame); err != nil {
		log.Fatal(err)
	}
}

// measure runs the three nodes, fills the large group, adds the third
// node to it, and tells what came of it.
func measure(groups, size, maxFrame int) error {
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
		p, err := nodes.Start(name, addrs,
			"-groups", fmt.Sprint(groups), "-maxframe", fmt.Sprint(maxFrame))
		if err != nil {
			return err
		}
		procs[name] = p
	}

	n := proc.NewNode("measure@localhost")
	d, err := dist.Start(n, dist.Config{Listen: addrs[n.Name()], Cookie: "snapshot", Resolve: nodes.Resolver(addrs), MaxFrame: maxFrame << 20})
	if err != nil {
		return err
	}
	defer d.Stop()
	ctx := context.Background()
	for g := range groups {
		if err := quorum.Found(ctx, n, small(g), names); err != nil {
			return err
		}
	}
	two := names[:2]
	if err := quorum.Found(ctx, n, big, two); err != nil {
		return err
	}
	start := time.Now()
	if _, err := quorum.Command[int](ctx, n, big, two, size<<20); err != nil {
		return err
	}
	leader := leaderOf(n, big)
	fmt.Printf("%d small groups; a state of %d MB on %s and %s, made in %v; %s leads it\n",
		groups, size, two[0], two[1], ms(time.Since(start)), leader)

	load := startLoad(n, groups)
	time.Sleep(3 * time.Second)
	before := load.window()
	elections := electionsOf(n)

	start = time.Now()
	addCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	err = quorum.AddMember(addCtx, n, big, two, names[2])
	cancel()
	took := time.Since(start)
	during := load.window()
	load.stop()
	if err != nil {
		fmt.Printf("adding %s: %v, after %v\n", names[2], err, ms(took))
	} else {
		fmt.Printf("%s added, the state sent, in %v\n", names[2], ms(took))
	}
	fmt.Printf("small groups before: %s\n", before)
	fmt.Printf("small groups during: %s\n", during)
	fmt.Printf("elections of the small groups during: %d\n", electionsOf(n)-elections)
	for _, name := range names {
		fmt.Printf("%s: %s\n", name, memory(procs[name].Process.Pid))
	}
	return nil
}

// leaderOf returns the node leading group, as the nodes tell.
func leaderOf(n *proc.Node, group string) string {
	for _, name := range names {
		if slices.Contains(ask(n, name).Groups, group) {
			return name
		}
	}
	return "none"
}

// electionsOf returns the elections won so far on all the nodes.
func electionsOf(n *proc.Node) int64 {
	var sum int64
	for _, name := range names {
		sum += ask(n, name).Elections
	}
	return sum
}

func ask(n *proc.Node, name string) leaders {
	ref := genserver.NewRef[leadersReq, leaders, genserver.None](actor.Remote{Node: name, Name: "leaders"})
	v, err := ref.Call(context.Background(), n, leadersReq{})
	if err != nil {
		log.Fatal(err)
	}
	return v
}

// load sends a command to each small group every tick, and keeps their
// latencies, a window at a time.
type load struct {
	mu        sync.Mutex
	latencies []time.Duration
	failed    int
	done      chan struct{}
	wg        sync.WaitGroup
}

func startLoad(n *proc.Node, groups int) *load {
	l := &load{done: make(chan struct{})}
	for g := range groups {
		l.wg.Go(func() {
			t := time.NewTicker(tick)
			defer t.Stop()
			for {
				select {
				case <-l.done:
					return
				case <-t.C:
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				start := time.Now()
				_, err := quorum.Command[int](ctx, n, small(g), names, 1)
				took := time.Since(start)
				cancel()
				l.mu.Lock()
				if err != nil {
					l.failed++
				} else {
					l.latencies = append(l.latencies, took)
				}
				l.mu.Unlock()
			}
		})
	}
	return l
}

// window returns the latencies since the last window, and starts another.
func (l *load) window() string {
	l.mu.Lock()
	lat, failed := l.latencies, l.failed
	l.latencies, l.failed = nil, 0
	l.mu.Unlock()
	if len(lat) == 0 {
		return fmt.Sprintf("no command applied, %d failed", failed)
	}
	slices.Sort(lat)
	return fmt.Sprintf("%d commands, %d failed: median %v, p99 %v, max %v",
		len(lat), failed, ms(lat[len(lat)/2]), ms(lat[len(lat)*99/100]), ms(lat[len(lat)-1]))
}

func (l *load) stop() {
	close(l.done)
	l.wg.Wait()
}

// memory tells the resident memory of the process pid, now and at most.
func memory(pid int) string {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return err.Error()
	}
	var rss, hwm string
	for line := range strings.Lines(string(b)) {
		if v, ok := strings.CutPrefix(line, "VmRSS:"); ok {
			rss = strings.TrimSpace(v)
		}
		if v, ok := strings.CutPrefix(line, "VmHWM:"); ok {
			hwm = strings.TrimSpace(v)
		}
	}
	return fmt.Sprintf("memory %s, at most %s", rss, hwm)
}

// runNode runs the node name, with the member of every group, until
// killed.
func runNode(name string, addrs map[string]string, groups, maxFrame int) {
	n := proc.NewNode(name)
	if _, err := dist.Start(n, dist.Config{Listen: addrs[name], Cookie: "snapshot", Resolve: nodes.Resolver(addrs), MaxFrame: maxFrame}); err != nil {
		log.Fatal(err)
	}
	children := []supervisor.ChildSpec{
		{ID: "leaders", Start: genserver.Child(leading{}, actor.WithName(actor.Local("leaders")))},
		{ID: big, Start: quorum.Child(quorum.Spec[[]byte, int, int]{Group: big, Machine: blob{}, SnapshotEvery: 1})},
	}
	for g := range groups {
		spec := quorum.Spec[int, int, int]{Group: small(g), Machine: counter{small(g)}}
		children = append(children, supervisor.ChildSpec{ID: small(g), Start: quorum.Child(spec)})
	}
	if _, err := supervisor.Start(context.Background(), n, supervisor.Spec{Strategy: supervisor.OneForOne, Children: children}); err != nil {
		log.Fatal(err)
	}
	nodes.Ready()
	select {}
}

func small(g int) string { return fmt.Sprintf("g%d", g) }

// The groups this node leads, as their machines are told, and the
// elections they won.
var (
	led       sync.Map
	elections atomic.Int64
)

// counter is a machine that tells when its member leads.
type counter struct{ group string }

func (counter) Init() int { return 0 }

func (counter) Apply(sum, n int) (int, int, []actor.Effect) { return sum + n, sum + n, nil }

func (c counter) Elected(int, uint64) []actor.Effect {
	led.Store(c.group, true)
	elections.Add(1)
	return nil
}

func (c counter) Surrendered(int) []actor.Effect {
	led.Delete(c.group)
	return nil
}

// blob is a machine whose state is as many bytes as a command says.
type blob struct{}

func (blob) Init() []byte { return nil }

func (blob) Apply(_ []byte, n int) ([]byte, int, []actor.Effect) {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i)
	}
	return b, n, nil
}

// EncodeState and DecodeState send the state as it is, gob holding it
// whole in one message, at several times its size.
func (blob) EncodeState(b []byte) ([]byte, error) { return b, nil }

func (blob) DecodeState(b []byte) ([]byte, error) { return b, nil }

func (blob) Elected([]byte, uint64) []actor.Effect {
	led.Store(big, true)
	return nil
}

func (blob) Surrendered([]byte) []actor.Effect {
	led.Delete(big)
	return nil
}

// leading answers which groups its node leads, and how many elections
// they won.
type leading struct{ genserver.Default[struct{}] }

type (
	leadersReq struct{}
	leaders    struct {
		Groups    []string
		Elections int64
	}
)

func init() { dist.Register(leadersReq{}, leaders{}) }

func (leading) HandleCall(s struct{}, _ leadersReq, from genserver.From[leaders]) (struct{}, []actor.Effect) {
	l := leaders{Elections: elections.Load()}
	led.Range(func(g, _ any) bool {
		l.Groups = append(l.Groups, g.(string))
		return true
	})
	return s, actor.Do(from.Reply(l))
}

func ms(d time.Duration) string { return d.Round(time.Millisecond).String() }

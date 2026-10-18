// Command failover measures how long groups take to take commands again
// once the node of their leader is lost, over dist, each node a process
// of its own. It starts three nodes, founds the groups, lets them go
// quiet, then stops the node that leads the most of them, and measures,
// for each group it led, the time until a command through the other two
// is applied.
//
//	go run ./internal/cmd/failover -groups 100 -how stop  # frozen: its node falls silent
//	go run ./internal/cmd/failover -groups 100 -how kill  # killed: its connections close
//	go run ./internal/cmd/failover -groups 100 -quiet=-1s # groups never quiet
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os/exec"
	"slices"
	"sync"
	"syscall"
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
	groups := flag.Int("groups", 100, "how many groups")
	quiet := flag.Duration("quiet", 0, "Spec.Quiet of the groups: negative, as -1s, they never go quiet")
	silent := flag.Duration("silent", 0, "dist.Config.Silent of the nodes: zero is dist's default")
	how := flag.String("how", "stop", "how the node is lost: stop, frozen by SIGSTOP; kill, by SIGKILL")
	flag.Parse()
	if *node != "" {
		runNode(*node, nodes.Parse(*addrs), *groups, *quiet, *silent)
		return
	}
	if err := measure(*groups, *quiet, *silent, *how); err != nil {
		log.Fatal(err)
	}
}

// measure runs the three nodes, loses the one leading most, and tells how
// long its groups took to take a command again.
func measure(groups int, quiet, silent time.Duration, how string) error {
	addrs := map[string]string{}
	for _, name := range append(slices.Clone(names), "measure@localhost") {
		addrs[name] = nodes.FreeAddr()
	}
	var procs []*exec.Cmd
	defer func() {
		for _, p := range procs {
			p.Process.Signal(syscall.SIGCONT)
			p.Process.Kill()
			p.Wait()
		}
	}()
	for _, name := range names {
		p, err := nodes.Start(name, addrs,
			"-groups", fmt.Sprint(groups), "-quiet", quiet.String(), "-silent", silent.String())
		if err != nil {
			return err
		}
		procs = append(procs, p)
	}

	n := proc.NewNode("measure@localhost")
	d, err := dist.Start(n, dist.Config{Listen: addrs[n.Name()], Cookie: "failover", Resolve: nodes.Resolver(addrs), Silent: silent})
	if err != nil {
		return err
	}
	defer d.Stop()
	if err := found(n, groups); err != nil {
		return err
	}
	time.Sleep(3 * time.Second) // to go quiet

	led := map[string][]string{}
	for _, name := range names {
		v, err := genserver.NewRef[leadersReq, leaders, genserver.None](actor.Remote{Node: name, Name: "leaders"}).Call(context.Background(), n, leadersReq{})
		if err != nil {
			return err
		}
		led[name] = v.Groups
	}
	lost := slices.MaxFunc(names, func(x, y string) int { return len(led[x]) - len(led[y]) })
	others := slices.DeleteFunc(slices.Clone(names), func(s string) bool { return s == lost })
	fmt.Printf("%d groups, quiet %v, silent %v: %s leads %d, lost by %s\n", groups, quiet, cmpOr(silent, dist.DefaultSilent), lost, len(led[lost]), how)

	sig := syscall.SIGSTOP
	if how == "kill" {
		sig = syscall.SIGKILL
	}
	start := time.Now()
	procs[slices.Index(names, lost)].Process.Signal(sig)
	took := make([]time.Duration, len(led[lost]))
	var wg sync.WaitGroup
	for i, g := range led[lost] {
		wg.Go(func() { took[i] = commandAgain(n, g, others, start) })
	}
	wg.Wait()
	slices.Sort(took)
	fmt.Printf("took: min %v, median %v, p90 %v, max %v\n",
		ms(took[0]), ms(took[len(took)/2]), ms(took[len(took)*9/10]), ms(took[len(took)-1]))
	return nil
}

// commandAgain sends a command to group through others until one is
// applied, and returns when, since start. Each try waits 100ms at most:
// a member may still send it to the leader lost.
func commandAgain(n *proc.Node, group string, others []string, start time.Time) time.Duration {
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		_, err := quorum.Command[int](ctx, n, group, others, 1)
		cancel()
		if err == nil {
			return time.Since(start)
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			time.Sleep(10 * time.Millisecond)
		}
	}
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

// runNode runs the node name, with the member of every group, until
// killed.
func runNode(name string, addrs map[string]string, groups int, quiet, silent time.Duration) {
	n := proc.NewNode(name)
	if _, err := dist.Start(n, dist.Config{Listen: addrs[name], Cookie: "failover", Resolve: nodes.Resolver(addrs), Silent: silent}); err != nil {
		log.Fatal(err)
	}
	children := []supervisor.ChildSpec{{ID: "leaders", Start: genserver.Child(leading{}, actor.WithName(actor.Local("leaders")))}}
	for g := range groups {
		spec := quorum.Spec[int, int, int]{Group: group(g), Machine: counter{group(g)}, Quiet: quiet}
		children = append(children, supervisor.ChildSpec{ID: group(g), Start: quorum.Child(spec)})
	}
	if _, err := supervisor.Start(context.Background(), n, supervisor.Spec{Strategy: supervisor.OneForOne, Children: children}); err != nil {
		log.Fatal(err)
	}
	nodes.Ready()
	select {}
}

func group(g int) string { return fmt.Sprintf("g%d", g) }

// led are the groups this node leads, as their machines are told.
var led sync.Map

// counter is a machine that tells when its member leads.
type counter struct{ group string }

func (counter) Init() int { return 0 }

func (counter) Apply(sum, n int) (int, int, []actor.Effect) { return sum + n, sum + n, nil }

func (c counter) Elected(int, uint64) []actor.Effect {
	led.Store(c.group, true)
	return nil
}

func (c counter) Surrendered(int) []actor.Effect {
	led.Delete(c.group)
	return nil
}

// leading answers which groups its node leads.
type leading struct{ genserver.Default[struct{}] }

type (
	leadersReq struct{}
	leaders    struct{ Groups []string }
)

func init() { dist.Register(leadersReq{}, leaders{}) }

func (leading) HandleCall(s struct{}, _ leadersReq, from genserver.From[leaders]) (struct{}, []actor.Effect) {
	var l leaders
	led.Range(func(g, _ any) bool {
		l.Groups = append(l.Groups, g.(string))
		return true
	})
	return s, actor.Do(from.Reply(l))
}

func ms(d time.Duration) string { return d.Round(time.Millisecond).String() }

func cmpOr(d, def time.Duration) time.Duration {
	if d == 0 {
		return def
	}
	return d
}

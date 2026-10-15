package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"strings"
	"sync/atomic"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/behaviours/supervisor"
	"github.com/shun159/molecule/dist"
	"github.com/shun159/molecule/proc"
	"github.com/shun159/quorum"
	"github.com/shun159/quorum/internal/register"
)

const (
	cookie  = "linearizable"
	control = "control@localhost"
)

// runNode runs the node name, with its member of the group, kept in dir,
// until killed. It finds the others where addrs says: through proxies.
func runNode(name string, addrs map[string]string, dir string) {
	n := proc.NewNode(name)
	if _, err := dist.Start(n, dist.Config{Listen: addrs[name], Cookie: cookie, Resolve: resolver(addrs)}); err != nil {
		log.Fatal(err)
	}
	spec := quorum.Spec[int, int, int]{Group: group, Machine: machine{}, Dir: dir, SnapshotEvery: 100}
	_, err := supervisor.Start(context.Background(), n, supervisor.Spec{
		Strategy: supervisor.OneForAll,
		Children: []supervisor.ChildSpec{
			{ID: "leading", Start: genserver.Child(leading{}, actor.WithName(actor.Local("leading")))},
			{ID: "storage", Start: quorum.Storage(dir)},
			{ID: "member", Start: quorum.Child(spec)},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("ready")
	select {}
}

// leads tells whether the member of this node leads, as its machine is
// told.
var leads atomic.Bool

// machine is the register, telling when its member leads.
type machine struct{ register.Machine }

func (machine) Elected(int, uint64) []actor.Effect {
	leads.Store(true)
	return nil
}

func (machine) Surrendered(int) []actor.Effect {
	leads.Store(false)
	return nil
}

// leading answers whether the member of its node leads.
type leading struct{ genserver.Default[struct{}] }

type leadsReq struct{}

func init() { dist.Register(leadsReq{}) }

func (leading) HandleCall(s struct{}, _ leadsReq, from genserver.From[bool]) (struct{}, []actor.Effect) {
	return s, actor.Do(from.Reply(leads.Load()))
}

func freeAddr() string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

func resolver(addrs map[string]string) func(string) (string, bool) {
	return func(node string) (string, bool) { a, ok := addrs[node]; return a, ok }
}

func parseAddrs(s string) map[string]string {
	addrs := map[string]string{}
	for _, pair := range strings.Split(s, ",") {
		name, addr, _ := strings.Cut(pair, "=")
		addrs[name] = addr
	}
	return addrs
}

func formatAddrs(addrs map[string]string) string {
	var pairs []string
	for name, addr := range addrs {
		pairs = append(pairs, name+"="+addr)
	}
	return strings.Join(pairs, ",")
}

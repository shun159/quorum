package quorum_test

import (
	"context"
	"fmt"
	"log"
	"maps"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/supervisor"
	"github.com/shun159/molecule/dist"
	"github.com/shun159/molecule/proc"
	"github.com/shun159/quorum"
)

// kv is a map of strings: a command sets a key to a value, and replies
// with the value it had; a read replies with the value of a key.
type kv struct{}

// op is a command, or a read, of kv.
type op struct{ Key, Value string }

// Its types go between nodes, and to disk.
func init() { dist.Register(op{}) }

func (kv) Init() map[string]string { return map[string]string{} }

func (kv) Apply(m map[string]string, o op) (map[string]string, string, []actor.Effect) {
	old := m[o.Key]
	m = maps.Clone(m) // the state given is left as it is
	m[o.Key] = o.Value
	return m, old, nil
}

func (kv) Read(m map[string]string, o op) string { return m[o.Key] }

// A map replicated over a group, of one node here: a group of three bears
// one failing, of five, two.
func Example() {
	ctx := context.Background()
	n := proc.NewNode("a@localhost")
	spec := quorum.Spec[map[string]string, op, string]{Group: "kv", Machine: kv{}}
	_, err := supervisor.Start(ctx, n, supervisor.Spec{
		Strategy: supervisor.OneForOne,
		Children: []supervisor.ChildSpec{{ID: "kv", Start: quorum.Child(spec)}},
	})
	if err != nil {
		log.Fatal(err)
	}

	nodes := []string{"a@localhost"}
	if err := quorum.Found(ctx, n, "kv", nodes); err != nil {
		log.Fatal(err)
	}
	if _, err := quorum.Command[string](ctx, n, "kv", nodes, op{Key: "lang", Value: "Go"}); err != nil {
		log.Fatal(err)
	}
	old, _ := quorum.Command[string](ctx, n, "kv", nodes, op{Key: "lang", Value: "Erlang"})
	now, _ := quorum.Read[string](ctx, n, "kv", nodes, op{Key: "lang"})
	fmt.Println(old, "then", now)
	// Output: Go then Erlang
}

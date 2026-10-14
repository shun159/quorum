# quorum

quorum replicates a state machine over the nodes of a group, by the Raft
consensus algorithm, on [molecule](https://github.com/shun159/molecule).

The machine is pure: it takes a state and a command and returns the next
state, a reply, and effects, which only the leader performs. A whole group
runs in a gensim simulation as well, where the safety of Raft is checked
under partitions and lost messages, in orders searched by the fuzzer.

## Machines

quorum replicates any state machine: a map, a lock service, a store of
configuration, a counter handing out numbers, a queue, a job one node runs
at a time. A machine is what a group is for, as a behaviour is what a
process is for: quorum agrees on the commands, the machine decides what
they do. It suits state that must stay consistent, of a moderate size,
kept by a few nodes, any of which may fail.

What it asks of a machine:

- **Deterministic.** Apply depends on the state and the command only, not
  on the clock or randomness, for every member to come to the same state.
  A machine needing the time is `Timed`, given the leader's.
- **Effects from the leader only.** The leader performs them as it applies a
  command; they are not replicated, and about a change of leader those of a
  command may be performed by none, or by two. What the leader does outside
  the group carries its term, as a fencing token.
- **Registered types.** The state, commands and replies go between nodes and
  to disk: their types are registered with `dist.Register`, and only their
  exported fields are kept.
- **A moderate size.** The state is held whole in memory, and sent whole to
  a member that needs it. quorum is not a store of much data.
- **One leader a group.** A group applies its commands one at a time; a load
  too much for one is spread over groups, by a hash of a key say, which
  agree on nothing together.

## Example

A counter, replicated over three nodes. The machine is the state, how a
command changes it, and the reply:

```go
type Counter struct{}

func (Counter) Init() int { return 0 }

func (Counter) Apply(sum, n int) (int, int, []actor.Effect) {
	return sum + n, sum + n, nil // next state, reply, effects of the leader
}
```

On each node, the storage of the node and the member of the group run
under a supervisor; `dir` is where the node keeps what it must not forget:

```go
spec := quorum.Spec[int, int, int]{Group: "counter", Machine: Counter{}, Dir: dir}
supervisor.Spec{
	Strategy: supervisor.OneForAll,
	Children: []supervisor.ChildSpec{
		{ID: "storage", Start: quorum.Storage(dir)},
		{ID: "counter", Start: quorum.Child(spec)},
	},
}
```

Once the three are up, the group is founded once, from any node; then any
node sends commands, which the group applies on every member alike:

```go
nodes := []string{"a@localhost", "b@localhost", "c@localhost"}
err := quorum.Found(ctx, node, "counter", nodes)
sum, err := quorum.Command[int](ctx, node, "counter", nodes, 5) // 5
```

A behaviour, which must not block, sends commands with a `quorum.Client`
instead, each applied once however often it is sent again.

[examples/locks](examples/locks) is a lock service on three nodes: its
locks spread over eight groups, each led on any node, which agree on who
holds each lock, with fencing tokens, while any one node is down.

Work in progress: see the package documentation for what there is not yet.

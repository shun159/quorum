# quorum

quorum replicates a state machine over the nodes of a group, by the Raft
consensus algorithm, on [molecule](https://github.com/shun159/molecule):
any state machine, of the application's own types, run in the
application's own processes. An application builds on it what must stay
one and the same across its nodes, and go on while some of them fail.

The machine is pure: it takes a state and a command and returns the next
state, a reply, and effects, which only the leader performs. A whole group
runs in a gensim simulation as well, where the safety of Raft is checked
under partitions and lost messages, in orders searched by the fuzzer; and
over real processes, killed, paused and cut off, checked linearizable.

## What it is for

etcd, or ZooKeeper, is a server of keys and values that applications use
from outside: their state is put into its keys, kept consistent by its
transactions. quorum is a library: the application's state is the
machine's, a lock table, a queue of jobs, a routing table, the steps of a
workflow, replicated as it is; the leader acts on the world for the group,
a singleton without a coordinator; and a member is a molecule process, in
the application's supervision tree, tested in its simulations.

It is not a database. The state of a group is held in memory, the
application's to shape: of the order of a GB a group, as measured below.
Writes go through one leader a group, and spread over groups as a key
says: groups cost nothing idle, and a node holds thousands. etcd takes
more writes in its one group, and serves keys, ranges, watches and leases
ready-made; quorum is what the application's own machine needs, under it.

## Machines

quorum replicates any state machine: a map, a lock service, a store of
configuration, a counter handing out numbers, a queue, a job one node runs
at a time. A machine is what a group is for, as a behaviour is what a
process is for: quorum agrees on the commands, the machine decides what
they do. It suits state that must stay consistent, held in memory, kept
by a group of three or five nodes, any of which may fail.

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
- **Held in memory.** The state is held whole in memory, never changed in
  place: a large one is better of a structure that changes at the cost of
  what changed, as molecule's `pmap.Map`, and an `Encoder`, encoding itself
  for its snapshots, which are written in the background and sent in
  chunks.
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

## How far it goes

Measured on one 8-core laptop, the three nodes of a group and the clients
on it, a group on an NVMe disk, with the commands in
[internal/cmd](internal/cmd):

- **A command** takes about 2 ms, synced on a majority of disks; a group
  takes some 9,000 a second from 64 clients at once (in memory, without
  disks: 25,000).
- **Many groups**: 100 groups take some 7,000 commands a second together
  on disk (30,000 in memory); idle, a thousand groups a node cost no
  messages and 0.3% of a core.
- **A million keys** of 100 bytes in a group (`internal/cmd/kv`): taken at
  4,500 a second, about 1 GB of memory a node, written among at 4,000 a
  second, p99 100 ms; the group writes again 3 s after all its nodes were
  killed.
- **A state of 500 MB** is sent to a member added in 1.3 s, the other
  groups of the nodes undisturbed.
- **A node lost**: its groups elect other leaders in 0.5 s, killed; 1–1.5 s,
  frozen silent.

Work in progress.

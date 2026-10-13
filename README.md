# quorum

quorum replicates a state machine over the nodes of a group, by the Raft
consensus algorithm, on [molecule](https://github.com/shun159/molecule).

The machine is pure: it takes a state and a command and returns the next
state, a reply, and effects, which only the leader performs. A whole group
runs in a gensim simulation as well, where the safety of Raft is checked
under partitions and lost messages, in orders searched by the fuzzer.

Work in progress: see the package documentation for what there is not yet.

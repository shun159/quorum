// Package raft is what package quorum hides: the member of a group, by the
// Raft consensus algorithm, and its clients.
//
// A member is a genstatem, pure, its writes to its Storage effects; gensim
// runs groups of them in the tests, through crashes, partitions, losses
// and disks replaced. It has Raft's election, with Pre-Vote and etcd's
// CheckQuorum; replication, durable before acknowledged; snapshots, sent
// whole; changes of members in the log, one at a time, learners promoted
// once caught up; reads by ReadIndex; the client sessions of Raft's thesis
// (6.3), by which a command sent again is applied once; and groups going
// quiet when idle, see quiet.go.
//
// quorum.Spec, Machine and Client are the faces of Spec, Machine and
// Client here; quorum documents them.
package raft

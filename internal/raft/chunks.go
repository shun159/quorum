package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/proc"
)

// The leader sends its snapshot encoded, in chunks, the next once the
// member has the one before: one large message would hold up the messages
// of every other group between the two nodes until it is through.

// snapshotChunk is the size in bytes of a chunk of a snapshot.
const snapshotChunk = 1 << 20

// receiving is the snapshot a member receives, so far.
type receiving struct {
	leader proc.PID
	index  uint64
	data   []byte
}

// sendSnapshot sends p the first chunk of the leader's snapshot, unless
// it is sending it one; then, the chunk on its way again, if the member
// has not answered it for an election timeout: it or its answer was lost.
func (b behaviour[S, Cmd, Rep]) sendSnapshot(d *replica[S], p peer) []actor.Effect {
	switch {
	case p.sending == 0:
		data, err := d.encodedSnapshot()
		if err != nil {
			return actor.Do(actor.Stop{Reason: fmt.Errorf("quorum: group %s cannot encode its snapshot: %w", b.spec.Group, err)})
		}
		q := d.peer(p.id)
		q.sending, q.data, q.offset = d.snapshot.Index, data, 0
	case d.round-p.chunkIn < checksPerTimeout:
		return nil
	}
	return actor.Do(b.chunkTo(d, p.id))
}

// encodedSnapshot returns the snapshot of the leader encoded, as it sends
// it to another member already, if it does.
func (d *replica[S]) encodedSnapshot() ([]byte, error) {
	for _, p := range d.peers {
		if p.sending == d.snapshot.Index {
			return p.data, nil
		}
	}
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(d.snapshot)
	return buf.Bytes(), err
}

func decodeSnapshot(data []byte) (Snapshot, error) {
	var snap Snapshot
	err := gob.NewDecoder(bytes.NewReader(data)).Decode(&snap)
	return snap, err
}

// chunkTo sends the member id the chunk of the snapshot after what it
// has.
func (b behaviour[S, Cmd, Rep]) chunkTo(d *replica[S], id proc.PID) actor.Effect {
	p := d.peer(id)
	p.chunkIn = d.round
	end := min(p.offset+b.spec.chunk, len(p.data))
	return b.send(id, installSnapshot{
		Term:   d.term,
		Leader: d.id,
		To:     id,
		Index:  p.sending,
		Offset: p.offset,
		Size:   len(p.data),
		Data:   p.data[p.offset:end],
	})
}

// snapshotAck sends the member the chunk after what it has, once it has
// more: or from the first, if it has none, having lost the others.
func (b behaviour[S, Cmd, Rep]) snapshotAck(r role, d replica[S], m snapshotAck) (role, replica[S], []actor.Effect) {
	if r != leader || m.Term != d.term {
		return r, d, nil
	}
	p := d.peer(m.From)
	if p == nil {
		return r, d, nil
	}
	d.heard(p)
	more := m.Offset > p.offset || m.Offset == 0 && p.offset > 0
	if p.sending != m.Index || m.Offset >= len(p.data) || !more {
		return r, d, nil
	}
	p.offset = m.Offset
	return r, d, actor.Do(b.chunkTo(&d, p.id))
}

// receive adds the chunk m to the snapshot the member receives, if it
// follows what it has, and returns how much of the snapshot it has. The
// first chunk of another begins it anew; any other of another, it has
// none of.
func (d *replica[S]) receive(m installSnapshot) int {
	in := d.incoming
	if in.leader != m.Leader || in.index != m.Index {
		if m.Offset != 0 {
			return 0
		}
		in = receiving{leader: m.Leader, index: m.Index, data: make([]byte, 0, m.Size)}
		d.incoming = in
	}
	if len(in.data) == m.Offset {
		d.incoming.data = append(in.data, m.Data...)
	}
	return len(d.incoming.data)
}

// snapshotArrived notes that the snapshot p was sent arrived, once m
// tells p holds the log as far. A reply sent before it arrived does not.
func (p *peer) snapshotArrived(m appendReply) {
	if p.sending != 0 && max(m.Match, m.LastIndex) >= p.sending {
		p.sending, p.data, p.offset = 0, nil, 0
	}
}

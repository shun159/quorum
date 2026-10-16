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
		data, err := b.encodedSnapshot(d)
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

func (e Encoded) len() int { return len(e.Head) + len(e.State) }

// slice returns the bytes of the snapshot encoded from from to to: Head
// and State one after the other, never copied together but for the chunk
// across them.
func (e Encoded) slice(from, to int) []byte {
	h := len(e.Head)
	switch {
	case to <= h:
		return e.Head[from:to]
	case from >= h:
		return e.State[from-h : to-h]
	}
	return append(e.Head[from:h:h], e.State[:to-h]...)
}

// encodedSnapshot returns the snapshot of the leader encoded, as it sends
// it to another member already, if it does.
func (b behaviour[S, Cmd, Rep]) encodedSnapshot(d *replica[S]) (Encoded, error) {
	for _, p := range d.peers {
		if p.sending == d.snapshot.Index {
			return p.data, nil
		}
	}
	return b.encode(d.snapshot)
}

// encode encodes snap: its state as the machine encodes it, if it is an
// Encoder, apart from the rest, which gob encodes.
func (b behaviour[S, Cmd, Rep]) encode(snap Snapshot) (Encoded, error) {
	var e Encoded
	if m, ok := b.spec.Machine.(Encoder[S]); ok {
		state, err := m.EncodeState(snap.State.(S))
		if err != nil {
			return e, err
		}
		e.State, snap.State = state, nil
	}
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(snap)
	e.Head = buf.Bytes()
	return e, err
}

// decode returns the snapshot e encodes.
func (b behaviour[S, Cmd, Rep]) decode(e Encoded) (Snapshot, error) {
	var snap Snapshot
	if err := gob.NewDecoder(bytes.NewReader(e.Head)).Decode(&snap); err != nil {
		return snap, err
	}
	if m, ok := b.spec.Machine.(Encoder[S]); ok {
		s, err := m.DecodeState(e.State)
		snap.State = s
		return snap, err
	}
	return snap, nil
}

// chunkTo sends the member id the chunk of the snapshot after what it
// has.
func (b behaviour[S, Cmd, Rep]) chunkTo(d *replica[S], id proc.PID) actor.Effect {
	p := d.peer(id)
	p.chunkIn = d.round
	end := min(p.offset+b.spec.chunk, p.data.len())
	return b.send(id, installSnapshot{
		Term:   d.term,
		Leader: d.id,
		To:     id,
		Index:  p.sending,
		Offset: p.offset,
		Size:   p.data.len(),
		Head:   len(p.data.Head),
		Data:   p.data.slice(p.offset, end),
	})
}

// snapshotAck sends the member the chunk after what it has, once it has
// more: or from the first, if it has none, having lost the others.
func (b behaviour[S, Cmd, Rep]) snapshotAck(r role, d *replica[S], m snapshotAck) (role, []actor.Effect) {
	if r != leader || m.Term != d.term {
		return r, nil
	}
	p := d.peer(m.From)
	if p == nil {
		return r, nil
	}
	d.heard(p)
	more := m.Offset > p.offset || m.Offset == 0 && p.offset > 0
	if p.sending != m.Index || m.Offset >= p.data.len() || !more {
		return r, nil
	}
	p.offset = m.Offset
	return r, actor.Do(b.chunkTo(d, p.id))
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
		p.sending, p.data, p.offset = 0, Encoded{}, 0
	}
}

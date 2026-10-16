package raft

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/shun159/molecule/dist"
	"github.com/shun159/molecule/proc"
)

// The messages between members encode themselves, as dist sends a binary
// type, at a fraction of the cost of gob: see dist.AppendAny. The commands
// of the entries go by dist.AppendAny too: of a type it cannot encode, the
// message goes by gob, as before.

// encoder appends the fields of a message, keeping the first error.
type encoder struct {
	b   []byte
	err error
}

func (e *encoder) uint(x uint64) { e.b = binary.AppendUvarint(e.b, x) }
func (e *encoder) int(x int64)   { e.b = binary.AppendVarint(e.b, x) }

func (e *encoder) bool(x bool) {
	if x {
		e.b = append(e.b, 1)
	} else {
		e.b = append(e.b, 0)
	}
}

func (e *encoder) bytes(x []byte) {
	e.uint(uint64(len(x)))
	e.b = append(e.b, x...)
}

func (e *encoder) any(v any) {
	if e.err == nil {
		e.b, e.err = dist.AppendAny(e.b, v)
	}
}

func (e *encoder) pid(p proc.PID) { e.any(p) }

func (e *encoder) pids(ps []proc.PID) {
	e.uint(uint64(len(ps)))
	for _, p := range ps {
		e.pid(p)
	}
}

func (e *encoder) done() ([]byte, error) { return e.b, e.err }

// decoder reads the fields of a message, keeping the first error: past it,
// each reads a zero.
type decoder struct {
	b   []byte
	err error
}

var errShort = errors.New("quorum: message cut short")

func (d *decoder) uint() uint64 {
	x, n := binary.Uvarint(d.b)
	return d.advance(x, n)
}

func (d *decoder) int() int64 {
	x, n := binary.Varint(d.b)
	return int64(d.advance(uint64(x), n))
}

func (d *decoder) advance(x uint64, n int) uint64 {
	if d.err != nil || n <= 0 {
		d.fail(errShort)
		return 0
	}
	d.b = d.b[n:]
	return x
}

func (d *decoder) bool() bool {
	if d.err != nil || len(d.b) == 0 {
		d.fail(errShort)
		return false
	}
	x := d.b[0] == 1
	d.b = d.b[1:]
	return x
}

// bytes reads a copy of the bytes, nil if none.
func (d *decoder) bytes() []byte {
	l := d.uint()
	if d.err != nil || uint64(len(d.b)) < l {
		d.fail(errShort)
		return nil
	}
	if l == 0 {
		return nil
	}
	x := append([]byte(nil), d.b[:l]...)
	d.b = d.b[l:]
	return x
}

func (d *decoder) any() any {
	if d.err != nil {
		return nil
	}
	v, rest, err := dist.ReadAny(d.b)
	if err != nil {
		d.fail(err)
		return nil
	}
	d.b = rest
	return v
}

func (d *decoder) pid() proc.PID {
	v := d.any()
	p, ok := v.(proc.PID)
	if !ok && d.err == nil {
		d.fail(fmt.Errorf("quorum: a %T for a PID", v))
	}
	return p
}

// pids reads PIDs, nil if none.
func (d *decoder) pids() []proc.PID {
	var ps []proc.PID
	for n := d.uint(); n > 0 && d.err == nil; n-- {
		ps = append(ps, d.pid())
	}
	return ps
}

func (d *decoder) fail(err error) {
	if d.err == nil {
		d.err = err
	}
}

// done tells the first error, if any, or that bytes are left.
func (d *decoder) done() error {
	if d.err == nil && len(d.b) > 0 {
		return fmt.Errorf("quorum: %d bytes left of a message", len(d.b))
	}
	return d.err
}

func (m requestVote) AppendBinary(b []byte) ([]byte, error) {
	e := encoder{b: b}
	e.uint(m.Term)
	e.pid(m.Candidate)
	e.uint(m.LastIndex)
	e.uint(m.LastTerm)
	return e.done()
}

func (m *requestVote) DecodeBinary(b []byte) error {
	d := decoder{b: b}
	m.Term = d.uint()
	m.Candidate = d.pid()
	m.LastIndex = d.uint()
	m.LastTerm = d.uint()
	return d.done()
}

func (m voteReply) AppendBinary(b []byte) ([]byte, error) {
	e := encoder{b: b}
	e.uint(m.Term)
	e.pid(m.From)
	e.bool(m.Granted)
	return e.done()
}

func (m *voteReply) DecodeBinary(b []byte) error {
	d := decoder{b: b}
	m.Term = d.uint()
	m.From = d.pid()
	m.Granted = d.bool()
	return d.done()
}

// preVote and preVoteReply are as those they are of.

func (m preVote) AppendBinary(b []byte) ([]byte, error) { return requestVote(m).AppendBinary(b) }
func (m *preVote) DecodeBinary(b []byte) error          { return (*requestVote)(m).DecodeBinary(b) }

func (m preVoteReply) AppendBinary(b []byte) ([]byte, error) { return voteReply(m).AppendBinary(b) }
func (m *preVoteReply) DecodeBinary(b []byte) error          { return (*voteReply)(m).DecodeBinary(b) }

func (m appendEntries) AppendBinary(b []byte) ([]byte, error) {
	e := encoder{b: b}
	e.uint(m.Term)
	e.pid(m.Leader)
	e.pid(m.To)
	e.uint(m.PrevIndex)
	e.uint(m.PrevTerm)
	e.uint(uint64(len(m.Entries)))
	for _, x := range m.Entries {
		e.entry(x)
	}
	e.uint(m.Commit)
	e.uint(m.Read)
	e.bool(m.Quiet)
	return e.done()
}

func (m *appendEntries) DecodeBinary(b []byte) error {
	d := decoder{b: b}
	m.Term = d.uint()
	m.Leader = d.pid()
	m.To = d.pid()
	m.PrevIndex = d.uint()
	m.PrevTerm = d.uint()
	for n := d.uint(); n > 0 && d.err == nil; n-- {
		m.Entries = append(m.Entries, d.entry())
	}
	m.Commit = d.uint()
	m.Read = d.uint()
	m.Quiet = d.bool()
	return d.done()
}

func (e *encoder) entry(x Entry) {
	e.uint(x.Term)
	e.int(x.Time)
	e.any(x.Cmd)
	e.uint(x.From.Session)
	e.uint(x.From.Seq)
	e.uint(x.From.Acked)
	e.bool(x.Config != nil)
	if x.Config != nil {
		e.pids(x.Config.Voters)
		e.pids(x.Config.Learners)
	}
	e.bool(x.Open)
}

func (d *decoder) entry() Entry {
	var e Entry
	e.Term = d.uint()
	e.Time = d.int()
	e.Cmd = d.any()
	e.From.Session = d.uint()
	e.From.Seq = d.uint()
	e.From.Acked = d.uint()
	if d.bool() {
		e.Config = &Config{Voters: d.pids(), Learners: d.pids()}
	}
	e.Open = d.bool()
	return e
}

func (m appendReply) AppendBinary(b []byte) ([]byte, error) {
	e := encoder{b: b}
	e.uint(m.Term)
	e.pid(m.From)
	e.bool(m.Success)
	e.uint(m.Match)
	e.uint(m.LastIndex)
	e.uint(m.Read)
	return e.done()
}

func (m *appendReply) DecodeBinary(b []byte) error {
	d := decoder{b: b}
	m.Term = d.uint()
	m.From = d.pid()
	m.Success = d.bool()
	m.Match = d.uint()
	m.LastIndex = d.uint()
	m.Read = d.uint()
	return d.done()
}

func (m installSnapshot) AppendBinary(b []byte) ([]byte, error) {
	e := encoder{b: b}
	e.uint(m.Term)
	e.pid(m.Leader)
	e.pid(m.To)
	e.uint(m.Index)
	e.int(int64(m.Offset))
	e.int(int64(m.Size))
	e.int(int64(m.Head))
	e.bytes(m.Data)
	return e.done()
}

func (m *installSnapshot) DecodeBinary(b []byte) error {
	d := decoder{b: b}
	m.Term = d.uint()
	m.Leader = d.pid()
	m.To = d.pid()
	m.Index = d.uint()
	m.Offset = int(d.int())
	m.Size = int(d.int())
	m.Head = int(d.int())
	m.Data = d.bytes()
	return d.done()
}

func (m snapshotAck) AppendBinary(b []byte) ([]byte, error) {
	e := encoder{b: b}
	e.uint(m.Term)
	e.pid(m.From)
	e.uint(m.Index)
	e.int(int64(m.Offset))
	return e.done()
}

func (m *snapshotAck) DecodeBinary(b []byte) error {
	d := decoder{b: b}
	m.Term = d.uint()
	m.From = d.pid()
	m.Index = d.uint()
	m.Offset = int(d.int())
	return d.done()
}

func (m timeoutNow) AppendBinary(b []byte) ([]byte, error) {
	e := encoder{b: b}
	e.uint(m.Term)
	e.pid(m.Leader)
	e.pid(m.To)
	return e.done()
}

func (m *timeoutNow) DecodeBinary(b []byte) error {
	d := decoder{b: b}
	m.Term = d.uint()
	m.Leader = d.pid()
	m.To = d.pid()
	return d.done()
}

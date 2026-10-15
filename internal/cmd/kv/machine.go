package main

import (
	"encoding/binary"
	"errors"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/dist"
	"github.com/shun159/molecule/pmap"
)

// kv is a machine of keys and values: a Put sets a key, and replies how
// many keys there are. It encodes its state itself, as a large one should.
type kv struct{}

// Put sets Key to Val.
type Put struct {
	Key string
	Val []byte
}

type state = pmap.Map[string, []byte]

func init() { dist.Register(Put{}, state{}) }

func (kv) Init() state { return state{} }

func (kv) Apply(m state, p Put) (state, int, []actor.Effect) {
	m = m.Set(p.Key, p.Val)
	return m, m.Len(), nil
}

// EncodeState writes the count of keys, then each key and value, prefixed
// by their lengths.
func (kv) EncodeState(m state) ([]byte, error) {
	b := binary.AppendUvarint(nil, uint64(m.Len()))
	for k, v := range m.All() {
		b = binary.AppendUvarint(b, uint64(len(k)))
		b = append(b, k...)
		b = binary.AppendUvarint(b, uint64(len(v)))
		b = append(b, v...)
	}
	return b, nil
}

var errState = errors.New("kv: a state cut short")

func (kv) DecodeState(b []byte) (state, error) {
	n, i := binary.Uvarint(b)
	if i <= 0 {
		return state{}, errState
	}
	b = b[i:]
	var err error
	next := func() []byte {
		l, i := binary.Uvarint(b)
		if i <= 0 || uint64(len(b)-i) < l {
			err = errState
			return nil
		}
		v := b[i : i+int(l) : i+int(l)]
		b = b[i+int(l):]
		return v
	}
	m := pmap.Collect(func(yield func(string, []byte) bool) {
		for range n {
			k := next()
			v := next()
			if err != nil || !yield(string(k), v) {
				return
			}
		}
	})
	return m, err
}

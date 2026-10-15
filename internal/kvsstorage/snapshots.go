package kvsstorage

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/gen"
	"github.com/shun159/molecule/kvs"
	"github.com/shun159/quorum/internal/raft"
)

// The snapshot of a member is kept in a file of its own, beside the table,
// which holds all it holds in memory: a snapshot is the whole state of a
// machine. The record of the snapshot tells its index; its file, in
// snapshots/ under the directory of the table, then the name of the group,
// is named by the index. A file is written whole, synced, then renamed:
// it is there whole or not at all.
//
// A file holds the length of the head, the head, the state, and a CRC of
// all before it.

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// snapshotEffect saves a snapshot: in the member's process, its writes in
// order with the member's others; or in the background, apart, as the
// member saves one to compact its log.
type snapshotEffect struct {
	actor.Extension
	s          storage
	index      uint64
	encode     func() (raft.Encoded, error)
	background bool
	tag        any
}

func (e snapshotEffect) Perform(env gen.Env) {
	t, err := e.s.table()
	if err != nil {
		env.Send(env.Self(), kvs.Durable{Tag: e.tag, Err: err})
		return
	}
	if e.background {
		env.Async(func(context.Context) (any, error) {
			return nil, e.s.saveSnapshot(t, e.index, e.encode, t.Writes)
		}, func(_ any, err error) any { return kvs.Durable{Tag: e.tag, Err: err} })
		return
	}
	made := false
	err = e.s.saveSnapshot(t, e.index, e.encode, func(writes ...write) error {
		made = true
		t.WritesEffect(e.tag, writes...).(gen.Performer).Perform(env)
		return nil
	})
	if err != nil || !made {
		env.Send(env.Self(), kvs.Durable{Tag: e.tag, Err: err})
	}
}

// saveSnapshot writes the snapshot of index encode encodes to its file, and
// has commit make its record and delete the entries up to it, unless a later
// snapshot is saved already: under the lock of the group, for two saved at
// once, one in the background, not to go back.
func (s storage) saveSnapshot(t *Table, index uint64, encode func() (raft.Encoded, error), commit func(...write) error) error {
	enc, err := encode()
	if err != nil {
		return err
	}
	dir := s.snapshotDir(t)
	tmp, err := writeTemp(dir, enc)
	if err != nil {
		return err
	}
	mu := s.lock(dir)
	mu.Lock()
	defer mu.Unlock()
	if r, ok := t.Get(s.key(KindSnapshot, 0)); ok && r.Snapshot >= index {
		return os.Remove(tmp)
	}
	if err := place(dir, tmp, index); err != nil {
		os.Remove(tmp)
		return err
	}
	writes := []write{put(s.key(KindSnapshot, 0), Record{Snapshot: index})}
	for _, k := range s.keys(t, 1) {
		if k.Index > index {
			break
		}
		writes = append(writes, del(k))
	}
	return commit(writes...)
}

// locks are the locks of the snapshots of the groups, by directory.
var locks sync.Map

func (s storage) lock(dir string) *sync.Mutex {
	mu, _ := locks.LoadOrStore(dir, &sync.Mutex{})
	return mu.(*sync.Mutex)
}

// snapshotDir is the directory of the snapshots of the group in the
// table t.
func (s storage) snapshotDir(t *Table) string {
	return filepath.Join(t.Info().Dir, "snapshots", url.PathEscape(s.group))
}

func snapshotFile(dir string, index uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%020d.snap", index))
}

// writeTemp writes enc to a file of its own in dir, synced, and returns
// its name, for place to put it in place.
func writeTemp(dir string, enc raft.Encoded) (string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	f, err := os.CreateTemp(dir, "snapshot-*.tmp")
	if err != nil {
		return "", err
	}
	header := binary.BigEndian.AppendUint64(nil, uint64(len(enc.Head)))
	crc := crc32.Update(crc32.Update(crc32.Checksum(header, castagnoli), castagnoli, enc.Head), castagnoli, enc.State)
	for _, b := range [][]byte{header, enc.Head, enc.State, binary.BigEndian.AppendUint32(nil, crc)} {
		if _, err = f.Write(b); err != nil {
			break
		}
	}
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(f.Name())
		return "", err
	}
	return f.Name(), nil
}

// place puts tmp in place as the file of the snapshot of index in dir,
// and deletes the files of the snapshots but the two latest before it,
// one of which the record may name until that of this one is durable.
func place(dir, tmp string, index uint64) error {
	if err := prune(dir, 2); err != nil {
		return err
	}
	if err := os.Rename(tmp, snapshotFile(dir, index)); err != nil {
		return err
	}
	return syncDir(dir)
}

var errSnapshotFile = errors.New("kvsstorage: a snapshot file cut short or changed")

// readSnapshot reads the snapshot of index from its file in dir.
func readSnapshot(dir string, index uint64) (raft.Encoded, error) {
	b, err := os.ReadFile(snapshotFile(dir, index))
	if err != nil {
		return raft.Encoded{}, err
	}
	if len(b) < 12 {
		return raft.Encoded{}, errSnapshotFile
	}
	body, sum := b[:len(b)-4], binary.BigEndian.Uint32(b[len(b)-4:])
	head := binary.BigEndian.Uint64(body)
	if crc32.Checksum(body, castagnoli) != sum || head > uint64(len(body)-8) {
		return raft.Encoded{}, errSnapshotFile
	}
	return raft.Encoded{Head: body[8 : 8+head], State: body[8+head:]}, nil
}

// prune deletes the files of the snapshots in dir but the keep latest.
func prune(dir string, keep int) error {
	snaps, err := filepath.Glob(filepath.Join(dir, "*.snap"))
	if err != nil {
		return err
	}
	slices.Sort(snaps) // by index: the names are of a width
	for _, name := range snaps[:max(0, len(snaps)-keep)] {
		if err := os.Remove(name); err != nil {
			return err
		}
	}
	return nil
}

// removeTemps deletes the files a write cut short left in dir: as the
// member starts, writing none.
func removeTemps(dir string) {
	tmps, _ := filepath.Glob(filepath.Join(dir, "*.tmp"))
	for _, name := range tmps {
		os.Remove(name)
	}
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

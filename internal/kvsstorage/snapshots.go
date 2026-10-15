package kvsstorage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

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

// snapshotDir is the directory of the snapshots of the group in the
// table t.
func (s storage) snapshotDir(t *Table) string {
	return filepath.Join(t.Info().Dir, "snapshots", url.PathEscape(s.group))
}

func snapshotFile(dir string, index uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%020d.snap", index))
}

// writeSnapshot writes enc, the snapshot of index, to its file in dir. It
// keeps the two latest files before, one of which the record may name
// until that of this one is durable, and deletes the others.
func writeSnapshot(dir string, index uint64, enc raft.Encoded) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	if err := prune(dir, 2); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, "snapshot-*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name()) // if not renamed
	header := binary.BigEndian.AppendUint64(nil, uint64(len(enc.Head)))
	crc := crc32.Update(crc32.Update(crc32.Checksum(header, castagnoli), castagnoli, enc.Head), castagnoli, enc.State)
	for _, b := range [][]byte{header, enc.Head, enc.State, binary.BigEndian.AppendUint32(nil, crc)} {
		if _, err := f.Write(b); err != nil {
			f.Close()
			return err
		}
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(f.Name(), snapshotFile(dir, index)); err != nil {
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

// prune deletes the files in dir but the keep latest snapshots: those of
// earlier snapshots, and those a write cut short left.
func prune(dir string, keep int) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	var snaps []string
	for _, e := range entries {
		name := e.Name()
		if index, ok := strings.CutSuffix(name, ".snap"); ok {
			if _, err := strconv.ParseUint(index, 10, 64); err == nil {
				snaps = append(snaps, name)
				continue
			}
		}
		if err := os.Remove(filepath.Join(dir, name)); err != nil {
			return err
		}
	}
	slices.Sort(snaps) // by index: the names are of a width
	for _, name := range snaps[:max(0, len(snaps)-keep)] {
		if err := os.Remove(filepath.Join(dir, name)); err != nil {
			return err
		}
	}
	return nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

package kvsstorage

import (
	"os"
	"strings"

	"github.com/shun159/molecule/kvs"
)

// The groups started on a node at runtime are kept in its table too, a
// record each, under the name of the group after registered, which sorts
// them before the records of the members.
const registered = "\x00"

func registration(group string) Key { return Key{Group: registered + group} }

// Register keeps in the table in dir that group was started on the node.
func Register(dir, group string) error {
	t, err := kvs.Opened[Key, Record](dir)
	if err != nil {
		return err
	}
	return t.Put(registration(group), Record{})
}

// Registered returns the groups started on the node of the table in dir,
// and those being deleted from it, by a Forget a crash cut short.
func Registered(dir string) (started, deleting []string, err error) {
	t, err := kvs.Opened[Key, Record](dir)
	if err != nil {
		return nil, nil, err
	}
	for k, r := range t.Range(Key{Group: registered}, Key{Group: "\x01"}) {
		group := strings.TrimPrefix(k.Group, registered)
		if r.Deleting {
			deleting = append(deleting, group)
		} else {
			started = append(started, group)
		}
	}
	return started, deleting, nil
}

// Forget deletes from the table in dir all the member of group on the
// node saved, and that the group was started on it. It marks the group
// being deleted first, for a crash amid it to leave it so, to Forget
// again, rather than a member blank, of no group.
func Forget(dir, group string) error {
	t, err := kvs.Opened[Key, Record](dir)
	if err != nil {
		return err
	}
	if err := t.Put(registration(group), Record{Deleting: true}); err != nil {
		return err
	}
	s := storage{t: t, group: group}
	var keys []Key
	for k := range t.Range(s.key(KindState, 0), s.end()) {
		keys = append(keys, k)
	}
	for i := len(keys) - 1; i >= 0; i-- {
		if err := t.Delete(keys[i]); err != nil {
			return err
		}
	}
	if err := os.RemoveAll(s.snapshotDir(t)); err != nil {
		return err
	}
	return t.Delete(registration(group))
}

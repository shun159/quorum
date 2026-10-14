package kvsstorage

import (
	"fmt"
	"path/filepath"
	"sync"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/gen"
	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/proc"
)

// Child returns the owner of the table kept in dir, made if missing, to
// start under a supervisor ahead of the members In it. It opens the table
// as it starts, failing to start if it cannot, and closes it as it stops.
func Child(dir string) gen.Child {
	return genserver.Child(owner{dir: clean(dir)})
}

// owner holds the table open in dir while it runs. It traps exits, for
// its supervisor stopping it to close the table.
type owner struct {
	genserver.Default[*Table]
	dir string
}

func (o owner) Init(proc.PID) (*Table, []actor.Effect, error) {
	t, err := Open(o.dir) // fails if open already, by flock
	if err != nil {
		return nil, nil, err
	}
	tables.Store(o.dir, t)
	return t, actor.Do(actor.TrapExit{On: true}), nil
}

func (o owner) Terminate(t *Table, _ error) []actor.Effect {
	tables.Delete(o.dir)
	t.Close()
	return nil
}

// tables are the tables open by owners, by directory.
var tables sync.Map

// opened returns the table open in dir.
func opened(dir string) (*Table, error) {
	if t, ok := tables.Load(dir); ok {
		return t.(*Table), nil
	}
	return nil, fmt.Errorf("kvsstorage: no table open in %s: kvsstorage.Child starts ahead of the members", dir)
}

// clean returns dir as tables knows it, absolute if it can be.
func clean(dir string) string {
	if abs, err := filepath.Abs(dir); err == nil {
		return abs
	}
	return filepath.Clean(dir)
}

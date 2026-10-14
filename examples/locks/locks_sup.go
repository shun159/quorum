package main

import (
	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/behaviours/supervisor"
	"github.com/shun159/molecule/proc"
	"github.com/shun159/quorum"
)

// locksSup is the top supervisor of the application: the group, and the
// holder of the node, showing on display what the node holds. Either
// restarts alone.
func locksSup(dir string, display proc.PID) supervisor.Spec {
	return supervisor.Spec{
		Strategy:  supervisor.OneForOne,
		Intensity: 5,
		Children: []supervisor.ChildSpec{
			{ID: "locks_group", Start: supervisor.Child(groupSup(dir))},
			{ID: "locks_holder", Start: genserver.Child(holder{Display: display}, actor.WithName(holderName))},
		},
	}
}

// groupSup supervises the storage in dir, then the member of the group,
// keeping its log in it. They restart together: the member stops on a
// write failed, after which the storage must open again. Restarted, the
// member rejoins with what it saved.
func groupSup(dir string) supervisor.Spec {
	return supervisor.Spec{
		Strategy:  supervisor.OneForAll,
		Intensity: 5,
		Children: []supervisor.ChildSpec{
			{ID: "locks_storage", Start: quorum.Storage(dir)},
			{ID: "locks_member", Start: quorum.Child(spec(dir))},
		},
	}
}

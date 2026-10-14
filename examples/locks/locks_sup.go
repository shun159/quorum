package main

import (
	"fmt"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/behaviours/supervisor"
	"github.com/shun159/molecule/proc"
	"github.com/shun159/quorum"
)

// locksSup is the top supervisor of the application: the groups, and the
// holder of the node, showing on display what the node holds and leads.
// Either restarts alone.
func locksSup(s Service, dir string, display proc.PID) supervisor.Spec {
	return supervisor.Spec{
		Strategy:  supervisor.OneForOne,
		Intensity: 5,
		Children: []supervisor.ChildSpec{
			{ID: "locks_groups", Start: supervisor.Child(groupsSup(s, dir))},
			{ID: "locks_holder", Start: genserver.Child(holder{Display: display}, actor.WithName(holderName))},
		},
	}
}

// groupsSup supervises the storage in dir, then the members of the
// groups, keeping their logs in it. They restart together: a member stops
// on a write failed, after which the storage must open again.
func groupsSup(s Service, dir string) supervisor.Spec {
	return supervisor.Spec{
		Strategy:  supervisor.OneForAll,
		Intensity: 5,
		Children: []supervisor.ChildSpec{
			{ID: "locks_storage", Start: quorum.Storage(dir)},
			{ID: "locks_members", Start: supervisor.Child(membersSup(s, dir))},
		},
	}
}

// membersSup supervises the members of the groups, each restarting alone,
// rejoining its group with what it saved. One stopping again at once, as
// on writes failed, makes it give up, and the storage open again with
// all.
func membersSup(s Service, dir string) supervisor.Spec {
	var children []supervisor.ChildSpec
	for i, spec := range s.specs(dir) {
		children = append(children, supervisor.ChildSpec{ID: fmt.Sprintf("locks_member_%d", i), Start: quorum.Child(spec)})
	}
	return supervisor.Spec{Strategy: supervisor.OneForOne, Children: children}
}

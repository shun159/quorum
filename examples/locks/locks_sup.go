package main

import (
	"github.com/shun159/molecule/behaviours/supervisor"
	"github.com/shun159/quorum"
)

// locksSup is the top supervisor of the application: the storage in dir,
// then the member of the group, keeping its log in it. They restart
// together: the member stops on a write failed, after which the storage
// must open again. Restarted, the member rejoins with what it saved.
func locksSup(dir string) supervisor.Spec {
	return supervisor.Spec{
		Strategy:  supervisor.OneForAll,
		Intensity: 5,
		Children: []supervisor.ChildSpec{
			{ID: "locks_storage", Start: quorum.Storage(dir)},
			{ID: "locks_member", Start: quorum.Child(spec(dir))},
		},
	}
}

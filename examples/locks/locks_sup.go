package main

import (
	"github.com/shun159/molecule/behaviours/supervisor"
	"github.com/shun159/quorum"
	"github.com/shun159/quorum/kvsstorage"
)

// locksSup is the top supervisor of the application: the table in dir,
// then the member of the group, keeping its log in it. They restart
// together: the member stops on a write failed, after which the table
// must be opened again. Restarted, the member rejoins with what it saved.
func locksSup(dir string) supervisor.Spec {
	return supervisor.Spec{
		Strategy:  supervisor.OneForAll,
		Intensity: 5,
		Children: []supervisor.ChildSpec{
			{ID: "locks_storage", Start: kvsstorage.Child(dir)},
			{ID: "locks_member", Start: quorum.Child(spec(kvsstorage.In(dir, group)))},
		},
	}
}

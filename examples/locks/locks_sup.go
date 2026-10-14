package main

import (
	"github.com/shun159/molecule/behaviours/supervisor"
	"github.com/shun159/quorum"
	"github.com/shun159/quorum/kvsstorage"
)

// locksSup is the top supervisor of the application. The member of the
// group is its only child; restarted, it rejoins with what it saved.
func locksSup(table *kvsstorage.Table) supervisor.Spec {
	return supervisor.Spec{
		Strategy:  supervisor.OneForOne,
		Intensity: 5,
		Children: []supervisor.ChildSpec{
			{ID: "locks_member", Start: quorum.Child(spec(kvsstorage.For(table, group)))},
		},
	}
}

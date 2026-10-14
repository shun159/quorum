package main

import (
	"context"

	"github.com/shun159/molecule/application"
	"github.com/shun159/molecule/behaviours/supervisor"
	"github.com/shun159/molecule/proc"
	"github.com/shun159/quorum/kvsstorage"
)

// startLocks starts the application on n: its supervision tree, the
// member of the group keeping its log in table.
func startLocks(ctx context.Context, n *proc.Node, table *kvsstorage.Table) (*application.Running, error) {
	return application.Start(ctx, n, application.App{
		Name:  "locks",
		Start: supervisor.Child(locksSup(table)),
	})
}

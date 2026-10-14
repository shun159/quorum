package main

import (
	"context"

	"github.com/shun159/molecule/application"
	"github.com/shun159/molecule/behaviours/supervisor"
	"github.com/shun159/molecule/proc"
)

// startLocks starts the application on n: its supervision tree, the
// member of the group keeping its log in dir, and what the node holds
// shown on display.
func startLocks(ctx context.Context, n *proc.Node, dir string, display proc.PID) (*application.Running, error) {
	return application.Start(ctx, n, application.App{
		Name:  "locks",
		Start: supervisor.Child(locksSup(dir, display)),
	})
}

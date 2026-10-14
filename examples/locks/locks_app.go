package main

import (
	"context"

	"github.com/shun159/molecule/application"
	"github.com/shun159/molecule/behaviours/supervisor"
	"github.com/shun159/molecule/proc"
)

// startLocks starts the application of s on n: its supervision tree, the
// members of the groups keeping their logs in dir, and what the node holds
// and leads shown on display.
func startLocks(ctx context.Context, n *proc.Node, s Service, dir string, display proc.PID) (*application.Running, error) {
	return application.Start(ctx, n, application.App{
		Name:  "locks",
		Start: supervisor.Child(locksSup(s, dir, display)),
	})
}

package main

import (
	"context"

	"github.com/shun159/molecule/application"
	"github.com/shun159/molecule/behaviours/supervisor"
	"github.com/shun159/molecule/proc"
)

// startPubsub starts the application of s on n: its supervision tree, the
// member of the group keeping its log in dir.
func startPubsub(ctx context.Context, n *proc.Node, s Service, dir string) (*application.Running, error) {
	return application.Start(ctx, n, application.App{
		Name:  "pubsub",
		Start: supervisor.Child(pubsubSup(s, dir)),
	})
}

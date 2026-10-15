package main

import (
	"github.com/shun159/molecule/behaviours/supervisor"
	"github.com/shun159/quorum"
)

// pubsubSup is the top supervisor of the application: the member of the
// group with the storage of the node, and the subscribers of the node.
// Either restarts alone: a subscriber reads on through any member.
func pubsubSup(s Service, dir string) supervisor.Spec {
	spec := quorum.Spec[Topics, Op, Reply]{Group: group, Machine: machine{keep: 1000}, Dir: dir}
	return supervisor.Spec{
		Strategy:  supervisor.OneForOne,
		Intensity: 5,
		Children: []supervisor.ChildSpec{
			{ID: "pubsub_group", Start: supervisor.Child(supervisor.Spec{
				// A member stops on a write failed, after which the storage
				// must open again: they restart together.
				Strategy: supervisor.OneForAll,
				Children: []supervisor.ChildSpec{
					{ID: "pubsub_storage", Start: quorum.Storage(dir)},
					{ID: "pubsub_member", Start: quorum.Child(spec)},
				},
			}), Type: supervisor.Supervisor},
			{ID: "pubsub_subs", Start: supervisor.DynamicChild(supervisor.DynamicSpec{Name: subsName}), Type: supervisor.Supervisor},
		},
	}
}

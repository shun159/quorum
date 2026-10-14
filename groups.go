package quorum

import (
	"context"
	"errors"
	"fmt"
	"maps"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/gen"
	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/behaviours/supervisor"
	"github.com/shun159/molecule/dist"
	"github.com/shun159/molecule/proc"
	"github.com/shun159/quorum/internal/kvsstorage"
)

// Groups returns the keeper of the groups started on its node at runtime,
// by CreateGroup or StartMember, to start under a supervisor after the
// node's Storage in dir. member makes the member of a group, from its
// name: the members run under the keeper, which keeps in the storage the
// groups started on the node, and starts them again as it starts.
//
//	{ID: "storage", Start: quorum.Storage(dir)},
//	{ID: "groups", Start: quorum.Groups(dir, func(group string) (gen.Child, error) {
//		return quorum.Child(quorum.Spec[S, Cmd, Rep]{Group: group, Machine: m, Dir: dir}), nil
//	})},
func Groups(dir string, member func(group string) (gen.Child, error)) gen.Child {
	return keeperStarter{dir: dir, member: member}
}

// CreateGroup starts the member of group on each of nodes, by the Groups
// of the node, and founds the group with them, as Found does. Should it
// fail midway, it can be run again.
func CreateGroup(ctx context.Context, caller actor.Caller, group string, nodes []string) error {
	for _, n := range nodes {
		if err := StartMember(ctx, caller, group, n); err != nil {
			return err
		}
	}
	return Found(ctx, caller, group, nodes)
}

// DeleteGroup stops the member of group on each of nodes, and deletes what
// it saved. Should it fail midway, it can be run again.
func DeleteGroup(ctx context.Context, caller actor.Caller, group string, nodes []string) error {
	for _, n := range nodes {
		if err := DeleteMember(ctx, caller, group, n); err != nil {
			return err
		}
	}
	return nil
}

// StartMember starts the member of group on node, by the Groups of the
// node, if it does not run there already: on a blank storage, it waits to
// be added to the group, see AddMember, or founded.
func StartMember(ctx context.Context, caller actor.Caller, group, node string) error {
	return askKeeper(ctx, caller, node, keeperReq{Group: group})
}

// DeleteMember stops the member of group on node, by the Groups of the
// node, and deletes what it saved there. A member of the group still, it
// is one lost: RemoveMember removes it first.
func DeleteMember(ctx context.Context, caller actor.Caller, group, node string) error {
	return askKeeper(ctx, caller, node, keeperReq{Group: group, Delete: true})
}

// The names of the keeper of the groups, and of the supervisor of their
// members, on every node: apart from the names of the members.
const (
	keeperName  = actor.Local("quorum.groups")
	membersName = actor.Local("quorum.members")
)

// keeperReq asks the keeper to start the member of Group, or to delete it;
// keeperRep answers, with the error, if any.
type (
	keeperReq struct {
		Group  string
		Delete bool
	}
	keeperRep struct{ Err string }
)

func init() { dist.Register(keeperReq{}, keeperRep{}) }

func askKeeper(ctx context.Context, caller actor.Caller, node string, req keeperReq) error {
	ref := genserver.NewRef[keeperReq, keeperRep, genserver.None](actor.Remote{Node: node, Name: string(keeperName)})
	rep, err := ref.Call(ctx, caller, req)
	if err != nil {
		return fmt.Errorf("quorum: group %s on %s: %w", req.Group, node, err)
	}
	if rep.Err != "" {
		return errors.New(rep.Err)
	}
	return nil
}

// keeperStarter starts the keeper and the supervisor of the members, its
// node known as it starts.
type keeperStarter struct {
	dir    string
	member func(group string) (gen.Child, error)
}

func (s keeperStarter) StartLink(ctx context.Context, parent *proc.Self) (proc.PID, error) {
	k := keeper{node: parent.Node(), dir: s.dir, member: s.member}
	return s.spec(k).StartLink(ctx, parent)
}

func (s keeperStarter) Simulate(env gen.Env) (gen.Runner, any, actor.StartOptions) {
	return s.spec(keeper{dir: s.dir, member: s.member}).Simulate(env)
}

// spec is the members, then the keeper starting them: restarted, the
// members start the keeper again, which starts them again.
func (keeperStarter) spec(k keeper) gen.Child {
	return supervisor.Child(supervisor.Spec{
		Strategy: supervisor.RestForOne,
		Children: []supervisor.ChildSpec{
			{ID: "members", Start: supervisor.DynamicChild(supervisor.DynamicSpec{Name: membersName}), Shutdown: supervisor.Infinity},
			{ID: "keeper", Start: genserver.Child(k, actor.WithName(keeperName), actor.WithMaxAsync(1))},
		},
	})
}

// keeper keeps the groups started on its node, in the storage in dir, and
// starts and stops their members, one at a time, under the supervisor of
// the members. Starting a member, a call, is done aside, as an Async.
type keeper struct {
	genserver.Default[map[any]genserver.From[keeperRep]]
	node   *proc.Node
	dir    string
	member func(group string) (gen.Child, error)
}

// restarting is the key of the start of the groups started before.
type restarting struct{}

func (k keeper) Init(proc.PID) (map[any]genserver.From[keeperRep], []actor.Effect, error) {
	return nil, actor.Do(actor.Async{Key: restarting{}, Run: k.restart}), nil
}

func (k keeper) HandleCall(waiting map[any]genserver.From[keeperRep], req keeperReq, from genserver.From[keeperRep]) (map[any]genserver.From[keeperRep], []actor.Effect) {
	waiting = maps.Clone(waiting)
	if waiting == nil {
		waiting = map[any]genserver.From[keeperRep]{}
	}
	key := from.Tag
	waiting[key] = from
	run := func(context.Context) (any, error) { return nil, k.start(req.Group) }
	if req.Delete {
		run = func(context.Context) (any, error) { return nil, k.delete(req.Group) }
	}
	return waiting, actor.Do(actor.Async{Key: key, Run: run})
}

func (k keeper) HandleInfo(waiting map[any]genserver.From[keeperRep], msg any) (map[any]genserver.From[keeperRep], []actor.Effect) {
	r, ok := msg.(actor.AsyncResult)
	if !ok {
		return waiting, nil
	}
	if r.Key == (restarting{}) {
		if r.Err != nil {
			return waiting, actor.Do(actor.Stop{Reason: r.Err})
		}
		return waiting, nil
	}
	from, ok := waiting[r.Key]
	if !ok {
		return waiting, nil
	}
	waiting = maps.Clone(waiting)
	delete(waiting, r.Key)
	var rep keeperRep
	if r.Err != nil {
		rep.Err = r.Err.Error()
	}
	return waiting, actor.Do(from.Reply(rep))
}

// restart starts the members of the groups started on the node before,
// and deletes those a crash left half deleted.
func (k keeper) restart(context.Context) (any, error) {
	started, deleting, err := kvsstorage.Registered(k.dir)
	if err != nil {
		return nil, err
	}
	for _, g := range deleting {
		if err := k.delete(g); err != nil {
			return nil, err
		}
	}
	for _, g := range started {
		if err := k.run(g); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

// start keeps that group is started on the node, and starts its member.
func (k keeper) start(group string) error {
	if err := kvsstorage.Register(k.dir, group); err != nil {
		return err
	}
	return k.run(group)
}

// run starts the member of group, unless it runs.
func (k keeper) run(group string) error {
	if _, ok := k.node.WhereIs(replicaName(group)); ok {
		return nil
	}
	child, err := k.member(group)
	if err != nil {
		return err
	}
	_, err = supervisor.StartChild(context.Background(), k.node, membersName, supervisor.ChildSpec{ID: group, Start: child})
	var started *actor.AlreadyStartedError
	if errors.As(err, &started) {
		return nil
	}
	return err
}

// delete stops the member of group, if it runs, and deletes what it saved,
// and that the group was started on the node.
func (k keeper) delete(group string) error {
	if pid, ok := k.node.WhereIs(replicaName(group)); ok {
		if err := supervisor.TerminateChild(context.Background(), k.node, membersName, pid); err != nil && !errors.Is(err, supervisor.ErrNotFound) {
			return err
		}
	}
	return kvsstorage.Forget(k.dir, group)
}

// replicaName is the name of the member of group on each node.
func replicaName(group string) string { return "quorum/" + group }

// Package register is the register the tests of quorum replicate, and
// checks with porcupine that a history of writes and reads of it is
// linearizable: in the simulation, and over real processes.
package register

import (
	"fmt"
	"math"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/shun159/molecule/actor"
)

// Machine is the register as a Machine: a command writes its number,
// replying with the number it held before; a read replies with the number
// it holds.
type Machine struct{}

func (Machine) Init() int { return 0 }

func (Machine) Apply(old, n int) (int, int, []actor.Effect) { return n, old, nil }

func (Machine) Read(n, _ int) int { return n }

// Op is an operation of a user: a write of Value, or a read, called at
// Call, returned at Return with Out, in nanoseconds of one clock; Done
// false, it ended with an error, or did not end, and may have been
// applied or not.
type Op struct {
	User         int
	Read         bool
	Value        int
	Call, Return int64
	Out          int
	Done         bool
}

// model is the register for porcupine: a write not done may have been
// applied or not, a read not done tells nothing.
var model = porcupine.Model{
	Init: func() any { return 0 },
	Step: func(state, input, output any) (bool, any) {
		o := input.(Op)
		done := output.(bool)
		switch {
		case o.Read:
			return !done || o.Out == state.(int), state
		case !done:
			return true, o.Value
		}
		return o.Out == state.(int), o.Value
	},
	DescribeOperation: func(input, output any) string {
		o := input.(Op)
		res := fmt.Sprint(o.Out)
		if !output.(bool) {
			res = "?"
		}
		if o.Read {
			return "read → " + res
		}
		return fmt.Sprintf("write %d → %s", o.Value, res)
	},
}

// Check checks that history is linearizable, for at most timeout.
func Check(history []Op, timeout time.Duration) (porcupine.CheckResult, porcupine.LinearizationInfo) {
	return porcupine.CheckOperationsVerbose(model, operations(history), timeout)
}

// Visualize writes what Check found of a history to an HTML file at path.
func Visualize(info porcupine.LinearizationInfo, path string) error {
	return porcupine.VisualizePath(model, info, path)
}

// Count returns how many operations of history were done, and how many
// returned an error, unknown to have been applied or not.
func Count(history []Op) (done, unknown int) {
	for _, o := range history {
		if o.Done {
			done++
		} else if o.Return != 0 {
			unknown++
		}
	}
	return done, unknown
}

// operations returns the history for porcupine: an operation not done
// returns after all the others, if ever; a read not done is left out.
func operations(history []Op) []porcupine.Operation {
	var ops []porcupine.Operation
	for _, o := range history {
		if o.Read && !o.Done {
			continue
		}
		ret := o.Return
		if !o.Done {
			ret = math.MaxInt64
		}
		ops = append(ops, porcupine.Operation{ClientId: o.User, Input: o, Call: o.Call, Output: o.Done, Return: ret})
	}
	return ops
}

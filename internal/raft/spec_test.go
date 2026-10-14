package raft

import (
	"strings"
	"testing"
	"time"

	"github.com/shun159/molecule/actor"
)

type nop struct{}

func (nop) Init() int                                          { return 0 }
func (nop) Apply(s int, _ struct{}) (int, int, []actor.Effect) { return s, s, nil }

func TestSpecDefaults(t *testing.T) {
	spec := Spec[int, struct{}, int]{}.withDefaults()
	if spec.ElectionTimeout != DefaultElectionTimeout || spec.Heartbeat != DefaultElectionTimeout/6 {
		t.Errorf("defaults: %v, %v", spec.ElectionTimeout, spec.Heartbeat)
	}
}

func TestSpecValidate(t *testing.T) {
	valid := Spec[int, struct{}, int]{Group: "g", Machine: nop{}}
	if err := valid.withDefaults().validate(); err != nil {
		t.Fatalf("valid spec: %v", err)
	}
	for _, c := range []struct {
		change func(*Spec[int, struct{}, int])
		want   string
	}{
		{func(s *Spec[int, struct{}, int]) { s.Group = "" }, "needs a name"},
		{func(s *Spec[int, struct{}, int]) { s.Machine = nil }, "no machine"},
		{func(s *Spec[int, struct{}, int]) { s.Heartbeat = time.Second }, "not under its election timeout"},
	} {
		spec := valid
		c.change(&spec)
		err := spec.withDefaults().validate()
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("got %v, want %q", err, c.want)
		}
	}
}

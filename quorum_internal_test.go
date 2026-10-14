package quorum

import (
	"errors"
	"testing"

	"github.com/shun159/quorum/internal/raft"
)

// The errors of the member are quorum's own.
func TestErrOf(t *testing.T) {
	other := errors.New("other")
	for in, want := range map[error]error{
		raft.ErrLeaderChanged:  ErrLeaderChanged,
		raft.ErrSessionExpired: ErrSessionExpired,
		other:                  other,
		nil:                    nil,
	} {
		if got := errOf(in); got != want {
			t.Errorf("errOf(%v) = %v, want %v", in, got, want)
		}
	}
}

// A result of the member is quorum's own.
func TestResultOf(t *testing.T) {
	if r := resultOf[int](nil); r != nil {
		t.Errorf("none: %+v", r)
	}
	r := resultOf(&raft.Result[int]{Tag: 1, Rep: 2, Err: raft.ErrSessionExpired})
	if *r != (Result[int]{Tag: 1, Rep: 2, Err: ErrSessionExpired}) {
		t.Errorf("result: %+v", r)
	}
}

// A Spec goes to the member with its defaults, and a storage if it has a
// Dir.
func TestSpecRaft(t *testing.T) {
	r := Spec[int, int, int]{Group: "g"}.raft()
	if r.ElectionTimeout != DefaultElectionTimeout || r.SnapshotEvery != DefaultSnapshotEvery ||
		r.SessionTimeout != DefaultSessionTimeout || r.Storage != nil {
		t.Errorf("without a Dir: %+v", r)
	}
	if r := (Spec[int, int, int]{Group: "g", Dir: t.TempDir()}).raft(); r.Storage == nil {
		t.Error("with a Dir: no storage")
	}
}

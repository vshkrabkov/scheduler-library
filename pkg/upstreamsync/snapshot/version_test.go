// Copyright The Kubernetes Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package snapshot

import (
	"testing"

	"k8s.io/kubernetes/pkg/scheduler/backend/cache"
)

// These tests pin the contract of the Backup/Restore/Commit session API that
// mirrors the MutableSnapshot proposal on kubernetes/kubernetes#140181. When
// the library switches from its internal undo log to the upstream
// undo-log-backed snapshot, this contract must hold unchanged.

func newVersionTestSnapshot() (*ClusterSnapshot, *int) {
	s := New(cache.NewEmptySnapshot(), nil)
	counter := 0
	return s, &counter
}

// mutate simulates one speculative snapshot mutation: it applies a state
// change (incrementing the counter) and registers its inverse in the log,
// which is exactly what schedulePods/PreemptPods do per scheduled/preempted pod.
func mutate(s *ClusterSnapshot, counter *int) {
	*counter++
	s.undoLog.registerOperation(func() { *counter-- })
}

func TestBackupIsStableWithoutMutations(t *testing.T) {
	s, _ := newVersionTestSnapshot()
	if v1, v2 := s.Backup(), s.Backup(); v1 != v2 {
		t.Fatalf("Backup changed without mutations: %d then %d", v1, v2)
	}
}

func TestRestoreRunsInversesBackToVersion(t *testing.T) {
	s, counter := newVersionTestSnapshot()

	v0 := s.Backup()
	mutate(s, counter)
	mutate(s, counter)
	v2 := s.Backup()
	mutate(s, counter)

	if *counter != 3 {
		t.Fatalf("expected 3 applied mutations, got %d", *counter)
	}
	if err := s.Restore(v2); err != nil {
		t.Fatalf("Restore(%d) failed: %v", v2, err)
	}
	if *counter != 2 {
		t.Fatalf("expected 2 applied mutations after partial restore, got %d", *counter)
	}
	if err := s.Restore(v0); err != nil {
		t.Fatalf("Restore(%d) failed: %v", v0, err)
	}
	if *counter != 0 {
		t.Fatalf("expected 0 applied mutations after full restore, got %d", *counter)
	}
}

func TestRestoreToFutureVersionFails(t *testing.T) {
	s, _ := newVersionTestSnapshot()
	if err := s.Restore(s.Backup() + 1); err == nil {
		t.Fatal("Restore to a future version should fail")
	}
}

func TestRestoreToCurrentVersionIsNoop(t *testing.T) {
	s, counter := newVersionTestSnapshot()
	mutate(s, counter)
	if err := s.Restore(s.Backup()); err != nil {
		t.Fatalf("Restore to current version failed: %v", err)
	}
	if *counter != 1 {
		t.Fatalf("Restore to current version must not revert anything; counter=%d", *counter)
	}
}

func TestCommitMakesOlderVersionsUnrestorable(t *testing.T) {
	s, counter := newVersionTestSnapshot()

	v0 := s.Backup()
	mutate(s, counter)
	s.Commit()

	if err := s.Restore(v0); err == nil {
		t.Fatal("Restore to a version older than the last Commit should fail")
	}
	if *counter != 1 {
		t.Fatalf("failed Restore must not change state; counter=%d", *counter)
	}

	// Versions after the commit stay restorable.
	v1 := s.Backup()
	mutate(s, counter)
	if err := s.Restore(v1); err != nil {
		t.Fatalf("Restore(%d) after commit failed: %v", v1, err)
	}
	if *counter != 1 {
		t.Fatalf("expected 1 applied mutation, got %d", *counter)
	}
}

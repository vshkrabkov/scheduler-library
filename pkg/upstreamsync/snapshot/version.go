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
	"fmt"
)

// This file defines the version-based snapshot session API on ClusterSnapshot:
// Backup, Restore, and Commit. The API mirrors the MutableSnapshot interface
// proposed on kubernetes/kubernetes#140181, where kube-scheduler's
// cache.Snapshot gains the same undo-log-backed mechanism internally
// (StartMutations/EndMutations become thin wrappers over it).
//
// TODO(kubernetes/kubernetes#140181): once the upstream snapshot exposes
// Backup/Restore/Commit through MutableSnapshotSharedLister, delete the
// library's internal undo log and delegate these three methods (and the
// mutation registration in schedulePods/PreemptPods) to the upstream snapshot.
// Everything else in this package is already written exclusively against this
// seam, so the switch is contained to this file.

// Backup returns the snapshot's current state version. It is O(1): no snapshot
// state is copied. Pass the returned version to Restore to revert every
// mutation recorded after this point.
func (s *ClusterSnapshot) Backup() uint64 {
	return s.undoLog.stateVersion
}

// Restore reverts all mutations recorded after version v, in LIFO order,
// returning the snapshot to the exact state it had when Backup returned v.
// It fails if v is in the future, or if it is no longer reachable because a
// Commit discarded the undo entries needed to reach it.
func (s *ClusterSnapshot) Restore(v uint64) error {
	if v > s.undoLog.stateVersion {
		return fmt.Errorf("cannot restore snapshot to version %d: it is newer than the current version %d", v, s.undoLog.stateVersion)
	}
	if v < s.undoLog.committedVersion {
		return fmt.Errorf("cannot restore snapshot to version %d: versions up to %d were committed and are unrestorable", v, s.undoLog.committedVersion)
	}
	s.undoLog.restoreState(v)
	return nil
}

// Commit discards the recorded undo entries, making all state versions older
// than the current one unrestorable. It bounds the memory growth of the undo
// log in long-running simulations.
func (s *ClusterSnapshot) Commit() {
	s.undoLog.commit()
}

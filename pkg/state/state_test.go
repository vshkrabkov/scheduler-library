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

package state

import (
	"fmt"
	"sync"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/events"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler"
	schedulerapi "k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/backend/cache"
	schedulerframework "k8s.io/kubernetes/pkg/scheduler/framework"
	plugins "k8s.io/kubernetes/pkg/scheduler/framework/plugins"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
	"k8s.io/kubernetes/pkg/scheduler/profile"
	"sigs.k8s.io/scheduler-library/pkg/snapshot"
)

func init() {
	metrics.Register()
}

func TestClusterState_AddPod(t *testing.T) {
	tests := []struct {
		name         string
		existingPods []*v1.Pod
		podToAdd     *v1.Pod
		expectCount  int
	}{
		{
			name: "add unassigned pod",
			podToAdd: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "pod1", Namespace: "default", UID: "uid1"},
			},
			expectCount: 1,
		},
		{
			name: "add assigned pod",
			podToAdd: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "pod1", Namespace: "default", UID: "uid1"},
				Spec:       v1.PodSpec{NodeName: "node1"},
			},
			expectCount: 1,
		},
		{
			name: "add duplicate pod",
			existingPods: []*v1.Pod{
				{ObjectMeta: metav1.ObjectMeta{Name: "pod1", Namespace: "default", UID: "uid1"}},
			},
			podToAdd: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "pod1", Namespace: "default", UID: "uid1"},
			},
			expectCount: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			logger := klog.FromContext(ctx)
			state := NewClusterState(cache.New(ctx, nil, false), nil, nil)

			for _, p := range tc.existingPods {
				if err := state.Cache.AddPod(logger, p); err != nil {
					t.Fatalf("Failed to add pod: %v", err)
				}
			}

			_ = state.Cache.AddPod(logger, tc.podToAdd)

			count, err := state.Cache.PodCount()
			if err != nil {
				t.Fatalf("Failed to get pod count: %v", err)
			}
			if count != tc.expectCount {
				t.Errorf("Expected pod count %d, got %d", tc.expectCount, count)
			}
		})
	}
}

func TestClusterState_RemovePod(t *testing.T) {
	tests := []struct {
		name         string
		existingPods []*v1.Pod
		podToRemove  *v1.Pod
		expectCount  int
	}{
		{
			name: "remove existing pod",
			existingPods: []*v1.Pod{
				{ObjectMeta: metav1.ObjectMeta{Name: "pod1", Namespace: "default", UID: "uid1"}},
			},
			podToRemove: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "pod1", Namespace: "default", UID: "uid1"},
			},
			expectCount: 0,
		},
		{
			name: "remove non-existent pod",
			existingPods: []*v1.Pod{
				{ObjectMeta: metav1.ObjectMeta{Name: "pod1", Namespace: "default", UID: "uid1"}},
			},
			podToRemove: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "pod2", Namespace: "default", UID: "uid2"},
			},
			expectCount: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			logger := klog.FromContext(ctx)
			state := NewClusterState(cache.New(ctx, nil, false), nil, nil)

			for _, p := range tc.existingPods {
				if err := state.Cache.AddPod(logger, p); err != nil {
					t.Fatalf("Failed to add pod: %v", err)
				}
			}

			_ = state.Cache.RemovePod(logger, tc.podToRemove)

			count, err := state.Cache.PodCount()
			if err != nil {
				t.Fatalf("Failed to get pod count: %v", err)
			}
			if count != tc.expectCount {
				t.Errorf("Expected pod count %d, got %d", tc.expectCount, count)
			}
		})
	}
}

func TestClusterState_AddNode(t *testing.T) {
	tests := []struct {
		name          string
		existingNodes []*v1.Node
		nodeToAdd     *v1.Node
		expectCount   int
	}{
		{
			name: "add valid node",
			nodeToAdd: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "node1"},
			},
			expectCount: 1,
		},
		{
			name: "add duplicate node",
			existingNodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1"}},
			},
			nodeToAdd: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "node1"},
			},
			expectCount: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			logger := klog.FromContext(ctx)
			state := NewClusterState(cache.New(ctx, nil, false), nil, nil)

			for _, n := range tc.existingNodes {
				state.Cache.AddNode(logger, n)
			}

			state.Cache.AddNode(logger, tc.nodeToAdd)

			count := state.Cache.NodeCount()
			if count != tc.expectCount {
				t.Errorf("Expected node count %d, got %d", tc.expectCount, count)
			}
		})
	}
}

func TestClusterState_RemoveNode(t *testing.T) {
	tests := []struct {
		name          string
		existingNodes []*v1.Node
		nodeToRemove  *v1.Node
		expectCount   int
	}{
		{
			name: "remove existing node",
			existingNodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1"}},
			},
			nodeToRemove: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "node1"},
			},
			expectCount: 0,
		},
		{
			name: "remove non-existent node",
			existingNodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1"}},
			},
			nodeToRemove: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "node2"},
			},
			expectCount: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			logger := klog.FromContext(ctx)
			state := NewClusterState(cache.New(ctx, nil, false), nil, nil)

			for _, n := range tc.existingNodes {
				state.Cache.AddNode(logger, n)
			}

			_ = state.Cache.RemoveNode(logger, tc.nodeToRemove)

			count := state.Cache.NodeCount()
			if count != tc.expectCount {
				t.Errorf("Expected node count %d, got %d", tc.expectCount, count)
			}
		})
	}
}

func TestClusterState_Snapshot(t *testing.T) {
	tests := []struct {
		name          string
		existingNodes []*v1.Node
		existingPods  []*v1.Pod
		hasFramework  bool
	}{
		{
			name: "empty snapshot",
		},
		{
			name: "snapshot with data",
			existingNodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1"}},
			},
			existingPods: []*v1.Pod{
				{ObjectMeta: metav1.ObjectMeta{Name: "pod1", Namespace: "default", UID: "uid1"}},
			},
		},
		{
			name: "snapshot in sync with framework snapshot",
			existingNodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1"}},
			},
			existingPods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "pod1", Namespace: "default", UID: "uid1"},
					Spec:       v1.PodSpec{NodeName: "node1"},
				},
			},
			hasFramework: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()

			var frameworks map[string]schedulerframework.Framework
			var sharedSnap *cache.Snapshot
			if tc.hasFramework {
				sharedSnap = cache.NewEmptySnapshot()
				informerFactory := informers.NewSharedInformerFactory(fake.NewClientset(), 0)
				registry := plugins.NewInTreeRegistry()
				profile := schedulerapi.KubeSchedulerProfile{
					SchedulerName: "default-scheduler",
				}
				fwk, err := frameworkruntime.NewFramework(ctx, registry, &profile,
					frameworkruntime.WithSnapshotSharedLister(sharedSnap),
					frameworkruntime.WithInformerFactory(informerFactory),
				)
				if err != nil {
					t.Fatalf("Failed to create framework: %v", err)
				}
				frameworks = map[string]schedulerframework.Framework{
					"default-scheduler": fwk,
				}
			}

			state := NewClusterState(cache.New(ctx, nil, false), frameworks, sharedSnap)
			logger := klog.FromContext(ctx)

			for _, n := range tc.existingNodes {
				state.Cache.AddNode(logger, n)
			}
			for _, p := range tc.existingPods {
				if err := state.Cache.AddPod(logger, p); err != nil {
					t.Fatalf("Failed to add pod: %v", err)
				}
			}

			snap, err := state.Snapshot(logger)
			if err != nil {
				t.Fatalf("Snapshot() error = %v", err)
			}
			if snap == nil {
				t.Fatal("Expected snapshot to be non-nil")
			}

			if tc.hasFramework {
				for _, n := range tc.existingNodes {
					nodeInfo, err := sharedSnap.NodeInfos().Get(n.Name)
					if err != nil {
						t.Fatalf("Failed to get node %s from shared framework snapshot: %v", n.Name, err)
					}
					if nodeInfo == nil {
						t.Fatalf("Expected nodeInfo for node %s to be non-nil", n.Name)
					}

					expectedPods := make(map[string]bool)
					for _, p := range tc.existingPods {
						if p.Spec.NodeName == n.Name {
							expectedPods[p.Name] = true
						}
					}

					pods := nodeInfo.GetPods()
					if len(pods) != len(expectedPods) {
						t.Fatalf("Expected %d pods on node %s in shared framework snapshot, got %d", len(expectedPods), n.Name, len(pods))
					}

					for _, podInfo := range pods {
						podName := podInfo.GetPod().Name
						if !expectedPods[podName] {
							t.Fatalf("Unexpected pod %s on node %s in shared framework snapshot", podName, n.Name)
						}
					}
				}
			}
		})
	}
}

func TestClusterState_ComplexScenarios(t *testing.T) {
	t.Run("pod assigned to non-existent node", func(t *testing.T) {
		ctx := t.Context()
		logger := klog.FromContext(ctx)
		state := NewClusterState(cache.New(ctx, nil, false), nil, nil)

		pod := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "pod1", Namespace: "default", UID: "uid1"},
			Spec:       v1.PodSpec{NodeName: "node1"},
		}

		// Add pod first
		if err := state.Cache.AddPod(logger, pod); err != nil {
			t.Fatalf("Failed to add pod: %v", err)
		}

		// Verify pod count
		pCount, err := state.Cache.PodCount()
		if err != nil {
			t.Fatal(err)
		}
		if pCount != 1 {
			t.Errorf("Expected pod count 1, got %d", pCount)
		}

		// Add node later
		node := &v1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "node1"},
		}
		state.Cache.AddNode(logger, node)

		// Verify node count
		nCount := state.Cache.NodeCount()
		if nCount != 1 {
			t.Errorf("Expected node count 1, got %d", nCount)
		}
	})

	t.Run("node removal with pods", func(t *testing.T) {
		ctx := t.Context()
		logger := klog.FromContext(ctx)
		state := NewClusterState(cache.New(ctx, nil, false), nil, nil)

		node := &v1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "node1"},
		}
		pod := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "pod1", Namespace: "default", UID: "uid1"},
			Spec:       v1.PodSpec{NodeName: "node1"},
		}

		state.Cache.AddNode(logger, node)
		if err := state.Cache.AddPod(logger, pod); err != nil {
			t.Fatalf("Failed to add pod: %v", err)
		}

		// Remove node
		if err := state.Cache.RemoveNode(logger, node); err != nil {
			t.Fatalf("Failed to remove node: %v", err)
		}

		// Verify node count is still 1 (ghost node) because pod still exists
		nCount := state.Cache.NodeCount()
		if nCount != 1 {
			t.Errorf("Expected node count 1 (ghost node), got %d", nCount)
		}

		// Verify pod count is still 1
		pCount, err := state.Cache.PodCount()
		if err != nil {
			t.Fatal(err)
		}
		if pCount != 1 {
			t.Errorf("Expected pod count 1, got %d", pCount)
		}

		// Now remove the pod
		if err := state.Cache.RemovePod(logger, pod); err != nil {
			t.Fatalf("Failed to remove pod: %v", err)
		}

		// Verify node count becomes 0 after pod is removed
		nCount = state.Cache.NodeCount()
		if nCount != 0 {
			t.Errorf("Expected node count 0 after pod removal, got %d", nCount)
		}
	})
}

func TestClusterState_ConcurrentAccess(t *testing.T) {
	ctx := t.Context()
	state := NewClusterState(cache.New(ctx, nil, false), nil, nil)

	const numGoroutines = 20
	const numOperations = 50

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			logger := klog.FromContext(ctx)
			for j := 0; j < numOperations; j++ {
				pod := &v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      fmt.Sprintf("pod-%d-%d", id, j),
						Namespace: "default",
						UID:       types.UID(fmt.Sprintf("uid-%d-%d", id, j)),
					},
				}
				node := &v1.Node{
					ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("node-%d-%d", id, j)},
				}

				state.Cache.AddNode(logger, node)
				if err := state.Cache.AddPod(logger, pod); err != nil {
					// Concurrent access might cause some pods to already exist if not careful,
					// but here names are unique by goroutine id and iteration j.
					t.Errorf("Failed to add pod: %v", err)
				}
				if _, err := state.Snapshot(logger); err != nil {
					t.Errorf("Snapshot() error = %v", err)
				}
				if err := state.Cache.RemovePod(logger, pod); err != nil {
					t.Errorf("Failed to remove pod: %v", err)
				}
				if err := state.Cache.RemoveNode(logger, node); err != nil {
					t.Errorf("Failed to remove node: %v", err)
				}
			}
		}(i)
	}

	wg.Wait()
}

func TestSnapshotIsolation(t *testing.T) {
	ctx := t.Context()
	logger := klog.FromContext(ctx)

	// 1. Create cluster state with a scheduler
	client := fake.NewClientset()
	informerFactory := informers.NewSharedInformerFactory(client, 0)
	broadcaster := events.NewBroadcaster(nil)
	recorderFactory := profile.NewRecorderFactory(broadcaster)

	prof := schedulerapi.KubeSchedulerProfile{
		SchedulerName: "default-scheduler",
		Plugins: &schedulerapi.Plugins{
			QueueSort: schedulerapi.PluginSet{
				Enabled: []schedulerapi.Plugin{
					{Name: "PrioritySort"},
				},
			},
			PreFilter: schedulerapi.PluginSet{
				Enabled: []schedulerapi.Plugin{
					{Name: "NodeResourcesFit"},
				},
			},
			Filter: schedulerapi.PluginSet{
				Enabled: []schedulerapi.Plugin{
					{Name: "NodeResourcesFit"},
				},
			},
			Bind: schedulerapi.PluginSet{
				Enabled: []schedulerapi.Plugin{
					{Name: "DefaultBinder"},
				},
			},
		},
		PluginConfig: []schedulerapi.PluginConfig{
			{
				Name: "NodeResourcesFit",
				Args: &schedulerapi.NodeResourcesFitArgs{
					ScoringStrategy: &schedulerapi.ScoringStrategy{
						Type: schedulerapi.LeastAllocated,
					},
				},
			},
		},
	}

	sched, err := scheduler.New(ctx,
		client,
		informerFactory,
		nil,
		recorderFactory,
		scheduler.WithProfiles(prof),
	)
	if err != nil {
		t.Fatalf("failed to create scheduler: %v", err)
	}

	// Get the shared snapshot instance from the scheduler's profile.
	var snap *cache.Snapshot
	for _, prof := range sched.Profiles {
		if s, ok := prof.SnapshotSharedLister().(*cache.Snapshot); ok {
			snap = s
			break
		}
	}
	if snap == nil {
		t.Fatalf("failed to get shared snapshot from scheduler")
	}

	state := NewClusterState(sched.Cache, sched.Profiles, snap)

	// Add a node to cache
	node1 := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node1"},
		Status: v1.NodeStatus{
			Allocatable: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse("10"),
				v1.ResourceMemory: resource.MustParse("10Gi"),
				v1.ResourcePods:   resource.MustParse("110"),
			},
			Capacity: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse("10"),
				v1.ResourceMemory: resource.MustParse("10Gi"),
				v1.ResourcePods:   resource.MustParse("110"),
			},
		},
	}
	state.Cache.AddNode(logger, node1)

	// 2. Create snapshot
	cs1, err := state.Snapshot(logger)
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}

	// 3. Run simulation
	vpod1 := snapshot.SchedulablePod{
		Pod: &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "vpod1", Namespace: "default", UID: "vuid1"},
			Spec: v1.PodSpec{
				SchedulerName: "default-scheduler",
				Containers: []v1.Container{{
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse("5"),
							v1.ResourceMemory: resource.MustParse("1Gi"),
						},
					},
				}},
			},
		},
		CandidateNodeNames: []string{"node1"},
	}

	results1, err := cs1.SchedulePods(ctx, sched, logger, []snapshot.SchedulablePod{vpod1}, snapshot.SchedulePodsOptions{})
	if err != nil {
		t.Fatalf("failed to run simulation 1: %v", err)
	}
	if len(results1) != 1 || !results1[0].Status.IsSuccess() {
		t.Fatalf("expected success in simulation 1, got: %+v", results1)
	}

	// Verify vpod1 is in Snapshot 1
	nodeInfo, err := snap.NodeInfos().Get("node1")
	if err != nil {
		t.Fatalf("failed to get node1 info from snap: %v", err)
	}
	found := false
	for _, p := range nodeInfo.GetPods() {
		if p.GetPod().Name == "vpod1" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("vpod1 not found on node1 in snapshot 1")
	}

	// Verify vpod1 is NOT in Cache
	count, _ := state.Cache.PodCount()
	if count != 0 {
		t.Fatalf("expected 0 pods in cache, got %d", count)
	}

	// 4. Create snapshot again
	cs2, err := state.Snapshot(logger)
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}

	// Verify vpod1 is NOT in Snapshot 2
	nodeInfo, err = snap.NodeInfos().Get("node1")
	if err != nil {
		t.Fatalf("failed to get node1 info from snap: %v", err)
	}
	found = false
	for _, p := range nodeInfo.GetPods() {
		if p.GetPod().Name == "vpod1" {
			found = true
			break
		}
	}
	if found {
		t.Fatalf("vpod1 found on node1 in snapshot 2, expected it to be cleared")
	}

	// 5. Run simulation again
	vpod2 := snapshot.SchedulablePod{
		Pod: &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "vpod2", Namespace: "default", UID: "vuid2"},
			Spec: v1.PodSpec{
				SchedulerName: "default-scheduler",
				Containers: []v1.Container{{
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse("5"),
							v1.ResourceMemory: resource.MustParse("1Gi"),
						},
					},
				}},
			},
		},
		CandidateNodeNames: []string{"node1"},
	}

	results2, err := cs2.SchedulePods(ctx, sched, logger, []snapshot.SchedulablePod{vpod2}, snapshot.SchedulePodsOptions{})
	if err != nil {
		t.Fatalf("failed to run simulation 2: %v", err)
	}
	if len(results2) != 1 || !results2[0].Status.IsSuccess() {
		t.Fatalf("expected success in simulation 2, got: %+v", results2)
	}
}

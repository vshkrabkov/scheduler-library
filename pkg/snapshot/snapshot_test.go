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
	"context"
	"fmt"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	toolscache "k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/events"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler"
	schedulerapi "k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/backend/cache"
	framework "k8s.io/kubernetes/pkg/scheduler/framework"
	plugins "k8s.io/kubernetes/pkg/scheduler/framework/plugins"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
	"k8s.io/kubernetes/pkg/scheduler/profile"
)

func init() {
	metrics.Register()
}

func TestPreemptionSnapshot(t *testing.T) {
	tests := []struct {
		name          string
		setupSnapshot func(cs *ClusterSnapshot)
		expectErr     bool
	}{
		{
			name: "Unpreempt succeeds when no state change",
			setupSnapshot: func(cs *ClusterSnapshot) {
				// Nothing to do
			},
			expectErr: false,
		},
		{
			name: "Unpreempt fails when transaction committed",
			setupSnapshot: func(cs *ClusterSnapshot) {
				cs.lastCommittedTx = "new-tx"
			},
			expectErr: true,
		},
		{
			name: "Unpreempt fails when new transaction started",
			setupSnapshot: func(cs *ClusterSnapshot) {
				cs.transactions = append(cs.transactions, "new-tx")
			},
			expectErr: true,
		},
		{
			name: "Unpreempt fails when mutation added to current tx",
			setupSnapshot: func(cs *ClusterSnapshot) {
				if cs.txCompensation == nil {
					cs.txCompensation = make(map[string][]func())
				}
				cs.txCompensation[""] = []func(){func() {}}
			},
			expectErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			registry := plugins.NewInTreeRegistry()
			prof := schedulerapi.KubeSchedulerProfile{}
			nodes := []*v1.Node{{ObjectMeta: metav1.ObjectMeta{Name: "node1"}}}
			snapshot := cache.NewSnapshot(nil, nodes)
			informerFactory := informers.NewSharedInformerFactory(fake.NewClientset(), 0)
			f, err := frameworkruntime.NewFramework(ctx, registry, &prof,
				frameworkruntime.WithSnapshotSharedLister(snapshot),
				frameworkruntime.WithInformerFactory(informerFactory),
			)
			if err != nil {
				t.Fatalf("failed to create framework: %v", err)
			}

			cs, err := NewClusterSnapshot(snapshot, map[string]framework.Framework{
				v1.DefaultSchedulerName: f,
			})
			if err != nil {
				t.Fatalf("failed to create cluster snapshot: %v", err)
			}
				ps := newPreemptionSnapshot(cs, []func(){})


			tc.setupSnapshot(cs)

			err = ps.Unpreempt()
			if (err != nil) != tc.expectErr {
				t.Errorf("Unpreempt() error = %v, expectErr %v", err, tc.expectErr)
			}
		})
	}
}

func TestUnpreemptRestoresPods(t *testing.T) {
	ctx := t.Context()
	logger := klog.FromContext(ctx)
	prof := schedulerapi.KubeSchedulerProfile{
		SchedulerName: v1.DefaultSchedulerName,
		Plugins: &schedulerapi.Plugins{
			QueueSort: schedulerapi.PluginSet{
				Enabled: []schedulerapi.Plugin{
					{Name: "PrioritySort"},
				},
			},
			Bind: schedulerapi.PluginSet{
				Enabled: []schedulerapi.Plugin{
					{Name: "DefaultBinder"},
				},
			},
		},
	}
	node := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node1"},
		Status: v1.NodeStatus{
			Allocatable: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse("10"),
				v1.ResourceMemory: resource.MustParse("10Gi"),
				v1.ResourcePods:   resource.MustParse("110"),
			},
		},
	}
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pod1", Namespace: "default", UID: "uid1"},
		Spec:       v1.PodSpec{NodeName: "node1"},
	}

	client := fake.NewClientset()
	informerFactory := informers.NewSharedInformerFactory(client, 0)
	broadcaster := events.NewBroadcaster(nil)
	recorderFactory := profile.NewRecorderFactory(broadcaster)
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

	// Populate cache
	sched.Cache.AddNode(logger, node)
	if err := sched.Cache.AssumePod(logger, pod); err != nil {
		t.Fatalf("failed to assume pod in cache: %v", err)
	}

	// Get shared snapshot from scheduler
	var snap *cache.Snapshot
	for _, p := range sched.Profiles {
		if s, ok := p.SnapshotSharedLister().(*cache.Snapshot); ok {
			snap = s
			break
		}
	}
	if snap == nil {
		t.Fatalf("failed to get shared snapshot from scheduler")
	}

	// Update snapshot from cache
	if err := sched.Cache.UpdateSnapshot(klog.FromContext(ctx), snap); err != nil {
		t.Fatalf("failed to update snapshot: %v", err)
	}

	cs, err := NewClusterSnapshot(snap, sched.Profiles)
	if err != nil {
		t.Fatalf("failed to create cluster snapshot: %v", err)
	}

	// Verify pod is on node before preemption.
	nodeInfo, err := snap.NodeInfos().Get("node1")
	if err != nil {
		t.Fatalf("failed to get node info: %v", err)
	}
	if len(nodeInfo.GetPods()) != 1 {
		t.Fatalf("expected 1 pod before preemption, got %d", len(nodeInfo.GetPods()))
	}

	// Preempt the pod.
	ps, err := cs.PreemptPods(ctx, sched, []*v1.Pod{pod})
	if err != nil {
		t.Fatalf("PreemptPods() error = %v", err)
	}

	// Verify pod is gone.
	nodeInfo, err = snap.NodeInfos().Get("node1")
	if err != nil {
		t.Fatalf("failed to get node info after preemption: %v", err)
	}
	if len(nodeInfo.GetPods()) != 0 {
		t.Errorf("expected 0 pods after preemption, got %d", len(nodeInfo.GetPods()))
	}

	// Restore via Unpreempt.
	if err := ps.Unpreempt(); err != nil {
		t.Fatalf("Unpreempt() error = %v", err)
	}

	// Verify pod is back.
	nodeInfo, err = snap.NodeInfos().Get("node1")
	if err != nil {
		t.Fatalf("failed to get node info after Unpreempt: %v", err)
	}
	if len(nodeInfo.GetPods()) != 1 {
		t.Errorf("expected 1 pod after Unpreempt, got %d", len(nodeInfo.GetPods()))
	}
}

func TestTransaction(t *testing.T) {
	tests := []struct {
		name             string
		transactionFn    func(cs *ClusterSnapshot, revertCalled *bool, state map[string]any) (TransactionResult, error)
		expectRevert     bool
		expectStackEmpty bool
		expectErr        bool
		checkResult      func(t *testing.T, revertCalled bool, state map[string]any)
	}{
		{
			name: "Commit succeeds and doesn't revert",
			transactionFn: func(cs *ClusterSnapshot, revertCalled *bool, state map[string]any) (TransactionResult, error) {
				var activeTx = cs.transactions[len(cs.transactions)-1]
				cs.txCompensation[activeTx] = append(cs.txCompensation[activeTx], func() {
					*revertCalled = true
				})
				return Commit, nil
			},
			expectRevert:     false,
			expectStackEmpty: true,
			expectErr:        false,
		},
		{
			name: "Revert requested and executed",
			transactionFn: func(cs *ClusterSnapshot, revertCalled *bool, state map[string]any) (TransactionResult, error) {
				var activeTx = cs.transactions[len(cs.transactions)-1]
				cs.txCompensation[activeTx] = append(cs.txCompensation[activeTx], func() {
					*revertCalled = true
				})
				return Revert, nil
			},
			expectRevert:     true,
			expectStackEmpty: true,
			expectErr:        false,
		},
		{
			name: "Revert on error",
			transactionFn: func(cs *ClusterSnapshot, revertCalled *bool, state map[string]any) (TransactionResult, error) {
				activeTx := cs.transactions[len(cs.transactions)-1]
				cs.txCompensation[activeTx] = append(cs.txCompensation[activeTx], func() {
					*revertCalled = true
				})
				return Commit, fmt.Errorf("some error")
			},
			expectRevert:     true,
			expectStackEmpty: true,
			expectErr:        true,
		},
		{
			name: "Multiple reverts called in reverse order",
			transactionFn: func(cs *ClusterSnapshot, revertCalled *bool, state map[string]any) (TransactionResult, error) {
				activeTx := cs.transactions[len(cs.transactions)-1]

				cs.txCompensation[activeTx] = append(cs.txCompensation[activeTx], func() {
					state["op1Called"] = true
					state["op1Order"] = state["order"]
					state["order"] = state["order"].(int) + 1
				})

				cs.txCompensation[activeTx] = append(cs.txCompensation[activeTx], func() {
					*revertCalled = true
					state["op2Order"] = state["order"]
					state["order"] = state["order"].(int) + 1
				})

				return Revert, nil
			},
			expectRevert:     true,
			expectStackEmpty: true,
			expectErr:        false,
			checkResult: func(t *testing.T, revertCalled bool, state map[string]any) {
				if state["op1Called"] != true {
					t.Errorf("Expected op1 to be called, but it wasn't")
				}
				if state["op2Order"].(int) >= state["op1Order"].(int) {
					t.Errorf("Expected op2 to be called before op1, but op2Order=%v, op1Order=%v", state["op2Order"], state["op1Order"])
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cs, err := NewClusterSnapshot(cache.NewEmptySnapshot(), nil)
			if err != nil {
				t.Fatalf("failed to create cluster snapshot: %v", err)
			}
			revertCalled := false
			state := make(map[string]any)
			state["order"] = 0
			ctx := t.Context()

			err = cs.Transaction(ctx, klog.FromContext(ctx), func() (TransactionResult, error) {
				return tc.transactionFn(cs, &revertCalled, state)
			})

			if (err != nil) != tc.expectErr {
				t.Errorf("Transaction() error = %v, expectErr %v", err, tc.expectErr)
			}

			if revertCalled != tc.expectRevert {
				t.Errorf("Expected revertCalled %v, got %v", tc.expectRevert, revertCalled)
			}

			if tc.expectStackEmpty && len(cs.transactions) != 0 {
				t.Errorf("Expected transactions stack to be empty, got %d", len(cs.transactions))
			}

			if tc.checkResult != nil {
				tc.checkResult(t, revertCalled, state)
			}
		})
	}
}

func TestCanSchedulePod(t *testing.T) {
	tests := []struct {
		name           string
		candidateNodes []string
		schedulerName  string
		expectNodes    []string
		expectErr      bool
	}{
		{
			name:           "Success - all nodes eligible",
			candidateNodes: []string{"node1", "node2"},
			expectNodes:    []string{"node1", "node2"},
			expectErr:      false,
		},
		{
			name:           "Error - unknown scheduler name",
			candidateNodes: []string{"node1"},
			schedulerName:  "unknown-scheduler",
			expectErr:      true,
		},
		{
			name:           "Success - empty candidate list returns empty result",
			candidateNodes: []string{},
			expectNodes:    nil,
			expectErr:      false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()

			registry := plugins.NewInTreeRegistry()
			prof := schedulerapi.KubeSchedulerProfile{}

			snapshotNodes := make([]*v1.Node, len(tc.candidateNodes))
			for i, name := range tc.candidateNodes {
				snapshotNodes[i] = &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}
			}
			snapshot := cache.NewSnapshot(nil, snapshotNodes)
			informerFactory := informers.NewSharedInformerFactory(fake.NewClientset(), 0)

			f, err := frameworkruntime.NewFramework(ctx, registry, &prof,
				frameworkruntime.WithSnapshotSharedLister(snapshot),
				frameworkruntime.WithInformerFactory(informerFactory),
			)
			if err != nil {
				t.Fatalf("failed to create framework: %v", err)
			}

			cs, err := NewClusterSnapshot(snapshot, map[string]framework.Framework{
				v1.DefaultSchedulerName: f,
			})
			if err != nil {
				t.Fatalf("failed to create cluster snapshot: %v", err)
			}

			pod := SchedulablePod{
				Pod: &v1.Pod{
					Spec: v1.PodSpec{SchedulerName: tc.schedulerName},
				},
				CandidateNodeNames: tc.candidateNodes,
			}

			nodes, err := cs.CanSchedulePod(ctx, klog.FromContext(ctx), pod)
			if (err != nil) != tc.expectErr {
				t.Fatalf("CanSchedulePod() error = %v, expectErr %v", err, tc.expectErr)
			}

			if !tc.expectErr {
				if len(nodes) != len(tc.expectNodes) {
					t.Errorf("Expected nodes %v, got %v", tc.expectNodes, nodes)
				} else {
					for i, n := range nodes {
						if n != tc.expectNodes[i] {
							t.Errorf("Expected nodes %v, got %v", tc.expectNodes, nodes)
							break
						}
					}
				}
			}
		})
	}
}

func TestSchedulePods(t *testing.T) {
	tests := []struct {
		name               string
		pods               []SchedulablePod
		opts               SchedulePodsOptions
		expectResults      int
		expectErr          bool
		expectTxLen        int // expected length of txCompensation for current tx
		inTransaction      bool
		unschedulableNodes []string
	}{
		{
			name: "Success - schedule one pod",
			pods: []SchedulablePod{
				{
					Pod: &v1.Pod{
						ObjectMeta: metav1.ObjectMeta{Name: "pod1", Namespace: "default", UID: "uid1"},
					},
					CandidateNodeNames: []string{"node1"},
				},
			},
			opts:          SchedulePodsOptions{},
			expectResults: 1,
			expectErr:     false,
		},
		{
			name: "DryRun - does not persist",
			pods: []SchedulablePod{
				{
					Pod: &v1.Pod{
						ObjectMeta: metav1.ObjectMeta{Name: "pod1", Namespace: "default", UID: "uid1"},
					},
					CandidateNodeNames: []string{"node1"},
				},
			},
			opts:          NewSchedulePodsOptions(true, false),
			expectResults: 1,
			expectErr:     false,
		},
		{
			name: "InTransaction - adds to compensation",
			pods: []SchedulablePod{
				{
					Pod: &v1.Pod{
						ObjectMeta: metav1.ObjectMeta{Name: "pod1", Namespace: "default", UID: "uid1"},
					},
					CandidateNodeNames: []string{"node1"},
				},
			},
			opts:          SchedulePodsOptions{},
			inTransaction: true,
			expectResults: 1,
			expectErr:     false,
			expectTxLen:   1,
		},
		{
			name: "StopOnFailure - fails on first pod",
			pods: []SchedulablePod{
				{
					Pod: &v1.Pod{
						ObjectMeta: metav1.ObjectMeta{Name: "pod1", Namespace: "default", UID: "uid1"},
					},
					CandidateNodeNames: []string{"non-existent-node"}, // will fail filter or node lookup
				},
			},
			opts:          NewSchedulePodsOptions(false, true),
			expectResults: 0,
			expectErr:     true,
		},
		{
			name: "Fails due to node unschedulable",
			pods: []SchedulablePod{
				{
					Pod: &v1.Pod{
						ObjectMeta: metav1.ObjectMeta{Name: "pod1", Namespace: "default", UID: "uid1"},
					},
					CandidateNodeNames: []string{"node1"},
				},
			},
			opts:               NewSchedulePodsOptions(false, true),
			expectResults:      0,
			expectErr:          true,
			unschedulableNodes: []string{"node1"},
		},
		{
			name: "StopOnFailure - succeeds on first, fails on second due to node unschedulable",
			pods: []SchedulablePod{
				{
					Pod: &v1.Pod{
						ObjectMeta: metav1.ObjectMeta{Name: "pod1", Namespace: "default", UID: "uid1"},
					},
					CandidateNodeNames: []string{"node1"},
				},
				{
					Pod: &v1.Pod{
						ObjectMeta: metav1.ObjectMeta{Name: "pod2", Namespace: "default", UID: "uid2"},
					},
					CandidateNodeNames: []string{"node2"},
				},
			},
			opts:               NewSchedulePodsOptions(false, true),
			expectResults:      1,    // Pod 1 succeeds on node1
			expectErr:          true, // Pod 2 fails on node2
			unschedulableNodes: []string{"node2"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			prof := schedulerapi.KubeSchedulerProfile{
				SchedulerName: v1.DefaultSchedulerName,
				Plugins: &schedulerapi.Plugins{
					QueueSort: schedulerapi.PluginSet{
						Enabled: []schedulerapi.Plugin{
							{Name: "PrioritySort"},
						},
					},
					Filter: schedulerapi.PluginSet{
						Enabled: []schedulerapi.Plugin{
							{Name: "NodeUnschedulable"},
						},
					},
					Bind: schedulerapi.PluginSet{
						Enabled: []schedulerapi.Plugin{
							{Name: "DefaultBinder"},
						},
					},
				},
			}
			var nodes []*v1.Node
			nodeMap := make(map[string]*v1.Node)
			unschedSet := make(map[string]struct{})
			for _, n := range tc.unschedulableNodes {
				unschedSet[n] = struct{}{}
			}
			for _, pod := range tc.pods {
				for _, nodeName := range pod.CandidateNodeNames {
					if nodeName != "non-existent-node" && nodeMap[nodeName] == nil {
						node := &v1.Node{
							ObjectMeta: metav1.ObjectMeta{Name: nodeName},
							Status: v1.NodeStatus{
								Allocatable: v1.ResourceList{
									v1.ResourceCPU:    resource.MustParse("10"),
									v1.ResourceMemory: resource.MustParse("10Gi"),
									v1.ResourcePods:   resource.MustParse("110"),
								},
							},
						}
						if _, ok := unschedSet[nodeName]; ok {
							node.Spec.Unschedulable = true
						}
						nodeMap[nodeName] = node
						nodes = append(nodes, node)
					}
				}
			}

			client := fake.NewClientset()
			for _, node := range nodes {
				client.CoreV1().Nodes().Create(ctx, node, metav1.CreateOptions{})
			}
			for _, pod := range tc.pods {
				client.CoreV1().Pods(pod.Pod.Namespace).Create(ctx, pod.Pod, metav1.CreateOptions{})
			}

			informerFactory := informers.NewSharedInformerFactory(client, 0)

			broadcaster := events.NewBroadcaster(nil)
			recorderFactory := profile.NewRecorderFactory(broadcaster)
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

			informerFactory.Start(ctx.Done())
			if !toolscache.WaitForCacheSync(ctx.Done(), informerFactory.Core().V1().Nodes().Informer().HasSynced) {
				t.Fatalf("failed to sync nodes")
			}

			// Wait for scheduler cache to populate
			err = wait.PollUntilContextTimeout(ctx, 10*time.Millisecond, 2*time.Second, true, func(ctx context.Context) (bool, error) {
				return sched.Cache.NodeCount() >= len(nodes), nil
			})
			if err != nil {
				t.Fatalf("scheduler cache failed to populate nodes: %v (got %d, want %d)", err, sched.Cache.NodeCount(), len(nodes))
			}

			// Get the shared snapshot instance from the scheduler's profile.
			var snapshot *cache.Snapshot
			for _, p := range sched.Profiles {
				if s, ok := p.SnapshotSharedLister().(*cache.Snapshot); ok {
					snapshot = s
					break
				}
			}
			if snapshot == nil {
				t.Fatalf("failed to get shared snapshot from scheduler")
			}

			// Update the shared snapshot from the cache.
			if err := sched.Cache.UpdateSnapshot(klog.FromContext(ctx), snapshot); err != nil {
				t.Fatalf("failed to update snapshot from cache: %v", err)
			}

			cs, err := NewClusterSnapshot(snapshot, sched.Profiles)
			if err != nil {
				t.Fatalf("failed to create cluster snapshot: %v", err)
			}

			if tc.inTransaction {
				cs.transactions = append(cs.transactions, "tx1")
				cs.txCompensation["tx1"] = []func(){}
			}

			results, err := cs.SchedulePods(ctx, sched, klog.FromContext(ctx), tc.pods, tc.opts)
			if (err != nil) != tc.expectErr {
				t.Fatalf("SchedulePods() error = %v, expectErr %v", err, tc.expectErr)
			}

			if len(results) != tc.expectResults {
				t.Errorf("Expected results %d, got %d", tc.expectResults, len(results))
			}

			if tc.inTransaction {
				txLen := len(cs.txCompensation["tx1"])
				if txLen != tc.expectTxLen {
					t.Errorf("Expected tx compensation len %d, got %d", tc.expectTxLen, txLen)
				}
			}
		})
	}
}

func TestSchedulePodsByTemplate(t *testing.T) {
	tests := []struct {
		name           string
		template       *v1.PodTemplateSpec
		candidateNodes []string
		maxPods        int
		opts           SchedulePodsByTemplateOptions
		expectResults  int
		expectErr      bool
		expectTxLen    int
		inTransaction  bool
	}{
		{
			name:           "Success - schedule maxPods",
			template:       &v1.PodTemplateSpec{Spec: v1.PodSpec{}},
			candidateNodes: []string{"node1", "node2"},
			maxPods:        2,
			opts:           SchedulePodsByTemplateOptions{},
			expectResults:  2,
			expectErr:      false,
		},
		{
			name:           "DryRun - does not persist",
			template:       &v1.PodTemplateSpec{Spec: v1.PodSpec{}},
			candidateNodes: []string{"node1"},
			maxPods:        2,
			opts:           NewSchedulePodsByTemplateOptions(true),
			expectResults:  2,
			expectErr:      false,
		},
		{
			name:           "InTransaction - adds to compensation",
			template:       &v1.PodTemplateSpec{Spec: v1.PodSpec{}},
			candidateNodes: []string{"node1"},
			maxPods:        1,
			opts:           SchedulePodsByTemplateOptions{},
			inTransaction:  true,
			expectResults:  1,
			expectErr:      false,
			expectTxLen:    1,
		},
		{
			name: "Custom Namespace",
			template: &v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Namespace: "custom-ns"},
				Spec:       v1.PodSpec{},
			},
			candidateNodes: []string{"node1"},
			maxPods:        1,
			opts:           SchedulePodsByTemplateOptions{},
			expectResults:  1,
			expectErr:      false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			prof := schedulerapi.KubeSchedulerProfile{
				SchedulerName: v1.DefaultSchedulerName,
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

			client := fake.NewClientset()
			var nodes []*v1.Node
			for _, nodeName := range tc.candidateNodes {
				n := &v1.Node{
					ObjectMeta: metav1.ObjectMeta{Name: nodeName},
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
				nodes = append(nodes, n)
				if _, err := client.CoreV1().Nodes().Create(ctx, n, metav1.CreateOptions{}); err != nil {
					t.Fatalf("failed to create node: %v", err)
				}
			}

			informerFactory := informers.NewSharedInformerFactory(client, 0)
			broadcaster := events.NewBroadcaster(nil)
			recorderFactory := profile.NewRecorderFactory(broadcaster)
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

			informerFactory.Start(ctx.Done())
			if !toolscache.WaitForCacheSync(ctx.Done(), informerFactory.Core().V1().Nodes().Informer().HasSynced, informerFactory.Core().V1().Pods().Informer().HasSynced) {
				t.Fatalf("failed to sync informers")
			}

			// Wait for scheduler cache to populate
			err = wait.PollUntilContextTimeout(ctx, 10*time.Millisecond, 2*time.Second, true, func(ctx context.Context) (bool, error) {
				return sched.Cache.NodeCount() >= len(nodes), nil
			})
			if err != nil {
				t.Fatalf("scheduler cache failed to populate nodes: %v (got %d, want %d)", err, sched.Cache.NodeCount(), len(nodes))
			}

			// Get the shared snapshot instance from the scheduler's profile.
			var snapshot *cache.Snapshot
			for _, p := range sched.Profiles {
				if s, ok := p.SnapshotSharedLister().(*cache.Snapshot); ok {
					snapshot = s
					break
				}
			}
			if snapshot == nil {
				t.Fatalf("failed to get shared snapshot from scheduler")
			}

			// Update the shared snapshot from the cache.
			if err := sched.Cache.UpdateSnapshot(klog.FromContext(ctx), snapshot); err != nil {
				t.Fatalf("failed to update snapshot from cache: %v", err)
			}

			cs, err := NewClusterSnapshot(snapshot, sched.Profiles)
			if err != nil {
				t.Fatalf("failed to create cluster snapshot: %v", err)
			}

			if tc.inTransaction {
				cs.transactions = append(cs.transactions, "tx1")
				cs.txCompensation["tx1"] = []func(){}
			}

			results, err := cs.SchedulePodsByTemplate(ctx, sched, klog.FromContext(ctx), tc.template, tc.candidateNodes, tc.maxPods, tc.opts)
			if (err != nil) != tc.expectErr {
				t.Fatalf("SchedulePodsByTemplate() error = %v, expectErr %v", err, tc.expectErr)
			}

			if len(results) != tc.expectResults {
				t.Errorf("Expected results %d, got %d", tc.expectResults, len(results))
			}

			if tc.inTransaction {
				txLen := len(cs.txCompensation["tx1"])
				if txLen != tc.expectTxLen {
					t.Errorf("Expected tx compensation len %d, got %d", tc.expectTxLen, txLen)
				}
			}
		})
	}
}

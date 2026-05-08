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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"fmt"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/klog/v2"
	schedulerapi "k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/backend/cache"
	framework "k8s.io/kubernetes/pkg/scheduler/framework"
	plugins "k8s.io/kubernetes/pkg/scheduler/framework/plugins"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
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
					cs.txCompensation = make(map[string][]func() error)
				}
				cs.txCompensation[""] = []func() error{func() error { return nil }}
			},
			expectErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			registry := plugins.NewInTreeRegistry()
			profile := schedulerapi.KubeSchedulerProfile{}
			nodes := []*v1.Node{{ObjectMeta: metav1.ObjectMeta{Name: "node1"}}}
			snapshot := cache.NewSnapshot(nil, nodes)
			informerFactory := informers.NewSharedInformerFactory(fake.NewClientset(), 0)
			f, err := frameworkruntime.NewFramework(ctx, registry, &profile,
				frameworkruntime.WithSnapshotSharedLister(snapshot),
				frameworkruntime.WithInformerFactory(informerFactory),
			)
			if err != nil {
				t.Fatalf("failed to create framework: %v", err)
			}

			cs := &ClusterSnapshot{
				frameworks: map[string]framework.Framework{
					v1.DefaultSchedulerName: f,
				},
				schedulerSnapshot: snapshot,
			}
			pods := []*v1.Pod{{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pod1",
					Namespace: "default",
					UID:       "uid1",
				},
				Spec: v1.PodSpec{
					NodeName: "node1",
				},
			}} // dummy pod

			ps := newPreemptionSnapshot(cs, pods, []string{"node1"})

			tc.setupSnapshot(cs)

			err = ps.Unpreempt(ctx, klog.FromContext(ctx))
			if (err != nil) != tc.expectErr {
				t.Errorf("Unpreempt() error = %v, expectErr %v", err, tc.expectErr)
			}
		})
	}
}

func TestUnpreemptRestoresPods(t *testing.T) {
	ctx := t.Context()
	registry := plugins.NewInTreeRegistry()
	prof := schedulerapi.KubeSchedulerProfile{}
	nodes := []*v1.Node{{ObjectMeta: metav1.ObjectMeta{Name: "node1"}}}
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pod1", Namespace: "default", UID: "uid1"},
		Spec:       v1.PodSpec{NodeName: "node1"},
	}
	snap := cache.NewSnapshot([]*v1.Pod{pod}, nodes)
	informerFactory := informers.NewSharedInformerFactory(fake.NewClientset(), 0)
	f, err := frameworkruntime.NewFramework(ctx, registry, &prof,
		frameworkruntime.WithSnapshotSharedLister(snap),
		frameworkruntime.WithInformerFactory(informerFactory),
	)
	if err != nil {
		t.Fatalf("failed to create framework: %v", err)
	}

	cs := &ClusterSnapshot{
		frameworks:        map[string]framework.Framework{v1.DefaultSchedulerName: f},
		schedulerSnapshot: snap,
		txCompensation:    make(map[string][]func() error),
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
	ps, err := cs.PreemptPods(ctx, klog.FromContext(ctx), []*v1.Pod{pod})
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
	if err := ps.Unpreempt(ctx, klog.FromContext(ctx)); err != nil {
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
				cs.txCompensation[activeTx] = append(cs.txCompensation[activeTx], func() error {
					*revertCalled = true
					return nil
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
				cs.txCompensation[activeTx] = append(cs.txCompensation[activeTx], func() error {
					*revertCalled = true
					return nil
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
				cs.txCompensation[activeTx] = append(cs.txCompensation[activeTx], func() error {
					*revertCalled = true
					return nil
				})
				return Commit, fmt.Errorf("some error")
			},
			expectRevert:     true,
			expectStackEmpty: true,
			expectErr:        true,
		},
		{
			name: "Revert fails",
			transactionFn: func(cs *ClusterSnapshot, revertCalled *bool, state map[string]any) (TransactionResult, error) {
				activeTx := cs.transactions[len(cs.transactions)-1]
				cs.txCompensation[activeTx] = append(cs.txCompensation[activeTx], func() error {
					*revertCalled = true
					return fmt.Errorf("revert failed")
				})
				return Revert, nil
			},
			expectRevert:     true,
			expectStackEmpty: true,
			expectErr:        true,
		},
		{
			name: "Revert fails immediately",
			transactionFn: func(cs *ClusterSnapshot, revertCalled *bool, state map[string]any) (TransactionResult, error) {
				activeTx := cs.transactions[len(cs.transactions)-1]

				// This operation should NOT be called because the one added after it will fail first.
				cs.txCompensation[activeTx] = append(cs.txCompensation[activeTx], func() error {
					state["op1Called"] = true
					return nil
				})

				// This operation fails and should be executed first (last added).
				cs.txCompensation[activeTx] = append(cs.txCompensation[activeTx], func() error {
					*revertCalled = true
					return fmt.Errorf("revert failed")
				})

				return Revert, nil
			},
			expectRevert:     true,
			expectStackEmpty: true,
			expectErr:        true,
			checkResult: func(t *testing.T, revertCalled bool, state map[string]any) {
				if state["op1Called"] == true {
					t.Errorf("Expected op1 to NOT be called, but it was")
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cs := &ClusterSnapshot{
				txCompensation: make(map[string][]func() error),
			}
			revertCalled := false
			state := make(map[string]any)
			ctx := t.Context()

			err := cs.Transaction(ctx, klog.FromContext(ctx), func() (TransactionResult, error) {
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
			name:          "Error - unknown scheduler name",
			candidateNodes: []string{"node1"},
			schedulerName: "unknown-scheduler",
			expectErr:     true,
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
			profile := schedulerapi.KubeSchedulerProfile{}

			snapshotNodes := make([]*v1.Node, len(tc.candidateNodes))
			for i, name := range tc.candidateNodes {
				snapshotNodes[i] = &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}
			}
			snapshot := cache.NewSnapshot(nil, snapshotNodes)
			informerFactory := informers.NewSharedInformerFactory(fake.NewClientset(), 0)

			f, err := frameworkruntime.NewFramework(ctx, registry, &profile,
				frameworkruntime.WithSnapshotSharedLister(snapshot),
				frameworkruntime.WithInformerFactory(informerFactory),
			)
			if err != nil {
				t.Fatalf("failed to create framework: %v", err)
			}

			cs := &ClusterSnapshot{
				frameworks: map[string]framework.Framework{
					v1.DefaultSchedulerName: f,
				},
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
			registry := plugins.NewInTreeRegistry()
			profile := schedulerapi.KubeSchedulerProfile{
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
			informerFactory := informers.NewSharedInformerFactory(fake.NewClientset(), 0)

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
						}
						if _, ok := unschedSet[nodeName]; ok {
							node.Spec.Unschedulable = true
						}
						nodeMap[nodeName] = node
						nodes = append(nodes, node)
					}
				}
			}

			snapshot := cache.NewSnapshot(nil, nodes)

			f, err := frameworkruntime.NewFramework(ctx, registry, &profile,
				frameworkruntime.WithSnapshotSharedLister(snapshot),
				frameworkruntime.WithInformerFactory(informerFactory),
			)
			if err != nil {
				t.Fatalf("failed to create framework: %v", err)
			}

			cs := &ClusterSnapshot{
				frameworks: map[string]framework.Framework{
					v1.DefaultSchedulerName: f,
				},
				schedulerSnapshot: snapshot,
				txCompensation:    make(map[string][]func() error),
			}

			if tc.inTransaction {
				cs.transactions = append(cs.transactions, "tx1")
				cs.txCompensation["tx1"] = []func() error{}
			}

			results, err := cs.SchedulePods(ctx, klog.FromContext(ctx), tc.pods, tc.opts)
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
			registry := plugins.NewInTreeRegistry()
			profile := schedulerapi.KubeSchedulerProfile{}
			informerFactory := informers.NewSharedInformerFactory(fake.NewClientset(), 0)

			var nodes []*v1.Node
			for _, nodeName := range tc.candidateNodes {
				nodes = append(nodes, &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: nodeName}})
			}

			snapshot := cache.NewSnapshot(nil, nodes)

			f, err := frameworkruntime.NewFramework(ctx, registry, &profile,
				frameworkruntime.WithSnapshotSharedLister(snapshot),
				frameworkruntime.WithInformerFactory(informerFactory),
			)
			if err != nil {
				t.Fatalf("failed to create framework: %v", err)
			}

			cs := &ClusterSnapshot{
				frameworks: map[string]framework.Framework{
					v1.DefaultSchedulerName: f,
				},
				schedulerSnapshot: snapshot,
				txCompensation:    make(map[string][]func() error),
			}

			if tc.inTransaction {
				cs.transactions = append(cs.transactions, "tx1")
				cs.txCompensation["tx1"] = []func() error{}
			}

			results, err := cs.SchedulePodsByTemplate(ctx, klog.FromContext(ctx), tc.template, tc.candidateNodes, tc.maxPods, tc.opts)
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

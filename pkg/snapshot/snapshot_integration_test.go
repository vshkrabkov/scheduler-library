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

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/klog/v2"
	schedulerapi "k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/backend/cache"
	framework "k8s.io/kubernetes/pkg/scheduler/framework"
	plugins "k8s.io/kubernetes/pkg/scheduler/framework/plugins"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
)

func TestWorkloadSchedulingWithPreemptionAndFillBack(t *testing.T) {
	ctx := t.Context()
	logger := klog.FromContext(ctx)

	// 1. Test Setup and Initialization
	nodes := generateMockNodes(100)
	pods := generateMockPods(nodes)
	cs, snap := setupSnapshotTest(t, ctx, nodes, pods)

	// We use pods[1] (which is on node1) as the pod to preempt,
	// to match the original test logic.
	pod1 := pods[1]

	// 2. Initial Snapshot and Virtual Pod Scheduling
	vpod1 := SchedulablePod{
		Pod: &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "vpod1", Namespace: "default", UID: "vuid1"},
			Spec: v1.PodSpec{
				NodeName: "node1",
				Containers: []v1.Container{{
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("3"),
						},
					},
				}},
			},
		},
		CandidateNodeNames: []string{"node1"},
	}

	_, err := cs.SchedulePods(ctx, logger, []SchedulablePod{vpod1}, SchedulePodsOptions{})
	if err != nil {
		t.Fatalf("failed to schedule virtual pod: %v", err)
	}

	// 3. Workload Processing Loop
	workload1 := Workload{
		PodTemplate: &v1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "w1",
				Labels: map[string]string{"app": "w1"},
			},
			Spec: v1.PodSpec{
				Containers: []v1.Container{{
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("6"),
						},
					},
				}},
			},
		},
		TargetCount: 1,
	}

	preemptionSet1 := []PreemptionBatch{
		{pod1},
	}
	preemptionSet2 := []PreemptionBatch{
		{pod1},
		{vpod1.Pod},
	}

	workloadTestData := []struct {
		workload       Workload
		preemptionSets []PreemptionSet
	}{
		{
			workload:       workload1,
			preemptionSets: []PreemptionSet{preemptionSet1, preemptionSet2},
		},
	}

	for _, wtd := range workloadTestData {
		var bestSet PreemptionSet
		var bestPreemptions = 999 // Infinity

		for _, pSet := range wtd.preemptionSets {
			logger.Info("Step 4: Trying combination", "pSet", pSet)
			var currentPreemptions int
			// Use a fresh snapshot for each simulation attempt to avoid transaction issues
			// Append vpod1.Pod to pods for simulation so it is in NodeInfo.
			simPods := append([]*v1.Pod{vpod1.Pod}, pods...)
			csSim, _ := setupSnapshotTest(t, ctx, nodes, simPods)

			for _, batch := range pSet {
				logger.Info("Step 4: Preempting batch", "batch", batch)
				_, err := csSim.PreemptPods(ctx, logger, batch)
				if err != nil {
					t.Fatalf("PreemptPods failed in simulation: %v", err)
				}
				currentPreemptions += len(batch)
			}

			logger.Info("Step 4: Before simulateWorkload")
			fits, err := simulateWorkload(ctx, logger, csSim, wtd.workload)
			if err != nil {
				t.Fatalf("simulateWorkload failed: %v", err)
			}
			logger.Info("Step 4: After simulateWorkload", "fits", fits)

			var reducedSet PreemptionSet
			if fits {
				// Fill-back logic (reverse order)
				reducedSet = pSet // Start with full set
				for i := len(pSet) - 1; i >= 0; i-- {
					batch := pSet[i]
					// Try simulation WITHOUT this batch
					csSimFB, _ := setupSnapshotTest(t, ctx, nodes, pods)
					_, err := csSimFB.SchedulePods(ctx, logger, []SchedulablePod{vpod1}, SchedulePodsOptions{})
					if err != nil {
						t.Fatalf("failed to schedule virtual pod in fill-back: %v", err)
					}

					// Preempt all OTHER batches in reducedSet
					for j, b := range reducedSet {
						if j == i {
							continue
						}
						_, err := csSimFB.PreemptPods(ctx, logger, b)
						if err != nil {
							t.Fatalf("PreemptPods failed in fill-back: %v", err)
						}
					}

					fitsFB, err := simulateWorkload(ctx, logger, csSimFB, wtd.workload)
					if err != nil {
						t.Fatalf("simulateWorkload failed in fill-back: %v", err)
					}

					if fitsFB {
						// We don't need this batch! Remove it from reducedSet.
						reducedSet = append(reducedSet[:i], reducedSet[i+1:]...)
						currentPreemptions -= len(batch)
					}
				}
			} else {
				reducedSet = pSet
			}

			if fits && currentPreemptions < bestPreemptions {
				bestPreemptions = currentPreemptions
				bestSet = reducedSet
			}
		}

		// Step 5: Apply Best Option
		if bestSet != nil {
			logger.Info("Step 5: Applying best set", "batches", len(bestSet))
			for _, batch := range bestSet {
				_, err := cs.PreemptPods(ctx, logger, batch)
				if err != nil {
					t.Fatalf("failed to apply preemption: %v", err)
				}

			}
			results, err := cs.SchedulePodsByTemplate(ctx, logger, wtd.workload.PodTemplate, []string{"node1"}, wtd.workload.TargetCount, SchedulePodsByTemplateOptions{})
			logger.Info("Step 5: SchedulePodsByTemplate results", "results", len(results), "err", err)
			if err != nil {
				t.Fatalf("failed to schedule workload: %v", err)
			}
			if len(results) != wtd.workload.TargetCount {
				t.Fatalf("expected to schedule %d pods, but scheduled %d", wtd.workload.TargetCount, len(results))
			}
		} else {
			logger.Info("Step 5: bestSet is nil")
		}
	}

	// Step 6: Final Verification
	nodeInfo, err := snap.NodeInfos().Get("node1")
	if err != nil {
		t.Fatalf("failed to get node info: %v", err)
	}

	if len(nodeInfo.GetPods()) != 2 {
		t.Errorf("Expected 2 pods on node1, got %d", len(nodeInfo.GetPods()))
	}

	foundWorkloadPod := false
	logger.Info("Pods on node1 at verification", "count", len(nodeInfo.GetPods()))
	for _, p := range nodeInfo.GetPods() {
		logger.Info("Pod on node1 at verification", "name", p.GetPod().Name)
		if p.GetPod().Name == "pod1" {
			t.Errorf("Expected pod1 to be preempted from node1, but it is still there")
		}
		if p.GetPod().Labels["app"] == "w1" {
			foundWorkloadPod = true
		}
	}
	if !foundWorkloadPod {
		t.Errorf("Expected workload pod to be scheduled on node1, but it was not found")
	}
}

func setupSnapshotTest(t *testing.T, ctx context.Context, nodes []*v1.Node, pods []*v1.Pod) (*ClusterSnapshot, *cache.Snapshot) {
	registry := plugins.NewInTreeRegistry()
	profile := schedulerapi.KubeSchedulerProfile{
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

	logger := klog.FromContext(ctx)
	client := fake.NewClientset()

	for _, n := range nodes {
		if _, err := client.CoreV1().Nodes().Create(ctx, n, metav1.CreateOptions{}); err != nil {
			t.Fatalf("failed to create node in fake client: %v", err)
		}
	}
	for _, p := range pods {
		if _, err := client.CoreV1().Pods(p.Namespace).Create(ctx, p, metav1.CreateOptions{}); err != nil {
			t.Fatalf("failed to create pod in fake client: %v", err)
		}
	}

	informerFactory := informers.NewSharedInformerFactory(client, 0)

	c := cache.New(ctx, nil, false)
	for _, n := range nodes {
		c.AddNode(logger, n)
	}
	for _, p := range pods {
		if err := c.AddPod(logger, p); err != nil {
			t.Fatalf("failed to add pod to cache: %v", err)
		}
	}

	snap := cache.NewEmptySnapshot()
	if err := c.UpdateSnapshot(logger, snap); err != nil {
		t.Fatalf("failed to update snapshot from cache: %v", err)
	}

	f, err := frameworkruntime.NewFramework(ctx, registry, &profile,
		frameworkruntime.WithSnapshotSharedLister(snap),
		frameworkruntime.WithInformerFactory(informerFactory),
	)
	if err != nil {
		t.Fatalf("failed to create framework: %v", err)
	}
	informerFactory.Start(ctx.Done())

	cs := &ClusterSnapshot{
		frameworks:        map[string]framework.Framework{v1.DefaultSchedulerName: f},
		schedulerSnapshot: snap,
		txCompensation:    make(map[string][]func() error),
	}

	return cs, snap
}

type Workload struct {
	PodTemplate *v1.PodTemplateSpec
	TargetCount int
}

type PreemptionBatch []*v1.Pod
type PreemptionSet []PreemptionBatch

func simulateWorkload(ctx context.Context, logger klog.Logger, cs *ClusterSnapshot, workload Workload) (bool, error) {
	logger.Info("simulateWorkload started", "pod", workload.PodTemplate.Name)
	nodeInfo, err := cs.schedulerSnapshot.NodeInfos().Get("node1")
	if err != nil {
		logger.Error(err, "failed to get node1 info in simulateWorkload")
	} else {
		logger.Info("node1 state in simulateWorkload", "pods", len(nodeInfo.GetPods()))
	}

	fwk, _ := cs.getFramework("")
	if fwk != nil {
		state := framework.NewCycleState()
		pod := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "simulated-workload", Namespace: "default"},
			Spec:       workload.PodTemplate.Spec,
		}
		preFilterResult, status, _ := fwk.RunPreFilterPlugins(ctx, state, pod)
		logger.Info("simulateWorkload debug", "PreFilterStatus", status, "PreFilterResult", preFilterResult)

		nodeInfo, _ := fwk.SnapshotSharedLister().NodeInfos().Get("node1")
		if nodeInfo != nil {
			filterStatus := fwk.RunFilterPlugins(ctx, state, pod, nodeInfo)
			logger.Info("simulateWorkload debug", "FilterStatus", filterStatus)
		}
	}

	feasibleNodes, err := cs.CanSchedulePod(ctx, logger, SchedulablePod{
		Pod: &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "simulated-workload", Namespace: "default"},
			Spec:       workload.PodTemplate.Spec,
		},
		CandidateNodeNames: []string{"node1"},
	})
	if err != nil {
		logger.Error(err, "CanSchedulePod failed")
		return false, err
	}

	logger.Info("simulateWorkload", "feasibleNodes", feasibleNodes)

	if len(feasibleNodes) > 0 {
		results, err := cs.SchedulePodsByTemplate(ctx, logger, workload.PodTemplate, feasibleNodes, workload.TargetCount, NewSchedulePodsByTemplateOptions(true))
		if err != nil {
			return false, err
		}
		logger.Info("simulateWorkload", "results", len(results))
		if len(results) == workload.TargetCount {
			return true, nil
		}
	}
	return false, nil
}



func generateMockNodes(count int) []*v1.Node {
	nodes := make([]*v1.Node, count)
	for i := 0; i < count; i++ {
		nodes[i] = &v1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("node%d", i)},
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
	}
	return nodes
}

func generateMockPods(nodes []*v1.Node) []*v1.Pod {
	pods := make([]*v1.Pod, len(nodes))
	for i, node := range nodes {
		pods[i] = &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("pod%d", i), Namespace: "default", UID: types.UID(fmt.Sprintf("uid%d", i))},
			Spec: v1.PodSpec{
				NodeName: node.Name,
				Containers: []v1.Container{{
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("5"),
						},
					},
				}},
			},
		}
	}
	return pods
}

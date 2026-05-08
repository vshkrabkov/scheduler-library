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

	"github.com/google/uuid"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"k8s.io/kube-scheduler/framework"
	fwk "k8s.io/kubernetes/pkg/scheduler/framework"

	"k8s.io/kubernetes/pkg/scheduler"
	"k8s.io/kubernetes/pkg/scheduler/backend/cache"
	"k8s.io/kubernetes/pkg/scheduler/profile"
)

// ClusterSnapshot wraps a scheduler snapshot and its associated frameworks.
// All ClusterSnapshot instances created from the same ClusterState share the
// same underlying cache.Snapshot. Creating a new snapshot via ClusterState.Snapshot
// updates that shared snapshot in-place, which invalidates any previously returned
// ClusterSnapshot instance — callers must not use a prior snapshot after requesting a new one.
type ClusterSnapshot struct {
	schedulerSnapshot *cache.Snapshot
	frameworks        profile.Map
	transactions      []string
	lastCommittedTx   string
	txCompensation    map[string][]func()
}

// NewClusterSnapshot creates a new ClusterSnapshot stub wrapping the provided scheduler snapshot and frameworks.
func NewClusterSnapshot(s *cache.Snapshot, frameworks profile.Map) (*ClusterSnapshot, error) {
	return &ClusterSnapshot{
		schedulerSnapshot: s,
		frameworks:        frameworks,
		txCompensation:    make(map[string][]func()),
	}, nil
}

// Transaction executes the provided function within a transaction.
// It rolls back operations if the function returns Revert or an error.
func (s *ClusterSnapshot) Transaction(ctx context.Context, logger klog.Logger, transactionFn func() (TransactionResult, error)) error {
	txId := uuid.New().String()
	s.transactions = append(s.transactions, txId)
	s.txCompensation[txId] = []func(){}

	defer func() {
		delete(s.txCompensation, txId)
		s.transactions = s.transactions[:len(s.transactions)-1]
	}()

	result, err := transactionFn()

	if err != nil || result == Revert {
		operations := s.txCompensation[txId]
		for i := len(operations) - 1; i >= 0; i-- {
			operations[i]()
		}
	} else {
		s.lastCommittedTx = txId
	}

	if err != nil {
		return fmt.Errorf("transaction failed: %w", err)
	}
	return nil
}

// CanSchedulePod checks feasibility of a single pod on the specified nodes by running
// PreFilter and Filter plugins. Returns the names of nodes on which the pod can be scheduled.
func (s *ClusterSnapshot) CanSchedulePod(ctx context.Context, logger klog.Logger, pod SchedulablePod) ([]string, error) {
	f, err := s.getFramework(pod.Pod.Spec.SchedulerName)
	if err != nil {
		return nil, fmt.Errorf("failed to get framework: %w", err)
	}
	state := fwk.NewCycleState()

	preFilterResult, status, _ := f.RunPreFilterPlugins(ctx, state, pod.Pod)
	if !status.IsSuccess() {
		if status.IsRejected() {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to run prefilter plugins: %w", status.AsError())
	}

	var feasibleNodes []string
	for _, nodeName := range pod.CandidateNodeNames {
		if preFilterResult != nil && !preFilterResult.AllNodes() && !preFilterResult.NodeNames.Has(nodeName) {
			continue
		}
		nodeInfo, err := f.SnapshotSharedLister().NodeInfos().Get(nodeName)
		if err != nil {
			return nil, fmt.Errorf("failed to get node %s: %w", nodeName, err)
		}
		if f.RunFilterPlugins(ctx, state, pod.Pod, nodeInfo).IsSuccess() {
			feasibleNodes = append(feasibleNodes, nodeName)
		}
	}

	return feasibleNodes, nil
}

func (s *ClusterSnapshot) withPlacement(candidates []string, fn func() (*scheduler.SchedulingTryResult, func())) (*scheduler.SchedulingTryResult, func(), error) {
	nodes := make([]framework.NodeInfo, 0, len(candidates))
	for _, name := range candidates {
		ni, err := s.schedulerSnapshot.NodeInfos().Get(name)
		if err != nil {
			return nil, nil, fmt.Errorf("node %s not in snapshot: %w", name, err)
		}
		nodes = append(nodes, ni)
	}
	if err := s.schedulerSnapshot.AssumePlacement(&framework.Placement{Nodes: nodes}); err != nil {
		return nil, nil, err
	}
	defer s.schedulerSnapshot.ForgetPlacement()
	res, revertFn := fn()
	return res, revertFn, nil
}

// SchedulePods schedules the given pods onto their candidate nodes using PreFilter and Filter plugins.
// StopOnFailure controls whether the first unschedulable pod stops the loop. Note that
// node-not-found errors always propagate immediately regardless of StopOnFailure, as they
// indicate a programming error rather than a scheduling failure.
func (s *ClusterSnapshot) SchedulePods(ctx context.Context, sched *scheduler.Scheduler, logger klog.Logger, pods []SchedulablePod, opts SchedulePodsOptions) ([]SchedulingResult, error) {
	var currTx []func()
	if opts.DryRun {
		currTx = []func(){}
		defer func() {
			for i := len(currTx) - 1; i >= 0; i-- {
				currTx[i]()
			}
		}()
	}

	result := make([]SchedulingResult, 0)
	if len(pods) == 0 {
		return result, nil
	}

	for _, p := range pods {
		res, revertFn, err := s.scheduleOnePod(ctx, sched, p.Pod, p.CandidateNodeNames)
		if err != nil {
			return result, err
		}

		if !res.Status.IsSuccess() {
			if opts.StopOnFailure {
				return result, fmt.Errorf("simulation failed: %w", res.Status.AsError())
			}
			result = append(result, *res)
			continue
		}

		result = append(result, *res)

		if opts.DryRun {
			currTx = append(currTx, revertFn)
		} else if len(s.transactions) > 0 {
			txId := s.transactions[len(s.transactions)-1]
			s.txCompensation[txId] = append(s.txCompensation[txId], revertFn)
		}
	}

	return result, nil
}

// SchedulePodsByTemplate attempts to schedule as many pods matching the template as possible.
// It assumes candidate nodes are feasible and moves to the next node only if the pod is unschedulable on the current node.
func (s *ClusterSnapshot) SchedulePodsByTemplate(ctx context.Context, sched *scheduler.Scheduler, logger klog.Logger, template *v1.PodTemplateSpec, candidateNodes []string, maxPods int, opts SchedulePodsByTemplateOptions) ([]SchedulingResult, error) {
	var currTx []func()
	if opts.DryRun {
		currTx = []func(){}
		defer func() {
			for i := len(currTx) - 1; i >= 0; i-- {
				currTx[i]()
			}
		}()
	}

	result := make([]SchedulingResult, 0)
	if maxPods <= 0 || len(candidateNodes) == 0 {
		return result, nil
	}

	nodeIdx := 0
	for i := 0; i < maxPods; i++ {
		pod := s.createPodFromTemplate(template, i)
		scheduled := false

		for nodeIdx < len(candidateNodes) {
			res, revertFn, err := s.scheduleOnePod(ctx, sched, pod, []string{candidateNodes[nodeIdx]})
			if err != nil {
				return result, err
			}

			if res.Status.IsSuccess() {
				result = append(result, *res)

				if opts.DryRun {
					currTx = append(currTx, revertFn)
				} else if len(s.transactions) > 0 {
					txId := s.transactions[len(s.transactions)-1]
					s.txCompensation[txId] = append(s.txCompensation[txId], revertFn)
				}

				scheduled = true
				break // Successfully scheduled, move to next pod using the same nodeIdx
			}

			// Unschedulable on this node, try next node
			nodeIdx++
		}

		if !scheduled {
			// No more nodes can fit this pod template, stop scheduling
			break
		}
	}

	return result, nil
}

// PreemptPods removes pods from the snapshot.
// It supports transaction rollbacks if called inside a transaction.
// If any pod fails to be preempted, all previously preempted pods in this call
// are automatically restored and an error is returned.
func (s *ClusterSnapshot) PreemptPods(ctx context.Context, sched *scheduler.Scheduler, pods []*v1.Pod) (*PreemptionSnapshot, error) {
	// Validate all pods before making any changes.
	for _, pod := range pods {
		if pod.Spec.NodeName == "" {
			return nil, fmt.Errorf("pod %s has no node name", klog.KObj(pod))
		}
	}
	var revertFns []func()

	var txId string
	insideTx := len(s.transactions) > 0
	if insideTx {
		txId = s.transactions[len(s.transactions)-1]
	}

	for _, pod := range pods {
		nodeName := pod.Spec.NodeName
		cycleState := fwk.NewCycleState()
		cycleState.SetPodGroupSchedulingCycle(fwk.NewCycleState())

		revertFn, err := s.unreserveAndForget(ctx, sched, pod, nodeName, cycleState)
		if err != nil {
			// Roll back all already-preempted pods.
			for i := len(revertFns) - 1; i >= 0; i-- {
				revertFns[i]()
			}
			// Remove the compensation callbacks added for the already-preempted pods,
			// since we just manually restored them. Without this, a later transaction
			// revert would try to re-add them again and double-add the pods.
			if insideTx {
				n := len(revertFns)
				comps := s.txCompensation[txId]
				s.txCompensation[txId] = comps[:len(comps)-n]
			}
			return nil, fmt.Errorf("failed to unreserve and forget pod %s: %w", klog.KObj(pod), err)
		}

		revertFns = append(revertFns, revertFn)

		if insideTx {
			s.txCompensation[txId] = append(s.txCompensation[txId], revertFn)
		}
	}

	return newPreemptionSnapshot(s, revertFns), nil
}

type PreemptionSnapshot struct {
	snapshot           *ClusterSnapshot
	revertFns          []func()
	currentTx          string
	currentTxMutations int
	lastCommittedTx    string
}

func newPreemptionSnapshot(s *ClusterSnapshot, revertFns []func()) *PreemptionSnapshot {
	var currentTx string
	if len(s.transactions) > 0 {
		currentTx = s.transactions[len(s.transactions)-1]
	}
	var currentTxMutations int
	if currentTx != "" {
		currentTxMutations = len(s.txCompensation[currentTx])
	}

	return &PreemptionSnapshot{
		snapshot:           s,
		revertFns:          revertFns,
		currentTx:          currentTx,
		currentTxMutations: currentTxMutations,
		lastCommittedTx:    s.lastCommittedTx,
	}
}

// Unpreempt undos the preemption done by the PreemptPods.
func (ps *PreemptionSnapshot) Unpreempt() error {
	newTxWasCommitedAfterPreemption := ps.lastCommittedTx != ps.snapshot.lastCommittedTx

	var currentSnapshotTx string
	if len(ps.snapshot.transactions) > 0 {
		currentSnapshotTx = ps.snapshot.transactions[len(ps.snapshot.transactions)-1]
	}
	newTxStarted := ps.currentTx != currentSnapshotTx

	newMutationForCurrentTx := !newTxStarted && ps.currentTxMutations != len(ps.snapshot.txCompensation[ps.currentTx])

	if newTxWasCommitedAfterPreemption || newTxStarted || newMutationForCurrentTx {
		return fmt.Errorf("snapshot was mutated after preemption")
	}

	for i := len(ps.revertFns) - 1; i >= 0; i-- {
		ps.revertFns[i]()
	}

	// Non transaction scope, nothing to revert
	if ps.currentTx == "" {
		return nil
	}

	txId := ps.currentTx
	// Number of pods being manually re-added right now
	numPods := len(ps.revertFns)

	compensationFuncs := ps.snapshot.txCompensation[txId]
	if len(compensationFuncs) < numPods {
		return fmt.Errorf("unexpected number of mutations in transaction compensation list")
	}

	ps.snapshot.txCompensation[txId] = compensationFuncs[:len(compensationFuncs)-numPods]

	return nil
}

func (s *ClusterSnapshot) getFramework(schedulerName string) (fwk.Framework, error) {
	if schedulerName == "" {
		schedulerName = v1.DefaultSchedulerName
	}

	framework, ok := s.frameworks[schedulerName]
	if !ok {
		return nil, fmt.Errorf("no framework found for scheduler: %q", schedulerName)
	}

	return framework, nil
}

func (s *ClusterSnapshot) assumeAndReserve(ctx context.Context, sched *scheduler.Scheduler, pod *v1.Pod, nodeName string, cycleState *fwk.CycleState) (func(), error) {
	nodeInfo, _ := s.schedulerSnapshot.Get(nodeName)
	podInfo, _ := fwk.NewPodInfo(pod.DeepCopy())
	nodeInfo.AddPodInfo(podInfo)

	revertFn := func() {
		s.unreserveAndForget(ctx, sched, pod, nodeName, cycleState)
	}

	return revertFn, nil
}

func (s *ClusterSnapshot) unreserveAndForget(ctx context.Context, sched *scheduler.Scheduler, pod *v1.Pod, nodeName string, cycleState *fwk.CycleState) (func(), error) {
	logger := klog.FromContext(ctx)
	nodeInfo, _ := s.schedulerSnapshot.Get(nodeName)

	nodeInfo.RemovePod(logger, pod)

	revertFn := func() {
		s.assumeAndReserve(ctx, sched, pod, nodeName, cycleState)
	}

	return revertFn, nil
}

func (s *ClusterSnapshot) scheduleOnePod(ctx context.Context, sched *scheduler.Scheduler, pod *v1.Pod, candidateNodes []string) (*SchedulingResult, func(), error) {
	framework, err := s.getFramework(pod.Spec.SchedulerName)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get framework for pod %s: %w", klog.KObj(pod), err)
	}

	cycleState := fwk.NewCycleState()
	cycleState.SetPodGroupSchedulingCycle(fwk.NewCycleState())
	podInfo, err := fwk.NewPodInfo(pod)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create pod info for pod %s: %w", klog.KObj(pod), err)
	}

	simRes, revertFn, err := s.withPlacement(candidateNodes, func() (*scheduler.SchedulingTryResult, func()) {
		return sched.TryScheduling(ctx, cycleState, framework, &fwk.QueuedPodInfo{PodInfo: podInfo})
	})

	if err != nil {
		return nil, nil, fmt.Errorf("failed to simulate scheduling for pod %s: %w", klog.KObj(pod), err)
	}

	if !simRes.Status.IsSuccess() {
		return &SchedulingResult{Pod: pod, Status: simRes.Status}, nil, nil
	}

	if simRes.AssumedPodInfo == nil {
		return nil, nil, fmt.Errorf("pod %s was not assumed", klog.KObj(pod))
	}

	nodeName := simRes.ScheduleResult.SuggestedHost
	pod.Spec.NodeName = nodeName
	return &SchedulingResult{
		Pod:              pod,
		Status:           simRes.Status,
		SelectedNodeName: nodeName,
	}, revertFn, nil
}

func (s *ClusterSnapshot) createPodFromTemplate(template *v1.PodTemplateSpec, index int) *v1.Pod {
	podNamePrefix := template.Name
	if podNamePrefix == "" {
		podNamePrefix = "templated-pod"
	}
	ns := template.Namespace
	if ns == "" {
		ns = "default"
	}

	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%d", podNamePrefix, index),
			Namespace: ns,
			UID:       types.UID(uuid.New().String()),
			Labels:    template.Labels,
		},
		Spec: template.Spec,
	}
}

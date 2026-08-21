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

package upstreamsync

/*

Tests for the logic extracted into scheduler.go.

Copied from kubernetes/kubernetes/pkg/scheduler/schedule_one_test.go and adjusted to the
extracted API:
  - the Scheduler under test is built with NewScheduler() instead of being assembled field
    by field, so numNodesToFind is passed in explicitly instead of being derived from
    percentageOfNodesToScore,
  - extenders are taken from the Framework rather than from the Scheduler, so they are
    registered with frameworkruntime.WithExtenders(),
  - schedulePod()/findNodesThatFitPod() take a *PendingPod, which carries the cycle state,
    instead of a *framework.QueuedPodInfo plus a separate fwk.CycleState,
  - findNodesThatFitPod() takes the extra findAll argument.

Tests of the parts of schedule_one.go that were not extracted (the binding cycle, the
scheduling queue, error handling and pod status updates) are intentionally not copied.

*/

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/informers"
	clientsetfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/component-helpers/storage/volume"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/ktesting"
	fwk "k8s.io/kube-scheduler/framework"
	internalcache "k8s.io/kubernetes/pkg/scheduler/backend/cache"
	internalqueue "k8s.io/kubernetes/pkg/scheduler/backend/queue"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/defaultbinder"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/imagelocality"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/noderesources"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/podtopologyspread"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/queuesort"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/volumebinding"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	tf "k8s.io/kubernetes/pkg/scheduler/testing/framework"
	schedutil "k8s.io/kubernetes/pkg/scheduler/util"
	"k8s.io/utils/ptr"
)

const mb int64 = 1024 * 1024

// TestMain registers the scheduler metrics. Upstream this happens as a side effect of
// scheduler.New(), which the extracted code doesn't call.
func TestMain(m *testing.M) {
	metrics.Register()
	os.Exit(m.Run())
}

var (
	podTopologySpreadFunc = frameworkruntime.FactoryAdapter(feature.Features{}, podtopologyspread.New)
	errPrioritize         = fmt.Errorf("priority map encounters an error")
	schedulerCmpOpts      = []cmp.Option{
		cmp.AllowUnexported(framework.NodeToStatus{}),
	}
)

// newTestSnapshot builds a snapshot holding the given nodes. It goes through the scheduler
// cache rather than calling internalcache.NewSnapshot() directly, because the latter builds
// its node list by ranging over a map, so the node order - and with it the order of the
// scores prioritizeNodes() returns - would be random.
func newTestSnapshot(ctx context.Context, t *testing.T, nodes []*v1.Node) *internalcache.Snapshot {
	t.Helper()
	logger := klog.FromContext(ctx)
	schedulerCache := internalcache.New(ctx, nil, false)
	for _, n := range nodes {
		schedulerCache.AddNode(logger, n)
	}
	snapshot := internalcache.NewEmptySnapshot()
	if err := schedulerCache.UpdateSnapshot(logger, snapshot); err != nil {
		t.Fatalf("Error building the snapshot: %v", err)
	}
	return snapshot
}

// newTestScheduler builds a Scheduler over a snapshot of the given nodes, configured to
// consider all of them feasible. Upstream this is makeScheduler(), which derives the number
// of nodes to look for from percentageOfNodesToScore; here numNodesToFind is an explicit
// constructor argument, and len(nodes) matches what numFeasibleNodesToFind() returns
// upstream for the small node counts these tests use.
func newTestScheduler(ctx context.Context, t *testing.T, nodes []*v1.Node) *Scheduler {
	t.Helper()
	return NewScheduler(newTestSnapshot(ctx, t, nodes), 0, 0, int32(len(nodes)))
}

// pendingPodForPod wraps a pod into the *PendingPod that the extracted scheduling methods
// take. Upstream this is queuedPodInfoForPod() plus a separately passed framework.NewCycleState().
func pendingPodForPod(t *testing.T, pod *v1.Pod) *PendingPod {
	t.Helper()
	podInfo, err := framework.NewPodInfo(pod)
	if err != nil {
		t.Fatalf("Error creating pod info for %s/%s: %v", pod.Namespace, pod.Name, err)
	}
	return &PendingPod{
		PodInfo:    podInfo,
		CycleState: framework.NewCycleState(),
	}
}

type falseMapPlugin struct{}

func newFalseMapPlugin() frameworkruntime.PluginFactory {
	return func(_ context.Context, _ runtime.Object, _ fwk.Handle) (fwk.Plugin, error) {
		return &falseMapPlugin{}, nil
	}
}

func (pl *falseMapPlugin) Name() string {
	return "FalseMap"
}

func (pl *falseMapPlugin) Score(_ context.Context, _ fwk.CycleState, _ *v1.Pod, _ fwk.NodeInfo) (int64, *fwk.Status) {
	return 0, fwk.AsStatus(errPrioritize)
}

func (pl *falseMapPlugin) ScoreExtensions() fwk.ScoreExtensions {
	return nil
}

type numericMapPlugin struct{}

func newNumericMapPlugin() frameworkruntime.PluginFactory {
	return func(_ context.Context, _ runtime.Object, _ fwk.Handle) (fwk.Plugin, error) {
		return &numericMapPlugin{}, nil
	}
}

func (pl *numericMapPlugin) Name() string {
	return "NumericMap"
}

func (pl *numericMapPlugin) Score(_ context.Context, _ fwk.CycleState, _ *v1.Pod, nodeInfo fwk.NodeInfo) (int64, *fwk.Status) {
	nodeName := nodeInfo.Node().Name
	score, err := strconv.Atoi(nodeName)
	if err != nil {
		return 0, fwk.NewStatus(fwk.Error, fmt.Sprintf("Error converting nodename to int: %+v", nodeName))
	}
	return int64(score), nil
}

func (pl *numericMapPlugin) ScoreExtensions() fwk.ScoreExtensions {
	return nil
}

// NewNoPodsFilterPlugin initializes a noPodsFilterPlugin and returns it.
func NewNoPodsFilterPlugin(_ context.Context, _ runtime.Object, _ fwk.Handle) (fwk.Plugin, error) {
	return &noPodsFilterPlugin{}, nil
}

type reverseNumericMapPlugin struct{}

func (pl *reverseNumericMapPlugin) Name() string {
	return "ReverseNumericMap"
}

func (pl *reverseNumericMapPlugin) Score(_ context.Context, _ fwk.CycleState, _ *v1.Pod, nodeInfo fwk.NodeInfo) (int64, *fwk.Status) {
	nodeName := nodeInfo.Node().Name
	score, err := strconv.Atoi(nodeName)
	if err != nil {
		return 0, fwk.NewStatus(fwk.Error, fmt.Sprintf("Error converting nodename to int: %+v", nodeName))
	}
	return int64(score), nil
}

func (pl *reverseNumericMapPlugin) ScoreExtensions() fwk.ScoreExtensions {
	return pl
}

func (pl *reverseNumericMapPlugin) NormalizeScore(_ context.Context, _ fwk.CycleState, _ *v1.Pod, nodeScores fwk.NodeScoreList) *fwk.Status {
	var maxScore float64
	minScore := math.MaxFloat64

	for _, hostPriority := range nodeScores {
		maxScore = math.Max(maxScore, float64(hostPriority.Score))
		minScore = math.Min(minScore, float64(hostPriority.Score))
	}
	for i, hostPriority := range nodeScores {
		nodeScores[i] = fwk.NodeScore{
			Name:  hostPriority.Name,
			Score: int64(maxScore + minScore - float64(hostPriority.Score)),
		}
	}
	return nil
}

func newReverseNumericMapPlugin() frameworkruntime.PluginFactory {
	return func(_ context.Context, _ runtime.Object, _ fwk.Handle) (fwk.Plugin, error) {
		return &reverseNumericMapPlugin{}, nil
	}
}

type trueMapPlugin struct{}

func (pl *trueMapPlugin) Name() string {
	return "TrueMap"
}

func (pl *trueMapPlugin) Score(_ context.Context, _ fwk.CycleState, _ *v1.Pod, _ fwk.NodeInfo) (int64, *fwk.Status) {
	return 1, nil
}

func (pl *trueMapPlugin) ScoreExtensions() fwk.ScoreExtensions {
	return pl
}

func (pl *trueMapPlugin) NormalizeScore(_ context.Context, _ fwk.CycleState, _ *v1.Pod, nodeScores fwk.NodeScoreList) *fwk.Status {
	for _, host := range nodeScores {
		if host.Name == "" {
			return fwk.NewStatus(fwk.Error, "unexpected empty host name")
		}
	}
	return nil
}

func newTrueMapPlugin() frameworkruntime.PluginFactory {
	return func(_ context.Context, _ runtime.Object, _ fwk.Handle) (fwk.Plugin, error) {
		return &trueMapPlugin{}, nil
	}
}

type noPodsFilterPlugin struct{}

// Name returns name of the plugin.
func (pl *noPodsFilterPlugin) Name() string {
	return "NoPodsFilter"
}

// Filter invoked at the filter extension point.
func (pl *noPodsFilterPlugin) Filter(_ context.Context, _ fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	if len(nodeInfo.GetPods()) == 0 {
		return nil
	}
	return fwk.NewStatus(fwk.Unschedulable, tf.ErrReasonFake)
}

func nodeToStatusDiff(want, got *framework.NodeToStatus) string {
	if want == nil || got == nil {
		return cmp.Diff(want, got)
	}
	return cmp.Diff(*want, *got, schedulerCmpOpts...)
}

func makeNodeList(nodeNames []string) []*v1.Node {
	result := make([]*v1.Node, 0, len(nodeNames))
	for _, nodeName := range nodeNames {
		result = append(result, &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: nodeName}})
	}
	return result
}

func makeNode(node string, milliCPU, memory int64, images ...v1.ContainerImage) *v1.Node {
	return &v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: node},
		Status: v1.NodeStatus{
			Capacity: v1.ResourceList{
				v1.ResourceCPU:    *resource.NewMilliQuantity(milliCPU, resource.DecimalSI),
				v1.ResourceMemory: *resource.NewQuantity(memory, resource.BinarySI),
				"pods":            *resource.NewQuantity(100, resource.DecimalSI),
			},
			Allocatable: v1.ResourceList{

				v1.ResourceCPU:    *resource.NewMilliQuantity(milliCPU, resource.DecimalSI),
				v1.ResourceMemory: *resource.NewQuantity(memory, resource.BinarySI),
				"pods":            *resource.NewQuantity(100, resource.DecimalSI),
			},
			Images: images,
		},
	}
}

var lowPriority, midPriority, highPriority = int32(0), int32(100), int32(1000)

// Test_SelectHost differs from upstream in that sortedNodeScores here only exposes the node
// name through Pop(), not the whole fwk.NodePluginScores, so the expected order is asserted
// on the node names.
func Test_SelectHost(t *testing.T) {
	tests := []struct {
		name             string
		list             []fwk.NodePluginScores
		expectedNodeList []fwk.NodePluginScores
	}{
		{
			name: "unique properly ordered scores",
			list: []fwk.NodePluginScores{
				{Name: "node1", TotalScore: 1},
				{Name: "node2", TotalScore: 2},
			},
			expectedNodeList: []fwk.NodePluginScores{
				{Name: "node2", TotalScore: 2},
				{Name: "node1", TotalScore: 1},
			},
		},
		{
			name: "equal scores",
			list: []fwk.NodePluginScores{
				{Name: "node2.2", TotalScore: 2, Randomizer: 2},
				{Name: "node2.1", TotalScore: 2, Randomizer: 1},
				{Name: "node2.3", TotalScore: 2, Randomizer: 3},
			},
			expectedNodeList: []fwk.NodePluginScores{
				{Name: "node2.3", TotalScore: 2, Randomizer: 3},
				{Name: "node2.2", TotalScore: 2, Randomizer: 2},
				{Name: "node2.1", TotalScore: 2, Randomizer: 1},
			},
		},
		{
			name: "out of order scores",
			list: []fwk.NodePluginScores{
				{Name: "node3.1", TotalScore: 3, Randomizer: 1},
				{Name: "node2.1", TotalScore: 2},
				{Name: "node1.1", TotalScore: 1},
				{Name: "node3.2", TotalScore: 3, Randomizer: 2},
			},
			expectedNodeList: []fwk.NodePluginScores{
				{Name: "node3.2", TotalScore: 3, Randomizer: 2},
				{Name: "node3.1", TotalScore: 3, Randomizer: 1},
				{Name: "node2.1", TotalScore: 2},
				{Name: "node1.1", TotalScore: 1},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			wantNodes := make([]string, 0, len(test.expectedNodeList))
			for _, scores := range test.expectedNodeList {
				wantNodes = append(wantNodes, scores.Name)
			}

			gotNodes := []string{}
			h := newSortedNodeScores(test.list)
			for i := range test.list {
				if want := len(test.list) - i; h.Len() != want {
					t.Errorf("Unexpected Len(): got %d, want %d", h.Len(), want)
				}
				gotNodes = append(gotNodes, h.Pop())
			}
			if h.Len() != 0 {
				t.Errorf("Unexpected Len() after popping every node: got %d, want 0", h.Len())
			}
			if diff := cmp.Diff(wantNodes, gotNodes); diff != "" {
				t.Errorf("Unexpected node order (-want, +got):\n%s", diff)
			}
		})
	}
}

func TestFindNodesThatPassExtenders(t *testing.T) {
	absentStatus := fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "node(s) didn't satisfy plugin(s) [PreFilter]")

	tests := []struct {
		name                  string
		extenders             []tf.FakeExtender
		nodes                 []*v1.Node
		filteredNodesStatuses *framework.NodeToStatus
		expectsErr            bool
		expectedNodes         []*v1.Node
		expectedStatuses      *framework.NodeToStatus
	}{
		{
			name: "error",
			extenders: []tf.FakeExtender{
				{
					ExtenderName: "FakeExtender1",
					Predicates:   []tf.FitPredicate{tf.ErrorPredicateExtender},
				},
			},
			nodes:                 makeNodeList([]string{"a"}),
			filteredNodesStatuses: framework.NewNodeToStatus(make(map[string]*fwk.Status), absentStatus),
			expectsErr:            true,
		},
		{
			name: "success",
			extenders: []tf.FakeExtender{
				{
					ExtenderName: "FakeExtender1",
					Predicates:   []tf.FitPredicate{tf.TruePredicateExtender},
				},
			},
			nodes:                 makeNodeList([]string{"a"}),
			filteredNodesStatuses: framework.NewNodeToStatus(make(map[string]*fwk.Status), absentStatus),
			expectsErr:            false,
			expectedNodes:         makeNodeList([]string{"a"}),
			expectedStatuses:      framework.NewNodeToStatus(make(map[string]*fwk.Status), absentStatus),
		},
		{
			name: "unschedulable",
			extenders: []tf.FakeExtender{
				{
					ExtenderName: "FakeExtender1",
					Predicates: []tf.FitPredicate{func(pod *v1.Pod, node fwk.NodeInfo) *fwk.Status {
						if node.Node().Name == "a" {
							return fwk.NewStatus(fwk.Success)
						}
						return fwk.NewStatus(fwk.Unschedulable, fmt.Sprintf("node %q is not allowed", node.Node().Name))
					}},
				},
			},
			nodes:                 makeNodeList([]string{"a", "b"}),
			filteredNodesStatuses: framework.NewNodeToStatus(make(map[string]*fwk.Status), absentStatus),
			expectsErr:            false,
			expectedNodes:         makeNodeList([]string{"a"}),
			expectedStatuses: framework.NewNodeToStatus(map[string]*fwk.Status{
				"b": fwk.NewStatus(fwk.Unschedulable, fmt.Sprintf("FakeExtender: node %q failed", "b")),
			}, absentStatus),
		},
		{
			name: "unschedulable and unresolvable",
			extenders: []tf.FakeExtender{
				{
					ExtenderName: "FakeExtender1",
					Predicates: []tf.FitPredicate{func(pod *v1.Pod, node fwk.NodeInfo) *fwk.Status {
						if node.Node().Name == "a" {
							return fwk.NewStatus(fwk.Success)
						}
						if node.Node().Name == "b" {
							return fwk.NewStatus(fwk.Unschedulable, fmt.Sprintf("node %q is not allowed", node.Node().Name))
						}
						return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, fmt.Sprintf("node %q is not allowed", node.Node().Name))
					}},
				},
			},
			nodes:                 makeNodeList([]string{"a", "b", "c"}),
			filteredNodesStatuses: framework.NewNodeToStatus(make(map[string]*fwk.Status), absentStatus),
			expectsErr:            false,
			expectedNodes:         makeNodeList([]string{"a"}),
			expectedStatuses: framework.NewNodeToStatus(map[string]*fwk.Status{
				"b": fwk.NewStatus(fwk.Unschedulable, fmt.Sprintf("FakeExtender: node %q failed", "b")),
				"c": fwk.NewStatus(fwk.UnschedulableAndUnresolvable, fmt.Sprintf("FakeExtender: node %q failed and unresolvable", "c")),
			}, absentStatus),
		},
		{
			name: "extender does not overwrite the previous statuses",
			extenders: []tf.FakeExtender{
				{
					ExtenderName: "FakeExtender1",
					Predicates: []tf.FitPredicate{func(pod *v1.Pod, node fwk.NodeInfo) *fwk.Status {
						if node.Node().Name == "a" {
							return fwk.NewStatus(fwk.Success)
						}
						if node.Node().Name == "b" {
							return fwk.NewStatus(fwk.Unschedulable, fmt.Sprintf("node %q is not allowed", node.Node().Name))
						}
						return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, fmt.Sprintf("node %q is not allowed", node.Node().Name))
					}},
				},
			},
			nodes: makeNodeList([]string{"a", "b"}),
			filteredNodesStatuses: framework.NewNodeToStatus(map[string]*fwk.Status{
				"c": fwk.NewStatus(fwk.Unschedulable, fmt.Sprintf("FakeFilterPlugin: node %q failed", "c")),
			}, absentStatus),
			expectsErr:    false,
			expectedNodes: makeNodeList([]string{"a"}),
			expectedStatuses: framework.NewNodeToStatus(map[string]*fwk.Status{
				"b": fwk.NewStatus(fwk.Unschedulable, fmt.Sprintf("FakeExtender: node %q failed", "b")),
				"c": fwk.NewStatus(fwk.Unschedulable, fmt.Sprintf("FakeFilterPlugin: node %q failed", "c")),
			}, absentStatus),
		},
		{
			name: "multiple extenders",
			extenders: []tf.FakeExtender{
				{
					ExtenderName: "FakeExtender1",
					Predicates: []tf.FitPredicate{func(pod *v1.Pod, node fwk.NodeInfo) *fwk.Status {
						if node.Node().Name == "a" {
							return fwk.NewStatus(fwk.Success)
						}
						if node.Node().Name == "b" {
							return fwk.NewStatus(fwk.Unschedulable, fmt.Sprintf("node %q is not allowed", node.Node().Name))
						}
						return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, fmt.Sprintf("node %q is not allowed", node.Node().Name))
					}},
				},
				{
					ExtenderName: "FakeExtender1",
					Predicates: []tf.FitPredicate{func(pod *v1.Pod, node fwk.NodeInfo) *fwk.Status {
						if node.Node().Name == "a" {
							return fwk.NewStatus(fwk.Success)
						}
						return fwk.NewStatus(fwk.Unschedulable, fmt.Sprintf("node %q is not allowed", node.Node().Name))
					}},
				},
			},
			nodes:                 makeNodeList([]string{"a", "b", "c"}),
			filteredNodesStatuses: framework.NewNodeToStatus(make(map[string]*fwk.Status), absentStatus),
			expectsErr:            false,
			expectedNodes:         makeNodeList([]string{"a"}),
			expectedStatuses: framework.NewNodeToStatus(map[string]*fwk.Status{
				"b": fwk.NewStatus(fwk.Unschedulable, fmt.Sprintf("FakeExtender: node %q failed", "b")),
				"c": fwk.NewStatus(fwk.UnschedulableAndUnresolvable, fmt.Sprintf("FakeExtender: node %q failed and unresolvable", "c")),
			}, absentStatus),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)
			var extenders []fwk.Extender
			for ii := range tt.extenders {
				extenders = append(extenders, &tt.extenders[ii])
			}

			pod := st.MakePod().Name("1").UID("1").Obj()
			got, err := findNodesThatPassExtenders(ctx, extenders, pod, tf.BuildNodeInfos(tt.nodes), tt.filteredNodesStatuses)
			nodes := make([]*v1.Node, len(got))
			for i := 0; i < len(got); i++ {
				nodes[i] = got[i].Node()
			}
			if tt.expectsErr {
				if err == nil {
					t.Error("Unexpected non-error")
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if diff := cmp.Diff(tt.expectedNodes, nodes); diff != "" {
					t.Errorf("filtered nodes (-want,+got):\n%s", diff)
				}
				if diff := nodeToStatusDiff(tt.expectedStatuses, tt.filteredNodesStatuses); diff != "" {
					t.Errorf("filtered statuses (-want,+got):\n%s", diff)
				}
			}
		})
	}
}

func TestSchedulerSchedulePod(t *testing.T) {
	fts := feature.Features{}
	tests := []struct {
		name               string
		registerPlugins    []tf.RegisterPluginFunc
		extenders          []tf.FakeExtender
		nodes              []*v1.Node
		pvcs               []v1.PersistentVolumeClaim
		pvs                []v1.PersistentVolume
		pod                *v1.Pod
		pods               []*v1.Pod
		wantNodes          sets.Set[string]
		wantEvaluatedNodes *int32
		wErr               error
	}{
		{
			registerPlugins: []tf.RegisterPluginFunc{
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				tf.RegisterFilterPlugin("FalseFilter", tf.NewFalseFilterPlugin),
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: map[string]string{"kubernetes.io/hostname": "node1"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node2", Labels: map[string]string{"kubernetes.io/hostname": "node2"}}},
			},
			pod:  st.MakePod().Name("2").UID("2").Obj(),
			name: "test 1",
			wErr: &framework.FitError{
				Pod:         st.MakePod().Name("2").UID("2").Obj(),
				NumAllNodes: 2,
				Diagnosis: framework.Diagnosis{
					NodeToStatus: framework.NewNodeToStatus(map[string]*fwk.Status{
						"node1": fwk.NewStatus(fwk.Unschedulable, tf.ErrReasonFake).WithPlugin("FalseFilter"),
						"node2": fwk.NewStatus(fwk.Unschedulable, tf.ErrReasonFake).WithPlugin("FalseFilter"),
					}, fwk.NewStatus(fwk.UnschedulableAndUnresolvable)),
					UnschedulablePlugins: sets.New("FalseFilter"),
				},
			},
		},
		{
			registerPlugins: []tf.RegisterPluginFunc{
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				tf.RegisterFilterPlugin("TrueFilter", tf.NewTrueFilterPlugin),
				tf.RegisterScorePlugin("EqualPrioritizerPlugin", tf.NewEqualPrioritizerPlugin(), 1),
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: map[string]string{"kubernetes.io/hostname": "node1"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node2", Labels: map[string]string{"kubernetes.io/hostname": "node2"}}},
			},
			pod:       st.MakePod().Name("ignore").UID("ignore").Obj(),
			wantNodes: sets.New("node1", "node2"),
			name:      "test 2",
			wErr:      nil,
		},
		{
			// Fits on a node where the pod ID matches the node name
			registerPlugins: []tf.RegisterPluginFunc{
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				tf.RegisterFilterPlugin("MatchFilter", tf.NewMatchFilterPlugin),
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: map[string]string{"kubernetes.io/hostname": "node1"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node2", Labels: map[string]string{"kubernetes.io/hostname": "node2"}}},
			},
			pod:       st.MakePod().Name("node2").UID("node2").Obj(),
			wantNodes: sets.New("node2"),
			name:      "test 3",
			wErr:      nil,
		},
		{
			registerPlugins: []tf.RegisterPluginFunc{
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				tf.RegisterFilterPlugin("TrueFilter", tf.NewTrueFilterPlugin),
				tf.RegisterScorePlugin("NumericMap", newNumericMapPlugin(), 1),
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "3", Labels: map[string]string{"kubernetes.io/hostname": "3"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "2", Labels: map[string]string{"kubernetes.io/hostname": "2"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "1", Labels: map[string]string{"kubernetes.io/hostname": "1"}}},
			},
			pod:       st.MakePod().Name("ignore").UID("ignore").Obj(),
			wantNodes: sets.New("3"),
			name:      "test 4",
			wErr:      nil,
		},
		{
			registerPlugins: []tf.RegisterPluginFunc{
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				tf.RegisterFilterPlugin("MatchFilter", tf.NewMatchFilterPlugin),
				tf.RegisterScorePlugin("NumericMap", newNumericMapPlugin(), 1),
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "3", Labels: map[string]string{"kubernetes.io/hostname": "3"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "2", Labels: map[string]string{"kubernetes.io/hostname": "2"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "1", Labels: map[string]string{"kubernetes.io/hostname": "1"}}},
			},
			pod:       st.MakePod().Name("2").UID("2").Obj(),
			wantNodes: sets.New("2"),
			name:      "test 5",
			wErr:      nil,
		},
		{
			registerPlugins: []tf.RegisterPluginFunc{
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				tf.RegisterFilterPlugin("TrueFilter", tf.NewTrueFilterPlugin),
				tf.RegisterScorePlugin("NumericMap", newNumericMapPlugin(), 1),
				tf.RegisterScorePlugin("ReverseNumericMap", newReverseNumericMapPlugin(), 2),
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "3", Labels: map[string]string{"kubernetes.io/hostname": "3"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "2", Labels: map[string]string{"kubernetes.io/hostname": "2"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "1", Labels: map[string]string{"kubernetes.io/hostname": "1"}}},
			},
			pod:       st.MakePod().Name("2").UID("2").Obj(),
			wantNodes: sets.New("1"),
			name:      "test 6",
			wErr:      nil,
		},
		{
			registerPlugins: []tf.RegisterPluginFunc{
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				tf.RegisterFilterPlugin("TrueFilter", tf.NewTrueFilterPlugin),
				tf.RegisterFilterPlugin("FalseFilter", tf.NewFalseFilterPlugin),
				tf.RegisterScorePlugin("NumericMap", newNumericMapPlugin(), 1),
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "3", Labels: map[string]string{"kubernetes.io/hostname": "3"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "2", Labels: map[string]string{"kubernetes.io/hostname": "2"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "1", Labels: map[string]string{"kubernetes.io/hostname": "1"}}},
			},
			pod:  st.MakePod().Name("2").UID("2").Obj(),
			name: "test 7",
			wErr: &framework.FitError{
				Pod:         st.MakePod().Name("2").UID("2").Obj(),
				NumAllNodes: 3,
				Diagnosis: framework.Diagnosis{
					NodeToStatus: framework.NewNodeToStatus(map[string]*fwk.Status{
						"3": fwk.NewStatus(fwk.Unschedulable, tf.ErrReasonFake).WithPlugin("FalseFilter"),
						"2": fwk.NewStatus(fwk.Unschedulable, tf.ErrReasonFake).WithPlugin("FalseFilter"),
						"1": fwk.NewStatus(fwk.Unschedulable, tf.ErrReasonFake).WithPlugin("FalseFilter"),
					}, fwk.NewStatus(fwk.UnschedulableAndUnresolvable)),
					UnschedulablePlugins: sets.New("FalseFilter"),
				},
			},
		},
		{
			registerPlugins: []tf.RegisterPluginFunc{
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				tf.RegisterFilterPlugin("NoPodsFilter", NewNoPodsFilterPlugin),
				tf.RegisterFilterPlugin("MatchFilter", tf.NewMatchFilterPlugin),
				tf.RegisterScorePlugin("NumericMap", newNumericMapPlugin(), 1),
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			},
			pods: []*v1.Pod{
				st.MakePod().Name("2").UID("2").Node("2").Phase(v1.PodRunning).Obj(),
			},
			pod: st.MakePod().Name("2").UID("2").Obj(),
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "1", Labels: map[string]string{"kubernetes.io/hostname": "1"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "2", Labels: map[string]string{"kubernetes.io/hostname": "2"}}},
			},
			name: "test 8",
			wErr: &framework.FitError{
				Pod:         st.MakePod().Name("2").UID("2").Obj(),
				NumAllNodes: 2,
				Diagnosis: framework.Diagnosis{
					NodeToStatus: framework.NewNodeToStatus(map[string]*fwk.Status{
						"1": fwk.NewStatus(fwk.Unschedulable, tf.ErrReasonFake).WithPlugin("MatchFilter"),
						"2": fwk.NewStatus(fwk.Unschedulable, tf.ErrReasonFake).WithPlugin("NoPodsFilter"),
					}, fwk.NewStatus(fwk.UnschedulableAndUnresolvable)),
					UnschedulablePlugins: sets.New("MatchFilter", "NoPodsFilter"),
				},
			},
		},
		{
			// Pod with existing PVC
			registerPlugins: []tf.RegisterPluginFunc{
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				tf.RegisterPreFilterPlugin(volumebinding.Name, frameworkruntime.FactoryAdapter(fts, volumebinding.New)),
				tf.RegisterFilterPlugin("TrueFilter", tf.NewTrueFilterPlugin),
				tf.RegisterScorePlugin("EqualPrioritizerPlugin", tf.NewEqualPrioritizerPlugin(), 1),
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: map[string]string{"kubernetes.io/hostname": "node1"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node2", Labels: map[string]string{"kubernetes.io/hostname": "node2"}}},
			},
			pvcs: []v1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "existingPVC", UID: types.UID("existingPVC"), Namespace: v1.NamespaceDefault},
					Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "existingPV"},
				},
			},
			pvs: []v1.PersistentVolume{
				{ObjectMeta: metav1.ObjectMeta{Name: "existingPV"}},
			},
			pod:       st.MakePod().Name("ignore").UID("ignore").Namespace(v1.NamespaceDefault).PVC("existingPVC").Obj(),
			wantNodes: sets.New("node1", "node2"),
			name:      "existing PVC",
			wErr:      nil,
		},
		{
			// Pod with non existing PVC
			registerPlugins: []tf.RegisterPluginFunc{
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				tf.RegisterPreFilterPlugin(volumebinding.Name, frameworkruntime.FactoryAdapter(fts, volumebinding.New)),
				tf.RegisterFilterPlugin("TrueFilter", tf.NewTrueFilterPlugin),
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: map[string]string{"kubernetes.io/hostname": "node1"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node2", Labels: map[string]string{"kubernetes.io/hostname": "node2"}}},
			},
			pod:  st.MakePod().Name("ignore").UID("ignore").PVC("unknownPVC").Obj(),
			name: "unknown PVC",
			wErr: &framework.FitError{
				Pod:         st.MakePod().Name("ignore").UID("ignore").PVC("unknownPVC").Obj(),
				NumAllNodes: 2,
				Diagnosis: framework.Diagnosis{
					NodeToStatus:         framework.NewNodeToStatus(make(map[string]*fwk.Status), fwk.NewStatus(fwk.UnschedulableAndUnresolvable, `persistentvolumeclaim "unknownPVC" not found`).WithPlugin("VolumeBinding")),
					PreFilterMsg:         `persistentvolumeclaim "unknownPVC" not found`,
					UnschedulablePlugins: sets.New(volumebinding.Name),
				},
			},
		},
		{
			// Pod with deleting PVC
			registerPlugins: []tf.RegisterPluginFunc{
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				tf.RegisterPreFilterPlugin(volumebinding.Name, frameworkruntime.FactoryAdapter(fts, volumebinding.New)),
				tf.RegisterFilterPlugin("TrueFilter", tf.NewTrueFilterPlugin),
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: map[string]string{"kubernetes.io/hostname": "node1"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node2", Labels: map[string]string{"kubernetes.io/hostname": "node2"}}},
			},
			pvcs: []v1.PersistentVolumeClaim{{ObjectMeta: metav1.ObjectMeta{Name: "existingPVC", UID: types.UID("existingPVC"), Namespace: v1.NamespaceDefault, DeletionTimestamp: &metav1.Time{}}}},
			pod:  st.MakePod().Name("ignore").UID("ignore").Namespace(v1.NamespaceDefault).PVC("existingPVC").Obj(),
			name: "deleted PVC",
			wErr: &framework.FitError{
				Pod:         st.MakePod().Name("ignore").UID("ignore").Namespace(v1.NamespaceDefault).PVC("existingPVC").Obj(),
				NumAllNodes: 2,
				Diagnosis: framework.Diagnosis{
					NodeToStatus:         framework.NewNodeToStatus(make(map[string]*fwk.Status), fwk.NewStatus(fwk.UnschedulableAndUnresolvable, `persistentvolumeclaim "existingPVC" is being deleted`).WithPlugin("VolumeBinding")),
					PreFilterMsg:         `persistentvolumeclaim "existingPVC" is being deleted`,
					UnschedulablePlugins: sets.New(volumebinding.Name),
				},
			},
		},
		{
			registerPlugins: []tf.RegisterPluginFunc{
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				tf.RegisterFilterPlugin("TrueFilter", tf.NewTrueFilterPlugin),
				tf.RegisterScorePlugin("FalseMap", newFalseMapPlugin(), 1),
				tf.RegisterScorePlugin("TrueMap", newTrueMapPlugin(), 2),
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "2", Labels: map[string]string{"kubernetes.io/hostname": "2"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "1", Labels: map[string]string{"kubernetes.io/hostname": "1"}}},
			},
			pod:  st.MakePod().Name("2").Obj(),
			name: "test error with priority map",
			wErr: fmt.Errorf("running Score plugins: %w", fmt.Errorf(`plugin "FalseMap" failed with: %w`, errPrioritize)),
		},
		{
			name: "test podtopologyspread plugin - 2 nodes with maxskew=1",
			registerPlugins: []tf.RegisterPluginFunc{
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				tf.RegisterPluginAsExtensions(
					podtopologyspread.Name,
					podTopologySpreadFunc,
					"PreFilter",
					"Filter",
				),
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: map[string]string{"kubernetes.io/hostname": "node1"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node2", Labels: map[string]string{"kubernetes.io/hostname": "node2"}}},
			},
			pod: st.MakePod().Name("p").UID("p").Label("foo", "").SpreadConstraint(1, "kubernetes.io/hostname", v1.DoNotSchedule, &metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{
					{
						Key:      "foo",
						Operator: metav1.LabelSelectorOpExists,
					},
				},
			}, nil, nil, nil, nil).Obj(),
			pods: []*v1.Pod{
				st.MakePod().Name("pod1").UID("pod1").Label("foo", "").Node("node1").Phase(v1.PodRunning).Obj(),
			},
			wantNodes: sets.New("node2"),
			wErr:      nil,
		},
		{
			name: "test podtopologyspread plugin - 3 nodes with maxskew=2",
			registerPlugins: []tf.RegisterPluginFunc{
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				tf.RegisterPluginAsExtensions(
					podtopologyspread.Name,
					podTopologySpreadFunc,
					"PreFilter",
					"Filter",
				),
				tf.RegisterScorePlugin("EqualPrioritizerPlugin", tf.NewEqualPrioritizerPlugin(), 1),
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: map[string]string{"kubernetes.io/hostname": "node1"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node2", Labels: map[string]string{"kubernetes.io/hostname": "node2"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node3", Labels: map[string]string{"kubernetes.io/hostname": "node3"}}},
			},
			pod: st.MakePod().Name("p").UID("p").Label("foo", "").SpreadConstraint(2, "kubernetes.io/hostname", v1.DoNotSchedule, &metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{
					{
						Key:      "foo",
						Operator: metav1.LabelSelectorOpExists,
					},
				},
			}, nil, nil, nil, nil).Obj(),
			pods: []*v1.Pod{
				st.MakePod().Name("pod1a").UID("pod1a").Label("foo", "").Node("node1").Phase(v1.PodRunning).Obj(),
				st.MakePod().Name("pod1b").UID("pod1b").Label("foo", "").Node("node1").Phase(v1.PodRunning).Obj(),
				st.MakePod().Name("pod2").UID("pod2").Label("foo", "").Node("node2").Phase(v1.PodRunning).Obj(),
			},
			wantNodes: sets.New("node2", "node3"),
			wErr:      nil,
		},
		{
			name: "test with filter plugin returning Unschedulable status",
			registerPlugins: []tf.RegisterPluginFunc{
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				tf.RegisterFilterPlugin(
					"FakeFilter",
					tf.NewFakeFilterPlugin(map[string]fwk.Code{"3": fwk.Unschedulable}),
				),
				tf.RegisterScorePlugin("NumericMap", newNumericMapPlugin(), 1),
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "3", Labels: map[string]string{"kubernetes.io/hostname": "3"}}},
			},
			pod:       st.MakePod().Name("test-filter").UID("test-filter").Obj(),
			wantNodes: nil,
			wErr: &framework.FitError{
				Pod:         st.MakePod().Name("test-filter").UID("test-filter").Obj(),
				NumAllNodes: 1,
				Diagnosis: framework.Diagnosis{
					NodeToStatus: framework.NewNodeToStatus(map[string]*fwk.Status{
						"3": fwk.NewStatus(fwk.Unschedulable, "injecting failure for pod test-filter").WithPlugin("FakeFilter"),
					}, fwk.NewStatus(fwk.UnschedulableAndUnresolvable)),
					UnschedulablePlugins: sets.New("FakeFilter"),
				},
			},
		},
		{
			name: "test with extender which filters out some Nodes",
			registerPlugins: []tf.RegisterPluginFunc{
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				tf.RegisterFilterPlugin(
					"FakeFilter",
					tf.NewFakeFilterPlugin(map[string]fwk.Code{"3": fwk.Unschedulable}),
				),
				tf.RegisterScorePlugin("NumericMap", newNumericMapPlugin(), 1),
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			},
			extenders: []tf.FakeExtender{
				{
					ExtenderName: "FakeExtender1",
					Predicates:   []tf.FitPredicate{tf.FalsePredicateExtender},
				},
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "1", Labels: map[string]string{"kubernetes.io/hostname": "1"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "2", Labels: map[string]string{"kubernetes.io/hostname": "2"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "3", Labels: map[string]string{"kubernetes.io/hostname": "3"}}},
			},
			pod:       st.MakePod().Name("test-filter").UID("test-filter").Obj(),
			wantNodes: nil,
			wErr: &framework.FitError{
				Pod:         st.MakePod().Name("test-filter").UID("test-filter").Obj(),
				NumAllNodes: 3,
				Diagnosis: framework.Diagnosis{
					NodeToStatus: framework.NewNodeToStatus(map[string]*fwk.Status{
						"1": fwk.NewStatus(fwk.Unschedulable, `FakeExtender: node "1" failed`),
						"2": fwk.NewStatus(fwk.Unschedulable, `FakeExtender: node "2" failed`),
						"3": fwk.NewStatus(fwk.Unschedulable, "injecting failure for pod test-filter").WithPlugin("FakeFilter"),
					}, fwk.NewStatus(fwk.UnschedulableAndUnresolvable)),
					UnschedulablePlugins: sets.New("FakeFilter", framework.ExtenderName),
				},
			},
		},
		{
			name: "test with filter plugin returning UnschedulableAndUnresolvable status",
			registerPlugins: []tf.RegisterPluginFunc{
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				tf.RegisterFilterPlugin(
					"FakeFilter",
					tf.NewFakeFilterPlugin(map[string]fwk.Code{"3": fwk.UnschedulableAndUnresolvable}),
				),
				tf.RegisterScorePlugin("NumericMap", newNumericMapPlugin(), 1),
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "3", Labels: map[string]string{"kubernetes.io/hostname": "3"}}},
			},
			pod:       st.MakePod().Name("test-filter").UID("test-filter").Obj(),
			wantNodes: nil,
			wErr: &framework.FitError{
				Pod:         st.MakePod().Name("test-filter").UID("test-filter").Obj(),
				NumAllNodes: 1,
				Diagnosis: framework.Diagnosis{
					NodeToStatus: framework.NewNodeToStatus(map[string]*fwk.Status{
						"3": fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "injecting failure for pod test-filter").WithPlugin("FakeFilter"),
					}, fwk.NewStatus(fwk.UnschedulableAndUnresolvable)),
					UnschedulablePlugins: sets.New("FakeFilter"),
				},
			},
		},
		{
			name: "test with partial failed filter plugin",
			registerPlugins: []tf.RegisterPluginFunc{
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				tf.RegisterFilterPlugin(
					"FakeFilter",
					tf.NewFakeFilterPlugin(map[string]fwk.Code{"1": fwk.Unschedulable}),
				),
				tf.RegisterScorePlugin("NumericMap", newNumericMapPlugin(), 1),
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "1", Labels: map[string]string{"kubernetes.io/hostname": "1"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "2", Labels: map[string]string{"kubernetes.io/hostname": "2"}}},
			},
			pod:       st.MakePod().Name("test-filter").UID("test-filter").Obj(),
			wantNodes: nil,
			wErr:      nil,
		},
		{
			name: "test prefilter plugin returning Unschedulable status",
			registerPlugins: []tf.RegisterPluginFunc{
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				tf.RegisterPreFilterPlugin(
					"FakePreFilter",
					tf.NewFakePreFilterPlugin("FakePreFilter", nil, fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "injected unschedulable status")),
				),
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "1", Labels: map[string]string{"kubernetes.io/hostname": "1"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "2", Labels: map[string]string{"kubernetes.io/hostname": "2"}}},
			},
			pod:       st.MakePod().Name("test-prefilter").UID("test-prefilter").Obj(),
			wantNodes: nil,
			wErr: &framework.FitError{
				Pod:         st.MakePod().Name("test-prefilter").UID("test-prefilter").Obj(),
				NumAllNodes: 2,
				Diagnosis: framework.Diagnosis{
					NodeToStatus:         framework.NewNodeToStatus(make(map[string]*fwk.Status), fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "injected unschedulable status").WithPlugin("FakePreFilter")),
					PreFilterMsg:         "injected unschedulable status",
					UnschedulablePlugins: sets.New("FakePreFilter"),
				},
			},
		},
		{
			name: "test prefilter plugin returning error status",
			registerPlugins: []tf.RegisterPluginFunc{
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				tf.RegisterPreFilterPlugin(
					"FakePreFilter",
					tf.NewFakePreFilterPlugin("FakePreFilter", nil, fwk.NewStatus(fwk.Error, "injected error status")),
				),
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "1", Labels: map[string]string{"kubernetes.io/hostname": "1"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "2", Labels: map[string]string{"kubernetes.io/hostname": "2"}}},
			},
			pod:       st.MakePod().Name("test-prefilter").UID("test-prefilter").Obj(),
			wantNodes: nil,
			wErr:      fmt.Errorf(`running PreFilter plugin "FakePreFilter": %w`, errors.New("injected error status")),
		},
		{
			name: "test prefilter plugin returning node",
			registerPlugins: []tf.RegisterPluginFunc{
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				tf.RegisterPreFilterPlugin(
					"FakePreFilter1",
					tf.NewFakePreFilterPlugin("FakePreFilter1", nil, nil),
				),
				tf.RegisterPreFilterPlugin(
					"FakePreFilter2",
					tf.NewFakePreFilterPlugin("FakePreFilter2", &fwk.PreFilterResult{NodeNames: sets.New("node2")}, nil),
				),
				tf.RegisterPreFilterPlugin(
					"FakePreFilter3",
					tf.NewFakePreFilterPlugin("FakePreFilter3", &fwk.PreFilterResult{NodeNames: sets.New("node1", "node2")}, nil),
				),
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: map[string]string{"kubernetes.io/hostname": "node1"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node2", Labels: map[string]string{"kubernetes.io/hostname": "node2"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node3", Labels: map[string]string{"kubernetes.io/hostname": "node3"}}},
			},
			pod:       st.MakePod().Name("test-prefilter").UID("test-prefilter").Obj(),
			wantNodes: sets.New("node2"),
			// since this case has no score plugin, we'll only try to find one node in Filter stage
			wantEvaluatedNodes: ptr.To[int32](1),
		},
		{
			name: "test prefilter plugin returning non-intersecting nodes",
			registerPlugins: []tf.RegisterPluginFunc{
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				tf.RegisterPreFilterPlugin(
					"FakePreFilter1",
					tf.NewFakePreFilterPlugin("FakePreFilter1", nil, nil),
				),
				tf.RegisterPreFilterPlugin(
					"FakePreFilter2",
					tf.NewFakePreFilterPlugin("FakePreFilter2", &fwk.PreFilterResult{NodeNames: sets.New("node2")}, nil),
				),
				tf.RegisterPreFilterPlugin(
					"FakePreFilter3",
					tf.NewFakePreFilterPlugin("FakePreFilter3", &fwk.PreFilterResult{NodeNames: sets.New("node1")}, nil),
				),
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: map[string]string{"kubernetes.io/hostname": "node1"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node2", Labels: map[string]string{"kubernetes.io/hostname": "node2"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node3", Labels: map[string]string{"kubernetes.io/hostname": "node3"}}},
			},
			pod: st.MakePod().Name("test-prefilter").UID("test-prefilter").Obj(),
			wErr: &framework.FitError{
				Pod:         st.MakePod().Name("test-prefilter").UID("test-prefilter").Obj(),
				NumAllNodes: 3,
				Diagnosis: framework.Diagnosis{
					NodeToStatus:         framework.NewNodeToStatus(make(map[string]*fwk.Status), fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "node(s) didn't satisfy plugin(s) [FakePreFilter2 FakePreFilter3] simultaneously")),
					UnschedulablePlugins: sets.New("FakePreFilter2", "FakePreFilter3"),
					PreFilterMsg:         "node(s) didn't satisfy plugin(s) [FakePreFilter2 FakePreFilter3] simultaneously",
				},
			},
		},
		{
			name: "test prefilter plugin returning empty node set",
			registerPlugins: []tf.RegisterPluginFunc{
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				tf.RegisterPreFilterPlugin(
					"FakePreFilter1",
					tf.NewFakePreFilterPlugin("FakePreFilter1", nil, nil),
				),
				tf.RegisterPreFilterPlugin(
					"FakePreFilter2",
					tf.NewFakePreFilterPlugin("FakePreFilter2", &fwk.PreFilterResult{NodeNames: sets.New[string]()}, nil),
				),
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: map[string]string{"kubernetes.io/hostname": "node1"}}},
			},
			pod: st.MakePod().Name("test-prefilter").UID("test-prefilter").Obj(),
			wErr: &framework.FitError{
				Pod:         st.MakePod().Name("test-prefilter").UID("test-prefilter").Obj(),
				NumAllNodes: 1,
				Diagnosis: framework.Diagnosis{
					NodeToStatus:         framework.NewNodeToStatus(make(map[string]*fwk.Status), fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "node(s) didn't satisfy plugin FakePreFilter2")),
					UnschedulablePlugins: sets.New("FakePreFilter2"),
					PreFilterMsg:         "node(s) didn't satisfy plugin FakePreFilter2",
				},
			},
		},
		{
			name: "test some nodes are filtered out by prefilter plugin and other are filtered out by filter plugin",
			registerPlugins: []tf.RegisterPluginFunc{
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				tf.RegisterPreFilterPlugin(
					"FakePreFilter",
					tf.NewFakePreFilterPlugin("FakePreFilter", &fwk.PreFilterResult{NodeNames: sets.New[string]("node2")}, nil),
				),
				tf.RegisterFilterPlugin(
					"FakeFilter",
					tf.NewFakeFilterPlugin(map[string]fwk.Code{"node2": fwk.Unschedulable}),
				),
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: map[string]string{"kubernetes.io/hostname": "node1"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node2", Labels: map[string]string{"kubernetes.io/hostname": "node2"}}},
			},
			pod: st.MakePod().Name("test-prefilter").UID("test-prefilter").Obj(),
			wErr: &framework.FitError{
				Pod:         st.MakePod().Name("test-prefilter").UID("test-prefilter").Obj(),
				NumAllNodes: 2,
				Diagnosis: framework.Diagnosis{
					NodeToStatus: framework.NewNodeToStatus(map[string]*fwk.Status{
						"node2": fwk.NewStatus(fwk.Unschedulable, "injecting failure for pod test-prefilter").WithPlugin("FakeFilter"),
					}, fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "node(s) didn't satisfy plugin(s) [FakePreFilter]")),
					UnschedulablePlugins: sets.New("FakePreFilter", "FakeFilter"),
					PreFilterMsg:         "",
				},
			},
		},
		{
			name: "test prefilter plugin returning skip",
			registerPlugins: []tf.RegisterPluginFunc{
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				tf.RegisterPreFilterPlugin(
					"FakePreFilter1",
					tf.NewFakePreFilterPlugin("FakeFilter1", nil, nil),
				),
				tf.RegisterFilterPlugin(
					"FakeFilter1",
					tf.NewFakeFilterPlugin(map[string]fwk.Code{
						"node1": fwk.Unschedulable,
					}),
				),
				tf.RegisterPluginAsExtensions("FakeFilter2", func(_ context.Context, configuration runtime.Object, f fwk.Handle) (fwk.Plugin, error) {
					return tf.FakePreFilterAndFilterPlugin{
						FakePreFilterPlugin: &tf.FakePreFilterPlugin{
							Result: nil,
							Status: fwk.NewStatus(fwk.Skip),
						},
						FakeFilterPlugin: &tf.FakeFilterPlugin{
							// This Filter plugin shouldn't be executed in the Filter extension point due to skip.
							// To confirm that, return the status code Error to all Nodes.
							FailedNodeReturnCodeMap: map[string]fwk.Code{
								"node1": fwk.Error, "node2": fwk.Error, "node3": fwk.Error,
							},
						},
					}, nil
				}, "PreFilter", "Filter"),
				tf.RegisterScorePlugin("EqualPrioritizerPlugin", tf.NewEqualPrioritizerPlugin(), 1),
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: map[string]string{"kubernetes.io/hostname": "node1"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node2", Labels: map[string]string{"kubernetes.io/hostname": "node2"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node3", Labels: map[string]string{"kubernetes.io/hostname": "node3"}}},
			},
			pod:                st.MakePod().Name("test-prefilter").UID("test-prefilter").Obj(),
			wantNodes:          sets.New("node2", "node3"),
			wantEvaluatedNodes: ptr.To[int32](3),
		},
		{
			name: "test all prescore plugins return skip",
			registerPlugins: []tf.RegisterPluginFunc{
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				tf.RegisterFilterPlugin("TrueFilter", tf.NewTrueFilterPlugin),
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
				tf.RegisterPluginAsExtensions("FakePreScoreAndScorePlugin", tf.NewFakePreScoreAndScorePlugin("FakePreScoreAndScorePlugin", 0,
					fwk.NewStatus(fwk.Skip, "fake skip"),
					fwk.NewStatus(fwk.Error, "this score function shouldn't be executed because this plugin returned Skip in the PreScore"),
				), "PreScore", "Score"),
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: map[string]string{"kubernetes.io/hostname": "node1"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node2", Labels: map[string]string{"kubernetes.io/hostname": "node2"}}},
			},
			pod:       st.MakePod().Name("ignore").UID("ignore").Obj(),
			wantNodes: sets.New("node1", "node2"),
		},
		{
			name: "test without score plugin no extra nodes are evaluated",
			registerPlugins: []tf.RegisterPluginFunc{
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				tf.RegisterFilterPlugin("TrueFilter", tf.NewTrueFilterPlugin),
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: map[string]string{"kubernetes.io/hostname": "node1"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node2", Labels: map[string]string{"kubernetes.io/hostname": "node2"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node3", Labels: map[string]string{"kubernetes.io/hostname": "node3"}}},
			},
			pod:                st.MakePod().Name("pod1").UID("pod1").Obj(),
			wantNodes:          sets.New("node1", "node2", "node3"),
			wantEvaluatedNodes: ptr.To[int32](1),
		},
		{
			name: "test no score plugin, prefilter plugin returning 2 nodes",
			registerPlugins: []tf.RegisterPluginFunc{
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				tf.RegisterPreFilterPlugin(
					"FakePreFilter",
					tf.NewFakePreFilterPlugin("FakePreFilter", &fwk.PreFilterResult{NodeNames: sets.New("node1", "node2")}, nil),
				),
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: map[string]string{"kubernetes.io/hostname": "node1"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node2", Labels: map[string]string{"kubernetes.io/hostname": "node2"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node3", Labels: map[string]string{"kubernetes.io/hostname": "node3"}}},
			},
			pod:       st.MakePod().Name("test-prefilter").UID("test-prefilter").Obj(),
			wantNodes: sets.New("node1", "node2"),
			// since this case has no score plugin, we'll only try to find one node in Filter stage
			wantEvaluatedNodes: ptr.To[int32](1),
		},
		{
			name: "test prefilter plugin returned an invalid node",
			registerPlugins: []tf.RegisterPluginFunc{
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				tf.RegisterPreFilterPlugin(
					"FakePreFilter",
					tf.NewFakePreFilterPlugin("FakePreFilter", &fwk.PreFilterResult{
						NodeNames: sets.New("invalid-node"),
					}, nil),
				),
				tf.RegisterFilterPlugin("TrueFilter", tf.NewTrueFilterPlugin),
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "1", Labels: map[string]string{"kubernetes.io/hostname": "1"}}}, {ObjectMeta: metav1.ObjectMeta{Name: "2", Labels: map[string]string{"kubernetes.io/hostname": "2"}}},
			},
			pod:       st.MakePod().Name("test-prefilter").UID("test-prefilter").Obj(),
			wantNodes: nil,
			wErr: &framework.FitError{
				Pod:         st.MakePod().Name("test-prefilter").UID("test-prefilter").Obj(),
				NumAllNodes: 2,
				Diagnosis: framework.Diagnosis{
					NodeToStatus:         framework.NewNodeToStatus(make(map[string]*fwk.Status), fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "node(s) didn't satisfy plugin(s) [FakePreFilter]")),
					UnschedulablePlugins: sets.New("FakePreFilter"),
				},
			},
		},
		{
			registerPlugins: []tf.RegisterPluginFunc{
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				tf.RegisterPreFilterPlugin(volumebinding.Name, frameworkruntime.FactoryAdapter(fts, volumebinding.New)),
				tf.RegisterFilterPlugin("TrueFilter", tf.NewTrueFilterPlugin),
				tf.RegisterScorePlugin("EqualPrioritizerPlugin", tf.NewEqualPrioritizerPlugin(), 1),
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: map[string]string{"kubernetes.io/hostname": "host1"}}},
			},
			pvcs: []v1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "PVC1", UID: types.UID("PVC1"), Namespace: v1.NamespaceDefault},
					Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "PV1"},
				},
			},
			pvs: []v1.PersistentVolume{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "PV1", UID: types.UID("PV1")},
					Spec: v1.PersistentVolumeSpec{
						NodeAffinity: &v1.VolumeNodeAffinity{
							Required: &v1.NodeSelector{
								NodeSelectorTerms: []v1.NodeSelectorTerm{
									{
										MatchExpressions: []v1.NodeSelectorRequirement{
											{
												Key:      "kubernetes.io/hostname",
												Operator: v1.NodeSelectorOpIn,
												Values:   []string{"host1"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			pod:       st.MakePod().Name("pod1").UID("pod1").Namespace(v1.NamespaceDefault).PVC("PVC1").Obj(),
			wantNodes: sets.New("node1"),
			name:      "hostname and nodename of the node do not match",
			wErr:      nil,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			var nodes []*v1.Node
			nodes = append(nodes, test.nodes...)

			cs := clientsetfake.NewClientset()
			informerFactory := informers.NewSharedInformerFactory(cs, 0)
			for _, pvc := range test.pvcs {
				metav1.SetMetaDataAnnotation(&pvc.ObjectMeta, volume.AnnBindCompleted, "true")
				if _, err := cs.CoreV1().PersistentVolumeClaims(pvc.Namespace).Create(ctx, &pvc, metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			for _, pv := range test.pvs {
				if _, err := cs.CoreV1().PersistentVolumes().Create(ctx, &pv, metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}
			}

			var extenders []fwk.Extender
			for ii := range test.extenders {
				extenders = append(extenders, &test.extenders[ii])
			}

			snapshot := internalcache.NewSnapshot(test.pods, nodes)
			// The extracted Scheduler reads the extenders off the Framework, so unlike
			// upstream they are registered here rather than on the Scheduler.
			schedFramework, err := tf.NewFramework(
				ctx,
				test.registerPlugins, "",
				frameworkruntime.WithSnapshotSharedLister(snapshot),
				frameworkruntime.WithInformerFactory(informerFactory),
				frameworkruntime.WithPodNominator(internalqueue.NewSchedulingQueue(nil, informerFactory)),
				frameworkruntime.WithExtenders(extenders),
			)
			if err != nil {
				t.Fatal(err)
			}

			sched := NewScheduler(snapshot, 0, 0, int32(len(nodes)))

			informerFactory.Start(ctx.Done())
			informerFactory.WaitForCacheSync(ctx.Done())

			podInfo := pendingPodForPod(t, test.pod)
			result, err := sched.schedulePod(ctx, schedFramework, podInfo)
			if err != test.wErr {
				gotFitErr, gotOK := err.(*framework.FitError)
				wantFitErr, wantOK := test.wErr.(*framework.FitError)
				if gotOK != wantOK {
					t.Errorf("Expected err to be FitError: %v, but got %v (error: %v)", wantOK, gotOK, err)
				} else if gotOK {
					if diff := cmp.Diff(wantFitErr, gotFitErr, schedulerCmpOpts...); diff != "" {
						t.Errorf("Unexpected fitErr for map (-want, +got):\n%s", diff)
					}
					if diff := nodeToStatusDiff(wantFitErr.Diagnosis.NodeToStatus, gotFitErr.Diagnosis.NodeToStatus); diff != "" {
						t.Errorf("Unexpected nodeToStatus within fitErr for map: (-want, +got):\n%s", diff)
					}
				}
			}
			if test.wantNodes != nil && !test.wantNodes.Has(result.SuggestedHost) {
				t.Errorf("Expected: %s, got: %s", test.wantNodes, result.SuggestedHost)
			}
			wantEvaluatedNodes := len(test.nodes)
			if test.wantEvaluatedNodes != nil {
				wantEvaluatedNodes = int(*test.wantEvaluatedNodes)
			}
			if test.wErr == nil && wantEvaluatedNodes != result.EvaluatedNodes {
				t.Errorf("Expected EvaluatedNodes: %d, got: %d", wantEvaluatedNodes, result.EvaluatedNodes)
			}
		})
	}
}

func TestFindFitAllError(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	nodes := makeNodeList([]string{"3", "2", "1"})
	scheduler := newTestScheduler(ctx, t, nodes)

	schedFramework, err := tf.NewFramework(
		ctx,
		[]tf.RegisterPluginFunc{
			tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
			tf.RegisterFilterPlugin("TrueFilter", tf.NewTrueFilterPlugin),
			tf.RegisterFilterPlugin("MatchFilter", tf.NewMatchFilterPlugin),
			tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
		},
		"",
		frameworkruntime.WithPodNominator(internalqueue.NewTestQueue(ctx, nil)),
		frameworkruntime.WithSnapshotSharedLister(scheduler.nodeInfoSnapshot),
	)
	if err != nil {
		t.Fatal(err)
	}

	podInfo := pendingPodForPod(t, &v1.Pod{})
	_, diagnosis, _, err := scheduler.findNodesThatFitPod(ctx, schedFramework, podInfo, false)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	expected := framework.Diagnosis{
		NodeToStatus: framework.NewNodeToStatus(map[string]*fwk.Status{
			"1": fwk.NewStatus(fwk.Unschedulable, tf.ErrReasonFake).WithPlugin("MatchFilter"),
			"2": fwk.NewStatus(fwk.Unschedulable, tf.ErrReasonFake).WithPlugin("MatchFilter"),
			"3": fwk.NewStatus(fwk.Unschedulable, tf.ErrReasonFake).WithPlugin("MatchFilter"),
		}, fwk.NewStatus(fwk.UnschedulableAndUnresolvable)),
		UnschedulablePlugins: sets.New("MatchFilter"),
	}
	if diff := cmp.Diff(expected, diagnosis, schedulerCmpOpts...); diff != "" {
		t.Errorf("Unexpected diagnosis (-want, +got):\n%s", diff)
	}
	if diff := nodeToStatusDiff(expected.NodeToStatus, diagnosis.NodeToStatus); diff != "" {
		t.Errorf("Unexpected nodeToStatus within diagnosis: (-want, +got):\n%s", diff)
	}
}

func TestFindFitSomeError(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	nodes := makeNodeList([]string{"3", "2", "1"})
	scheduler := newTestScheduler(ctx, t, nodes)

	fwk, err := tf.NewFramework(
		ctx,
		[]tf.RegisterPluginFunc{
			tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
			tf.RegisterFilterPlugin("TrueFilter", tf.NewTrueFilterPlugin),
			tf.RegisterFilterPlugin("MatchFilter", tf.NewMatchFilterPlugin),
			tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
		},
		"",
		frameworkruntime.WithPodNominator(internalqueue.NewTestQueue(ctx, nil)),
		frameworkruntime.WithSnapshotSharedLister(scheduler.nodeInfoSnapshot),
	)
	if err != nil {
		t.Fatal(err)
	}

	pod := st.MakePod().Name("1").UID("1").Obj()
	podInfo := pendingPodForPod(t, pod)
	_, diagnosis, _, err := scheduler.findNodesThatFitPod(ctx, fwk, podInfo, false)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if diagnosis.NodeToStatus.Len() != len(nodes)-1 {
		t.Errorf("unexpected failed status map: %v", diagnosis.NodeToStatus)
	}

	if diff := cmp.Diff(sets.New("MatchFilter"), diagnosis.UnschedulablePlugins); diff != "" {
		t.Errorf("Unexpected unschedulablePlugins: (-want, +got):\n%s", diff)
	}

	for _, node := range nodes {
		if node.Name == pod.Name {
			continue
		}
		t.Run(node.Name, func(t *testing.T) {
			status := diagnosis.NodeToStatus.Get(node.Name)
			reasons := status.Reasons()
			if len(reasons) != 1 || reasons[0] != tf.ErrReasonFake {
				t.Errorf("unexpected failures: %v", reasons)
			}
		})
	}
}

func TestFindFitPredicateCallCounts(t *testing.T) {
	tests := []struct {
		name          string
		pod           *v1.Pod
		expectedCount int32
	}{
		{
			name:          "nominated pods have lower priority, predicate is called once",
			pod:           st.MakePod().Name("1").UID("1").Priority(highPriority).Obj(),
			expectedCount: 1,
		},
		{
			name:          "nominated pods have higher priority, predicate is called twice",
			pod:           st.MakePod().Name("1").UID("1").Priority(lowPriority).Obj(),
			expectedCount: 2,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			nodes := makeNodeList([]string{"1"})

			plugin := tf.FakeFilterPlugin{}
			registerFakeFilterFunc := tf.RegisterFilterPlugin(
				"FakeFilter",
				func(_ context.Context, _ runtime.Object, fh fwk.Handle) (fwk.Plugin, error) {
					return &plugin, nil
				},
			)
			registerPlugins := []tf.RegisterPluginFunc{
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				registerFakeFilterFunc,
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			}
			logger, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			informerFactory := informers.NewSharedInformerFactory(clientsetfake.NewClientset(), 0)
			podInformer := informerFactory.Core().V1().Pods().Informer()
			err := podInformer.GetStore().Add(test.pod)
			if err != nil {
				t.Fatalf("Error adding pod to podInformer: %s", err)
			}
			scheduler := newTestScheduler(ctx, t, nodes)
			schedFramework, err := tf.NewFramework(
				ctx,
				registerPlugins, "",
				frameworkruntime.WithPodNominator(internalqueue.NewSchedulingQueue(nil, informerFactory)),
				frameworkruntime.WithSnapshotSharedLister(scheduler.nodeInfoSnapshot),
			)
			if err != nil {
				t.Fatal(err)
			}

			podinfo, err := framework.NewPodInfo(st.MakePod().UID("nominated").Priority(midPriority).Obj())
			if err != nil {
				t.Fatal(err)
			}
			err = podInformer.GetStore().Add(podinfo.Pod)
			if err != nil {
				t.Fatalf("Error adding nominated pod to podInformer: %s", err)
			}
			schedFramework.AddNominatedPod(logger, podinfo, &fwk.NominatingInfo{NominatingMode: fwk.ModeOverride, NominatedNodeName: "1"})

			podInfo := pendingPodForPod(t, test.pod)
			_, _, _, err = scheduler.findNodesThatFitPod(ctx, schedFramework, podInfo, false)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if test.expectedCount != plugin.NumFilterCalled {
				t.Errorf("predicate was called %d times, expected is %d", plugin.NumFilterCalled, test.expectedCount)
			}
		})
	}
}

func Test_prioritizeNodes(t *testing.T) {
	imageStatus1 := []v1.ContainerImage{
		{
			Names: []string{
				"gcr.io/40:latest",
				"gcr.io/40:v1",
			},
			SizeBytes: int64(80 * mb),
		},
		{
			Names: []string{
				"gcr.io/300:latest",
				"gcr.io/300:v1",
			},
			SizeBytes: int64(300 * mb),
		},
	}

	imageStatus2 := []v1.ContainerImage{
		{
			Names: []string{
				"gcr.io/300:latest",
			},
			SizeBytes: int64(300 * mb),
		},
		{
			Names: []string{
				"gcr.io/40:latest",
				"gcr.io/40:v1",
			},
			SizeBytes: int64(80 * mb),
		},
	}

	imageStatus3 := []v1.ContainerImage{
		{
			Names: []string{
				"gcr.io/600:latest",
			},
			SizeBytes: int64(600 * mb),
		},
		{
			Names: []string{
				"gcr.io/40:latest",
			},
			SizeBytes: int64(80 * mb),
		},
		{
			Names: []string{
				"gcr.io/900:latest",
			},
			SizeBytes: int64(900 * mb),
		},
	}
	tests := []struct {
		name                string
		pod                 *v1.Pod
		pods                []*v1.Pod
		nodes               []*v1.Node
		pluginRegistrations []tf.RegisterPluginFunc
		extenders           []tf.FakeExtender
		want                []fwk.NodePluginScores
	}{
		{
			name:  "the score from all plugins should be recorded in PluginToNodeScores",
			pod:   &v1.Pod{},
			nodes: []*v1.Node{makeNode("node1", 1000, schedutil.DefaultMemoryRequest*10), makeNode("node2", 1000, schedutil.DefaultMemoryRequest*10)},
			pluginRegistrations: []tf.RegisterPluginFunc{
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				tf.RegisterScorePlugin(noderesources.BalancedAllocationName, frameworkruntime.FactoryAdapter(feature.Features{}, noderesources.NewBalancedAllocation), 1),
				tf.RegisterScorePlugin("Node2Prioritizer", tf.NewNode2PrioritizerPlugin(), 1),
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			},
			extenders: nil,
			want: []fwk.NodePluginScores{
				{
					Name: "node1",
					Scores: []fwk.PluginScore{
						{
							Name:  "Node2Prioritizer",
							Score: 10,
						},
						{
							Name:  "NodeResourcesBalancedAllocation",
							Score: 0,
						},
					},
					TotalScore: 10,
				},
				{
					Name: "node2",
					Scores: []fwk.PluginScore{
						{
							Name:  "Node2Prioritizer",
							Score: 100,
						},
						{
							Name:  "NodeResourcesBalancedAllocation",
							Score: 0,
						},
					},
					TotalScore: 100,
				},
			},
		},
		{
			name:  "the score from extender should also be recorded in PluginToNodeScores with plugin scores",
			pod:   &v1.Pod{},
			nodes: []*v1.Node{makeNode("node1", 1000, schedutil.DefaultMemoryRequest*10), makeNode("node2", 1000, schedutil.DefaultMemoryRequest*10)},
			pluginRegistrations: []tf.RegisterPluginFunc{
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				tf.RegisterScorePlugin(noderesources.BalancedAllocationName, frameworkruntime.FactoryAdapter(feature.Features{}, noderesources.NewBalancedAllocation), 1),
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			},
			extenders: []tf.FakeExtender{
				{
					ExtenderName: "FakeExtender1",
					Weight:       1,
					Prioritizers: []tf.PriorityConfig{
						{
							Weight:   3,
							Function: tf.Node1PrioritizerExtender,
						},
					},
				},
				{
					ExtenderName: "FakeExtender2",
					Weight:       1,
					Prioritizers: []tf.PriorityConfig{
						{
							Weight:   2,
							Function: tf.Node2PrioritizerExtender,
						},
					},
				},
			},
			want: []fwk.NodePluginScores{
				{
					Name: "node1",
					Scores: []fwk.PluginScore{

						{
							Name:  "FakeExtender1",
							Score: 300,
						},
						{
							Name:  "FakeExtender2",
							Score: 20,
						},
						{
							Name:  "NodeResourcesBalancedAllocation",
							Score: 0,
						},
					},
					TotalScore: 320,
				},
				{
					Name: "node2",
					Scores: []fwk.PluginScore{
						{
							Name:  "FakeExtender1",
							Score: 30,
						},
						{
							Name:  "FakeExtender2",
							Score: 200,
						},
						{
							Name:  "NodeResourcesBalancedAllocation",
							Score: 0,
						},
					},
					TotalScore: 230,
				},
			},
		},
		{
			name:  "plugin which returned skip in preScore shouldn't be executed in the score phase",
			pod:   &v1.Pod{},
			nodes: []*v1.Node{makeNode("node1", 1000, schedutil.DefaultMemoryRequest*10), makeNode("node2", 1000, schedutil.DefaultMemoryRequest*10)},
			pluginRegistrations: []tf.RegisterPluginFunc{
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				tf.RegisterScorePlugin(noderesources.BalancedAllocationName, frameworkruntime.FactoryAdapter(feature.Features{}, noderesources.NewBalancedAllocation), 1),
				tf.RegisterScorePlugin("Node2Prioritizer", tf.NewNode2PrioritizerPlugin(), 1),
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
				tf.RegisterPluginAsExtensions("FakePreScoreAndScorePlugin", tf.NewFakePreScoreAndScorePlugin("FakePreScoreAndScorePlugin", 0,
					fwk.NewStatus(fwk.Skip, "fake skip"),
					fwk.NewStatus(fwk.Error, "this score function shouldn't be executed because this plugin returned Skip in the PreScore"),
				), "PreScore", "Score"),
			},
			extenders: nil,
			want: []fwk.NodePluginScores{
				{
					Name: "node1",
					Scores: []fwk.PluginScore{
						{
							Name:  "Node2Prioritizer",
							Score: 10,
						},
						{
							Name:  "NodeResourcesBalancedAllocation",
							Score: 0,
						},
					},
					TotalScore: 10,
				},
				{
					Name: "node2",
					Scores: []fwk.PluginScore{
						{
							Name:  "Node2Prioritizer",
							Score: 100,
						},
						{
							Name:  "NodeResourcesBalancedAllocation",
							Score: 0,
						},
					},
					TotalScore: 100,
				},
			},
		},
		{
			name:  "all score plugins are skipped",
			pod:   &v1.Pod{},
			nodes: []*v1.Node{makeNode("node1", 1000, schedutil.DefaultMemoryRequest*10), makeNode("node2", 1000, schedutil.DefaultMemoryRequest*10)},
			pluginRegistrations: []tf.RegisterPluginFunc{
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
				tf.RegisterPluginAsExtensions("FakePreScoreAndScorePlugin", tf.NewFakePreScoreAndScorePlugin("FakePreScoreAndScorePlugin", 0,
					fwk.NewStatus(fwk.Skip, "fake skip"),
					fwk.NewStatus(fwk.Error, "this score function shouldn't be executed because this plugin returned Skip in the PreScore"),
				), "PreScore", "Score"),
			},
			extenders: nil,
			want: []fwk.NodePluginScores{
				{Name: "node1", Scores: []fwk.PluginScore{}},
				{Name: "node2", Scores: []fwk.PluginScore{}},
			},
		},
		{
			name: "the score from Image Locality plugin with image in all nodes",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Image: "gcr.io/40",
						},
					},
				},
			},
			nodes: []*v1.Node{
				makeNode("node1", 1000, schedutil.DefaultMemoryRequest*10, imageStatus1...),
				makeNode("node2", 1000, schedutil.DefaultMemoryRequest*10, imageStatus2...),
				makeNode("node3", 1000, schedutil.DefaultMemoryRequest*10, imageStatus3...),
			},
			pluginRegistrations: []tf.RegisterPluginFunc{
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				tf.RegisterScorePlugin(imagelocality.Name, imagelocality.New, 1),
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			},
			extenders: nil,
			want: []fwk.NodePluginScores{
				{
					Name: "node1",
					Scores: []fwk.PluginScore{
						{
							Name:  "ImageLocality",
							Score: 5,
						},
					},
					TotalScore: 5,
				},
				{
					Name: "node2",
					Scores: []fwk.PluginScore{
						{
							Name:  "ImageLocality",
							Score: 5,
						},
					},
					TotalScore: 5,
				},
				{
					Name: "node3",
					Scores: []fwk.PluginScore{
						{
							Name:  "ImageLocality",
							Score: 5,
						},
					},
					TotalScore: 5,
				},
			},
		},
		{
			name: "the score from Image Locality plugin with image in partial nodes",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Image: "gcr.io/300",
						},
					},
				},
			},
			nodes: []*v1.Node{makeNode("node1", 1000, schedutil.DefaultMemoryRequest*10, imageStatus1...),
				makeNode("node2", 1000, schedutil.DefaultMemoryRequest*10, imageStatus2...),
				makeNode("node3", 1000, schedutil.DefaultMemoryRequest*10, imageStatus3...),
			},
			pluginRegistrations: []tf.RegisterPluginFunc{
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				tf.RegisterScorePlugin(imagelocality.Name, imagelocality.New, 1),
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			},
			extenders: nil,
			want: []fwk.NodePluginScores{
				{
					Name: "node1",
					Scores: []fwk.PluginScore{
						{
							Name:  "ImageLocality",
							Score: 18,
						},
					},
					TotalScore: 18,
				},
				{
					Name: "node2",
					Scores: []fwk.PluginScore{
						{
							Name:  "ImageLocality",
							Score: 18,
						},
					},
					TotalScore: 18,
				},
				{
					Name: "node3",
					Scores: []fwk.PluginScore{
						{
							Name:  "ImageLocality",
							Score: 0,
						},
					},
					TotalScore: 0,
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := clientsetfake.NewClientset()
			informerFactory := informers.NewSharedInformerFactory(client, 0)

			_, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			snapshot := newTestSnapshot(ctx, t, test.nodes)
			schedFramework, err := tf.NewFramework(
				ctx,
				test.pluginRegistrations, "",
				frameworkruntime.WithInformerFactory(informerFactory),
				frameworkruntime.WithSnapshotSharedLister(snapshot),
				frameworkruntime.WithClientSet(client),
				frameworkruntime.WithPodNominator(internalqueue.NewSchedulingQueue(nil, informerFactory)),
			)
			if err != nil {
				t.Fatalf("error creating framework: %+v", err)
			}

			state := framework.NewCycleState()
			var extenders []fwk.Extender
			for ii := range test.extenders {
				extenders = append(extenders, &test.extenders[ii])
			}
			nodeInfos, err := snapshot.NodeInfos().List()
			if err != nil {
				t.Fatalf("failed to list node from snapshot: %v", err)
			}
			nodesscores, err := prioritizeNodes(ctx, extenders, schedFramework, state, test.pod, nodeInfos)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			for i := range nodesscores {
				nodesscores[i].Randomizer = 0
			}
			for i := range nodesscores {
				sort.Slice(nodesscores[i].Scores, func(j, k int) bool {
					return nodesscores[i].Scores[j].Name < nodesscores[i].Scores[k].Name
				})
			}

			if diff := cmp.Diff(test.want, nodesscores); diff != "" {
				t.Errorf("returned nodes scores (-want,+got):\n%s", diff)
			}
		})
	}
}

func TestFairEvaluationForNodes(t *testing.T) {
	numAllNodes := 500
	nodeNames := make([]string, 0, numAllNodes)
	for i := 0; i < numAllNodes; i++ {
		nodeNames = append(nodeNames, strconv.Itoa(i))
	}
	nodes := makeNodeList(nodeNames)
	_, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// To make numAllNodes % nodesToFind != 0. Upstream this comes out of
	// numFeasibleNodesToFind() with percentageOfNodesToScore set to 30; here the number of
	// nodes to find is a constructor argument.
	nodesToFind := numAllNodes * 30 / 100
	sched := NewScheduler(newTestSnapshot(ctx, t, nodes), 0, 0, int32(nodesToFind))

	fwk, err := tf.NewFramework(
		ctx,
		[]tf.RegisterPluginFunc{
			tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
			tf.RegisterFilterPlugin("TrueFilter", tf.NewTrueFilterPlugin),
			tf.RegisterScorePlugin("EqualPrioritizerPlugin", tf.NewEqualPrioritizerPlugin(), 1),
			tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
		},
		"",
		frameworkruntime.WithPodNominator(internalqueue.NewTestQueue(ctx, nil)),
		frameworkruntime.WithSnapshotSharedLister(sched.nodeInfoSnapshot),
	)
	if err != nil {
		t.Fatal(err)
	}

	// Iterating over all nodes more than twice
	for i := 0; i < 2*(numAllNodes/nodesToFind+1); i++ {
		podInfo := pendingPodForPod(t, &v1.Pod{})
		nodesThatFit, _, _, err := sched.findNodesThatFitPod(ctx, fwk, podInfo, false)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if len(nodesThatFit) != nodesToFind {
			t.Errorf("got %d nodes filtered, want %d", len(nodesThatFit), nodesToFind)
		}
		if sched.nextStartNodeIndex != (i+1)*nodesToFind%numAllNodes {
			t.Errorf("got %d lastProcessedNodeIndex, want %d", sched.nextStartNodeIndex, (i+1)*nodesToFind%numAllNodes)
		}
	}
}

func TestPreferNominatedNodeFilterCallCounts(t *testing.T) {
	tests := []struct {
		name              string
		pod               *v1.Pod
		nodeReturnCodeMap map[string]fwk.Code
		expectedCount     int32
	}{
		{
			name:          "pod has the nominated node set, filter is called only once",
			pod:           st.MakePod().Name("p_with_nominated_node").UID("p").Priority(highPriority).NominatedNodeName("node1").Obj(),
			expectedCount: 1,
		},
		{
			name:          "pod without the nominated pod, filter is called for each node",
			pod:           st.MakePod().Name("p_without_nominated_node").UID("p").Priority(highPriority).Obj(),
			expectedCount: 3,
		},
		{
			name:              "nominated pod cannot pass the filter, filter is called for each node",
			pod:               st.MakePod().Name("p_with_nominated_node").UID("p").Priority(highPriority).NominatedNodeName("node1").Obj(),
			nodeReturnCodeMap: map[string]fwk.Code{"node1": fwk.Unschedulable},
			expectedCount:     4,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			// create three nodes in the cluster.
			nodes := makeNodeList([]string{"node1", "node2", "node3"})
			client := clientsetfake.NewClientset(test.pod)
			informerFactory := informers.NewSharedInformerFactory(client, 0)
			plugin := tf.FakeFilterPlugin{FailedNodeReturnCodeMap: test.nodeReturnCodeMap}
			registerFakeFilterFunc := tf.RegisterFilterPlugin(
				"FakeFilter",
				func(_ context.Context, _ runtime.Object, fh fwk.Handle) (fwk.Plugin, error) {
					return &plugin, nil
				},
			)
			registerPlugins := []tf.RegisterPluginFunc{
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				registerFakeFilterFunc,
				tf.RegisterScorePlugin("EqualPrioritizerPlugin", tf.NewEqualPrioritizerPlugin(), 1),
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			}
			snapshot := internalcache.NewSnapshot(nil, nodes)
			fwk, err := tf.NewFramework(
				ctx,
				registerPlugins, "",
				frameworkruntime.WithClientSet(client),
				frameworkruntime.WithPodNominator(internalqueue.NewSchedulingQueue(nil, informerFactory)),
				frameworkruntime.WithSnapshotSharedLister(snapshot),
			)
			if err != nil {
				t.Fatal(err)
			}

			sched := NewScheduler(snapshot, 0, 0, int32(len(nodes)))

			podInfo := pendingPodForPod(t, test.pod)
			_, _, _, err = sched.findNodesThatFitPod(ctx, fwk, podInfo, false)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if test.expectedCount != plugin.NumFilterCalled {
				t.Errorf("predicate was called %d times, expected is %d", plugin.NumFilterCalled, test.expectedCount)
			}
		})
	}
}

// TestFindNodesThatPassFiltersNumNodesToFind covers the numNodesToFind cap, which has no
// upstream counterpart: upstream derives it from numFeasibleNodesToFind(), which never
// returns more than the number of nodes, while here it is supplied by the caller and callers
// pass math.MaxInt32 to mean "all of them".
func TestFindNodesThatPassFiltersNumNodesToFind(t *testing.T) {
	tests := []struct {
		name           string
		numNodesToFind int32
		wantNodes      int
	}{
		{
			name:           "fewer nodes wanted than available",
			numNodesToFind: 2,
			wantNodes:      2,
		},
		{
			name:           "exactly as many nodes wanted as available",
			numNodesToFind: 3,
			wantNodes:      3,
		},
		{
			name:           "more nodes wanted than available",
			numNodesToFind: math.MaxInt32,
			wantNodes:      3,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			nodes := makeNodeList([]string{"node1", "node2", "node3"})
			snapshot := newTestSnapshot(ctx, t, nodes)
			schedFramework, err := tf.NewFramework(
				ctx,
				[]tf.RegisterPluginFunc{
					tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
					tf.RegisterFilterPlugin("TrueFilter", tf.NewTrueFilterPlugin),
					// A score plugin, so that numNodesToFind isn't forced down to 1.
					tf.RegisterScorePlugin("EqualPrioritizerPlugin", tf.NewEqualPrioritizerPlugin(), 1),
					tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
				},
				"",
				frameworkruntime.WithPodNominator(internalqueue.NewTestQueue(ctx, nil)),
				frameworkruntime.WithSnapshotSharedLister(snapshot),
			)
			if err != nil {
				t.Fatal(err)
			}

			sched := NewScheduler(snapshot, 0, 0, test.numNodesToFind)
			gotNodes, _, _, err := sched.findNodesThatFitPod(ctx, schedFramework, pendingPodForPod(t, st.MakePod().Name("p").UID("p").Obj()), false)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(gotNodes) != test.wantNodes {
				t.Errorf("got %d feasible nodes, want %d", len(gotNodes), test.wantNodes)
			}
			seen := sets.New[string]()
			for _, nodeInfo := range gotNodes {
				name := nodeInfo.Node().Name
				if seen.Has(name) {
					t.Errorf("node %q was returned more than once", name)
				}
				seen.Insert(name)
			}
		})
	}
}

// TestFindAllNodesThatFitPod covers FindAllNodesThatFitPod, which has no upstream
// counterpart: unlike findNodesThatFitPod() it evaluates every node, so it neither takes the
// nominated node shortcut nor stops once numNodesToFind feasible nodes have been found.
func TestFindAllNodesThatFitPod(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	nodes := makeNodeList([]string{"node1", "node2", "node3"})
	registerPlugins := []tf.RegisterPluginFunc{
		tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
		tf.RegisterFilterPlugin("FakeFilter", tf.NewFakeFilterPlugin(map[string]fwk.Code{"node3": fwk.Unschedulable})),
		tf.RegisterScorePlugin("EqualPrioritizerPlugin", tf.NewEqualPrioritizerPlugin(), 1),
		tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
	}
	pod := st.MakePod().Name("p").UID("p").NominatedNodeName("node1").Obj()

	// numNodesToFind is 1, so the regular path stops as soon as one node fits.
	newSchedulerAndFramework := func(t *testing.T) (*Scheduler, framework.Framework) {
		t.Helper()
		snapshot := newTestSnapshot(ctx, t, nodes)
		schedFramework, err := tf.NewFramework(
			ctx,
			registerPlugins, "",
			frameworkruntime.WithPodNominator(internalqueue.NewTestQueue(ctx, nil)),
			frameworkruntime.WithSnapshotSharedLister(snapshot),
		)
		if err != nil {
			t.Fatal(err)
		}
		return NewScheduler(snapshot, 0, 0, 1), schedFramework
	}

	nodeNames := func(nodeInfos []fwk.NodeInfo) []string {
		names := make([]string, 0, len(nodeInfos))
		for _, nodeInfo := range nodeInfos {
			names = append(names, nodeInfo.Node().Name)
		}
		sort.Strings(names)
		return names
	}

	sched, schedFramework := newSchedulerAndFramework(t)
	gotNodes, _, _, err := sched.findNodesThatFitPod(ctx, schedFramework, pendingPodForPod(t, pod), false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if diff := cmp.Diff([]string{"node1"}, nodeNames(gotNodes)); diff != "" {
		t.Errorf("Unexpected nodes from findNodesThatFitPod (-want, +got):\n%s", diff)
	}

	sched, schedFramework = newSchedulerAndFramework(t)
	gotNodes, _, _, err = sched.FindAllNodesThatFitPod(ctx, schedFramework, pendingPodForPod(t, pod))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if diff := cmp.Diff([]string{"node1", "node2"}, nodeNames(gotNodes)); diff != "" {
		t.Errorf("Unexpected nodes from FindAllNodesThatFitPod (-want, +got):\n%s", diff)
	}
}

func TestEvaluateNominatedNode(t *testing.T) {
	tests := map[string]struct {
		allNodes       []*v1.Node
		placementNodes []string
		pod            *v1.Pod
		wantNodeList   []string
		wantError      bool
	}{
		"When NNN is present in both snapshot and placement, returns node": {
			allNodes: []*v1.Node{
				st.MakeNode().Name("n1").Obj(),
				st.MakeNode().Name("n2").Obj(),
			},
			placementNodes: []string{"n1"},
			pod:            st.MakePod().NominatedNodeName("n1").Obj(),
			wantNodeList:   []string{"n1"},
		},
		"When NNN is present in snapshot but not in placement, returns success": {
			allNodes: []*v1.Node{
				st.MakeNode().Name("n1").Obj(),
				st.MakeNode().Name("n2").Obj(),
			},
			placementNodes: []string{"n1"},
			pod:            st.MakePod().NominatedNodeName("n2").Obj(),
			wantError:      false,
		},
		"When NNN is not present in snapshot, returns error": {
			allNodes: []*v1.Node{
				st.MakeNode().Name("n1").Obj(),
				st.MakeNode().Name("n2").Obj(),
			},
			placementNodes: []string{"n1"},
			pod:            st.MakePod().NominatedNodeName("n3").Obj(),
			wantError:      true,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)
			snapshot := internalcache.NewSnapshot(nil, tt.allNodes)
			placement := &fwk.Placement{}
			for _, nodeName := range tt.placementNodes {
				node, err := snapshot.Get(nodeName)
				if err != nil {
					t.Fatalf("Error getting node %s: %v", nodeName, err)
				}
				placement.Nodes = append(placement.Nodes, node)
			}
			err := snapshot.AssumePlacement(placement)
			if err != nil {
				t.Fatalf("AssumePlacement failed: %v", err)
			}
			fw, err := tf.NewFramework(
				ctx,
				[]tf.RegisterPluginFunc{
					tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
					tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
				},
				"",
				frameworkruntime.WithSnapshotSharedLister(snapshot),
			)
			if err != nil {
				t.Fatalf("NewFramework failed: %v", err)
			}
			sched := NewScheduler(snapshot, 0, 0, int32(len(tt.allNodes)))

			gotNodes, err := sched.evaluateNominatedNode(ctx, tt.pod, fw, framework.NewCycleState(), "", framework.Diagnosis{})

			if (err != nil) != tt.wantError {
				t.Errorf("Unexpected error, want error: %v, got: %v", tt.wantError, err)
			}
			gotNodeNames := make([]string, len(gotNodes))
			for i, n := range gotNodes {
				gotNodeNames[i] = n.Node().Name
			}
			if diff := cmp.Diff(tt.wantNodeList, gotNodeNames, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("Unexpected nodes (-want, +got):\n%s", diff)
			}
		})
	}
}

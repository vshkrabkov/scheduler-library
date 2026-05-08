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

package simulator

import (
	"context"

	"sigs.k8s.io/scheduler-library/pkg/snapshot"
	"sigs.k8s.io/scheduler-library/pkg/state"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/events"
	schedulerapi "k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/backend/cache"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
	"k8s.io/kubernetes/pkg/scheduler/profile"
)

type SchedulingSimulator struct {
	cfg             *schedulerapi.KubeSchedulerConfiguration
	informerFactory informers.SharedInformerFactory
	broadcaster     events.EventBroadcaster
}

// NewSchedulingSimulator creates a new SchedulingSimulator.
func NewSchedulingSimulator(cfg *schedulerapi.KubeSchedulerConfiguration, informerFactory informers.SharedInformerFactory, broadcaster events.EventBroadcaster) *SchedulingSimulator {
	return &SchedulingSimulator{
		cfg:             cfg,
		informerFactory: informerFactory,
		broadcaster:     broadcaster,
	}
}

// NewClusterState initializes a new runtime cluster state.
func (s *SchedulingSimulator) NewClusterState(ctx context.Context) (*state.ClusterState, error) {
	metrics.Register()
	internalCache := cache.New(ctx, nil, false)
	snap := cache.NewEmptySnapshot()

	frameworks, err := s.buildFrameworks(ctx, snap)
	if err != nil {
		return nil, err
	}

	return state.NewClusterState(internalCache, frameworks, snap), nil
}

// NewClusterSnapshot initializes a new snapshot with the provided pods and nodes.
func (s *SchedulingSimulator) NewClusterSnapshot(ctx context.Context, pods []*v1.Pod, nodes []*v1.Node) (*snapshot.ClusterSnapshot, error) {
	snap := cache.NewSnapshot(pods, nodes)

	frameworks, err := s.buildFrameworks(ctx, snap)
	if err != nil {
		return nil, err
	}

	return snapshot.NewClusterSnapshot(snap, frameworks), nil
}

func (s *SchedulingSimulator) buildFrameworks(ctx context.Context, snap *cache.Snapshot) (profile.Map, error) {
	registry := plugins.NewInTreeRegistry()
	recorderFactory := profile.NewRecorderFactory(s.broadcaster)
	return profile.NewMap(ctx, s.cfg.Profiles, registry, recorderFactory,
		frameworkruntime.WithSnapshotSharedLister(snap),
		frameworkruntime.WithInformerFactory(s.informerFactory),
	)
}

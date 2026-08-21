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

Tests for the logic extracted into framework.go.

Copied from kubernetes/kubernetes/pkg/scheduler/scheduler_test.go (TestSchedulerCreation) and
adjusted to the extracted API: NewProfileMap() builds only the profiles and the extenders, so
the cases covering the parts of scheduler.New() that were not extracted (the queue, the event
handlers, percentageOfNodesToScore) are not copied, and the extenders are asserted directly on
buildExtenders() because NewProfileMap() does not expose an option to pass them in yet.

*/

import (
	"context"
	"sort"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/events"
	"k8s.io/klog/v2/ktesting"
	schedulerapi "k8s.io/kubernetes/pkg/scheduler/apis/config"
	internalcache "k8s.io/kubernetes/pkg/scheduler/backend/cache"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/noderesources"
	"k8s.io/kubernetes/pkg/scheduler/profile"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
)

func minimalProfile(schedulerName string) schedulerapi.KubeSchedulerProfile {
	return schedulerapi.KubeSchedulerProfile{
		SchedulerName: schedulerName,
		Plugins: &schedulerapi.Plugins{
			QueueSort: schedulerapi.PluginSet{Enabled: []schedulerapi.Plugin{{Name: "PrioritySort"}}},
			Bind:      schedulerapi.PluginSet{Enabled: []schedulerapi.Plugin{{Name: "DefaultBinder"}}},
		},
	}
}

func newTestProfileMap(ctx context.Context, opts ...Option) (*ProfileMap, error) {
	client := fake.NewClientset()
	informerFactory := informers.NewSharedInformerFactory(client, 0)
	eventBroadcaster := events.NewBroadcaster(&events.EventSinkImpl{Interface: client.EventsV1()})

	return NewProfileMap(
		ctx,
		client,
		informerFactory,
		profile.NewRecorderFactory(eventBroadcaster),
		nil, /* podNominator */
		nil, /* podActivator */
		nil, /* apiCacher */
		internalcache.NewEmptySnapshot(),
		opts...,
	)
}

func TestNewProfileMap(t *testing.T) {
	cases := []struct {
		name         string
		opts         []Option
		wantErr      string
		wantProfiles []string
	}{
		{
			name:         "no profiles given, the default profile is applied",
			wantProfiles: []string{v1.DefaultSchedulerName},
		},
		{
			name:         "single profile",
			opts:         []Option{WithProfiles(minimalProfile("default-scheduler"))},
			wantProfiles: []string{"default-scheduler"},
		},
		{
			name:         "multiple profiles",
			opts:         []Option{WithProfiles(minimalProfile("foo"), minimalProfile("bar"))},
			wantProfiles: []string{"bar", "foo"},
		},
		{
			name:    "repeated profiles",
			opts:    []Option{WithProfiles(minimalProfile("foo"), minimalProfile("bar"), minimalProfile("foo"))},
			wantErr: `duplicate profile with scheduler name "foo"`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			profileMap, err := newTestProfileMap(ctx, tc.opts...)

			if len(tc.wantErr) != 0 {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("got error %q, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Failed to create the profile map: %v", err)
			}

			profiles := make([]string, 0, len(profileMap.Map))
			for name := range profileMap.Map {
				profiles = append(profiles, name)
			}
			sort.Strings(profiles)
			if diff := cmp.Diff(tc.wantProfiles, profiles); diff != "" {
				t.Errorf("unexpected profiles (-want, +got):\n%s", diff)
			}
		})
	}
}

// TestProfileMapFrameworkForPod covers FrameworkForPod, which is the extracted counterpart of
// the upstream Scheduler.frameworkForPod. Unlike upstream it falls back to the default
// scheduler name for pods that carry none, which happens when running simulations.
func TestProfileMapFrameworkForPod(t *testing.T) {
	tests := []struct {
		name          string
		profiles      []schedulerapi.KubeSchedulerProfile
		pod           *v1.Pod
		wantProfile   string
		wantErrSuffix string
	}{
		{
			name:        "pod naming an existing profile",
			profiles:    []schedulerapi.KubeSchedulerProfile{minimalProfile("foo"), minimalProfile("bar")},
			pod:         st.MakePod().Name("p").SchedulerName("bar").Obj(),
			wantProfile: "bar",
		},
		{
			name:        "pod without a scheduler name falls back to the default one",
			profiles:    []schedulerapi.KubeSchedulerProfile{minimalProfile(v1.DefaultSchedulerName)},
			pod:         st.MakePod().Name("p").SchedulerName("").Obj(),
			wantProfile: v1.DefaultSchedulerName,
		},
		{
			name:          "pod naming an unknown profile",
			profiles:      []schedulerapi.KubeSchedulerProfile{minimalProfile("foo")},
			pod:           st.MakePod().Name("p").SchedulerName("bar").Obj(),
			wantErrSuffix: `profile not found for scheduler name "bar"`,
		},
		{
			name:          "pod without a scheduler name and without a default profile",
			profiles:      []schedulerapi.KubeSchedulerProfile{minimalProfile("foo")},
			pod:           st.MakePod().Name("p").SchedulerName("").Obj(),
			wantErrSuffix: `profile not found for scheduler name ""`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			profileMap, err := newTestProfileMap(ctx, WithProfiles(tc.profiles...))
			if err != nil {
				t.Fatalf("Failed to create the profile map: %v", err)
			}

			schedFramework, err := profileMap.FrameworkForPod(tc.pod)
			if tc.wantErrSuffix != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrSuffix) {
					t.Fatalf("got error %q, want %q", err, tc.wantErrSuffix)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if got := schedFramework.ProfileName(); got != tc.wantProfile {
				t.Errorf("got profile %q, want %q", got, tc.wantProfile)
			}
		})
	}
}

// TestBuildExtenders covers buildExtenders. Upstream this is exercised through the
// "With extenders" case of TestSchedulerCreation; NewProfileMap does not expose an option to
// pass extenders in, so they are built directly here.
func TestBuildExtenders(t *testing.T) {
	tests := []struct {
		name          string
		extenders     []schedulerapi.Extender
		profiles      []schedulerapi.KubeSchedulerProfile
		wantExtenders []string
		wantErrSuffix string
	}{
		{
			name: "no extenders",
		},
		{
			name: "single extender",
			extenders: []schedulerapi.Extender{
				{URLPrefix: "http://extender.kube-system/"},
			},
			wantExtenders: []string{"http://extender.kube-system/"},
		},
		{
			name: "ignorable extenders are moved to the tail",
			extenders: []schedulerapi.Extender{
				{URLPrefix: "http://ignorable.kube-system/", Ignorable: true},
				{URLPrefix: "http://required.kube-system/"},
			},
			wantExtenders: []string{"http://required.kube-system/", "http://ignorable.kube-system/"},
		},
		{
			name: "invalid extender TLS config",
			extenders: []schedulerapi.Extender{
				{
					URLPrefix: "https://extender.kube-system/",
					TLSConfig: &schedulerapi.ExtenderTLSConfig{Insecure: true, CAFile: "/does/not/exist.crt"},
				},
			},
			wantErrSuffix: "specifying a root certificates file with the insecure flag is not allowed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logger, _ := ktesting.NewTestContext(t)

			gotExtenders, err := buildExtenders(logger, tc.extenders, tc.profiles)
			if tc.wantErrSuffix != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrSuffix) {
					t.Fatalf("got error %q, want %q", err, tc.wantErrSuffix)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			var gotNames []string
			for _, e := range gotExtenders {
				gotNames = append(gotNames, e.Name())
			}
			if diff := cmp.Diff(tc.wantExtenders, gotNames); diff != "" {
				t.Errorf("unexpected extenders (-want, +got):\n%s", diff)
			}
		})
	}
}

// TestBuildExtendersIgnoredResources checks that resources the extenders declare as ignored by
// the scheduler are appended to the NodeResourcesFit plugin args of every profile.
func TestBuildExtendersIgnoredResources(t *testing.T) {
	extenders := []schedulerapi.Extender{
		{
			URLPrefix: "http://extender.kube-system/",
			ManagedResources: []schedulerapi.ExtenderManagedResource{
				{Name: "example.com/foo", IgnoredByScheduler: true},
				{Name: "example.com/bar", IgnoredByScheduler: false},
			},
		},
	}

	t.Run("the ignored resources are added to the NodeResourcesFit args", func(t *testing.T) {
		logger, _ := ktesting.NewTestContext(t)

		prof := minimalProfile("foo")
		prof.PluginConfig = []schedulerapi.PluginConfig{
			{Name: noderesources.Name, Args: &schedulerapi.NodeResourcesFitArgs{}},
		}
		profiles := []schedulerapi.KubeSchedulerProfile{prof}

		if _, err := buildExtenders(logger, extenders, profiles); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		args, ok := profiles[0].PluginConfig[0].Args.(*schedulerapi.NodeResourcesFitArgs)
		if !ok {
			t.Fatalf("Unexpected args type %T for plugin %s", profiles[0].PluginConfig[0].Args, noderesources.Name)
		}
		if diff := cmp.Diff([]string{"example.com/foo"}, args.IgnoredResources); diff != "" {
			t.Errorf("unexpected ignored resources (-want, +got):\n%s", diff)
		}
	})

	t.Run("a profile without NodeResourcesFit args is rejected", func(t *testing.T) {
		logger, _ := ktesting.NewTestContext(t)

		profiles := []schedulerapi.KubeSchedulerProfile{minimalProfile("foo")}

		_, err := buildExtenders(logger, extenders, profiles)
		if err == nil || !strings.Contains(err.Error(), "can't find NodeResourcesFitArgs in plugin config") {
			t.Fatalf("got error %q, want it to mention the missing NodeResourcesFitArgs", err)
		}
	})
}

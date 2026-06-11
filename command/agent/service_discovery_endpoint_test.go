// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package agent

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/hashicorp/nomad/ci"
	"github.com/hashicorp/nomad/nomad/mock"
	"github.com/hashicorp/nomad/nomad/structs"
	"github.com/stretchr/testify/require"
)

func promSDTestAlloc() *structs.Allocation {
	alloc := mock.Alloc()
	alloc.Name = alloc.JobID + ".web[3]"
	alloc.ClientStatus = structs.AllocClientStatusRunning
	alloc.Job.Meta = map[string]string{"user-id": "github|abc"}
	alloc.Job.LookupTaskGroup(alloc.TaskGroup).Meta = map[string]string{"app_id": "kling"}
	alloc.AllocatedResources.Shared.Ports = structs.AllocatedPorts{
		{Label: "http", Value: 20001, To: 8080, HostIP: "10.0.0.5"},
		{Label: "metrics", Value: 20002, To: 9090, HostIP: "10.0.0.5"},
	}
	return alloc
}

func TestAllocPromSDTargetGroups(t *testing.T) {
	ci.Parallel(t)

	nodeLabels := map[string]string{"__meta_nomad_node_id": "node-1"}
	alloc := promSDTestAlloc()

	groups := allocPromSDTargetGroups(alloc, nodeLabels, "")
	require.Len(t, groups, 2)

	byPort := map[string]*PromSDTargetGroup{}
	for _, g := range groups {
		byPort[g.Labels["__meta_nomad_port_label"]] = g
	}

	metrics := byPort["metrics"]
	require.NotNil(t, metrics)
	require.Equal(t, []string{"10.0.0.5:20002"}, metrics.Targets)
	require.Equal(t, "node-1", metrics.Labels["__meta_nomad_node_id"])
	require.Equal(t, alloc.ID, metrics.Labels["__meta_nomad_alloc_id"])
	require.Equal(t, alloc.JobID, metrics.Labels["__meta_nomad_job_id"])
	require.Equal(t, alloc.Namespace, metrics.Labels["__meta_nomad_namespace"])
	require.Equal(t, alloc.TaskGroup, metrics.Labels["__meta_nomad_task_group"])
	require.Equal(t, "3", metrics.Labels["__meta_nomad_alloc_index"])
	require.Equal(t, "10.0.0.5", metrics.Labels["__meta_nomad_address"])
	require.Equal(t, "20002", metrics.Labels["__meta_nomad_port"])

	// Meta keys are sanitized for Prometheus label name rules; group meta
	// is exposed alongside job meta.
	require.Equal(t, "github|abc", metrics.Labels["__meta_nomad_meta_user_id"])
	require.Equal(t, "kling", metrics.Labels["__meta_nomad_meta_app_id"])

	httpGroup := byPort["http"]
	require.NotNil(t, httpGroup)
	require.Equal(t, []string{"10.0.0.5:20001"}, httpGroup.Targets)
}

func TestAllocPromSDTargetGroups_PortFilter(t *testing.T) {
	ci.Parallel(t)

	alloc := promSDTestAlloc()

	groups := allocPromSDTargetGroups(alloc, nil, "metrics")
	require.Len(t, groups, 1)
	require.Equal(t, []string{"10.0.0.5:20002"}, groups[0].Targets)

	groups = allocPromSDTargetGroups(alloc, nil, "nope")
	require.Empty(t, groups)
}

func TestAllocPromSDTargetGroups_LegacyTaskNetworks(t *testing.T) {
	ci.Parallel(t)

	alloc := promSDTestAlloc()
	alloc.AllocatedResources.Shared.Ports = nil
	alloc.AllocatedResources.Tasks = map[string]*structs.AllocatedTaskResources{
		"web": {
			Networks: []*structs.NetworkResource{
				{
					IP:            "192.168.0.100",
					DynamicPorts:  []structs.Port{{Label: "http", Value: 9876}},
					ReservedPorts: []structs.Port{{Label: "admin", Value: 5000}},
				},
			},
		},
	}

	groups := allocPromSDTargetGroups(alloc, nil, "")
	require.Len(t, groups, 2)

	byPort := map[string]*PromSDTargetGroup{}
	for _, g := range groups {
		byPort[g.Labels["__meta_nomad_port_label"]] = g
	}
	require.Equal(t, []string{"192.168.0.100:9876"}, byPort["http"].Targets)
	require.Equal(t, []string{"192.168.0.100:5000"}, byPort["admin"].Targets)
}

func TestAllocPromSDTargetGroups_SkipsIncomplete(t *testing.T) {
	ci.Parallel(t)

	// Ports without a bound host IP or value cannot be scraped.
	alloc := promSDTestAlloc()
	alloc.AllocatedResources.Shared.Ports = structs.AllocatedPorts{
		{Label: "metrics", Value: 0, HostIP: "10.0.0.5"},
		{Label: "http", Value: 20001, HostIP: ""},
	}
	require.Empty(t, allocPromSDTargetGroups(alloc, nil, ""))

	// Allocations missing job or resources are skipped entirely.
	alloc = promSDTestAlloc()
	alloc.Job = nil
	require.Empty(t, allocPromSDTargetGroups(alloc, nil, ""))

	alloc = promSDTestAlloc()
	alloc.AllocatedResources = nil
	require.Empty(t, allocPromSDTargetGroups(alloc, nil, ""))
}

func TestClientServiceDiscoveryRequest(t *testing.T) {
	ci.Parallel(t)
	httpTest(t, nil, func(s *TestAgent) {
		// Wrong method is rejected.
		req, err := http.NewRequest(http.MethodPost, "/v1/client/service_discovery", nil)
		require.NoError(t, err)
		respW := httptest.NewRecorder()
		_, err = s.Server.ClientServiceDiscoveryRequest(respW, req)
		require.Error(t, err)
		require.Contains(t, err.Error(), ErrInvalidMethod)

		// GET on a node with no allocations returns an empty list.
		req, err = http.NewRequest(http.MethodGet, "/v1/client/service_discovery", nil)
		require.NoError(t, err)
		respW = httptest.NewRecorder()
		obj, err := s.Server.ClientServiceDiscoveryRequest(respW, req)
		require.NoError(t, err)
		groups, ok := obj.([]*PromSDTargetGroup)
		require.True(t, ok)
		require.Empty(t, groups)
	})
}

func TestClientServiceDiscoveryRequest_NoClient(t *testing.T) {
	ci.Parallel(t)
	httpTest(t, nil, func(s *TestAgent) {
		c := s.client
		s.client = nil
		defer func() { s.client = c }()

		req, err := http.NewRequest(http.MethodGet, "/v1/client/service_discovery", nil)
		require.NoError(t, err)
		respW := httptest.NewRecorder()
		_, err = s.Server.ClientServiceDiscoveryRequest(respW, req)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not running a Nomad Client")
	})
}

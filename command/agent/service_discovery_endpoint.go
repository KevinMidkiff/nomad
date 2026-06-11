// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package agent

import (
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strconv"

	"github.com/hashicorp/nomad/nomad/structs"
)

const (
	// promSDMetaLabelPrefix prefixes every label emitted by the client
	// service discovery endpoint, following the convention used by
	// Prometheus' built-in service discovery mechanisms.
	promSDMetaLabelPrefix = "__meta_nomad_"
)

// promSDInvalidLabelChars matches characters that are not allowed in
// Prometheus label names ([a-zA-Z_][a-zA-Z0-9_]*).
var promSDInvalidLabelChars = regexp.MustCompile(`[^a-zA-Z0-9_]`)

// PromSDTargetGroup is a single target group in the Prometheus HTTP service
// discovery format: https://prometheus.io/docs/prometheus/latest/http_sd/
type PromSDTargetGroup struct {
	Targets []string          `json:"targets"`
	Labels  map[string]string `json:"labels"`
}

// ClientServiceDiscoveryRequest serves Prometheus HTTP SD target groups for
// the allocations running on the local client node. One target group is
// emitted per allocated port of every running allocation, so scrapers can
// select ports via the __meta_nomad_port_label label or the ?port= query
// parameter (e.g. ?port=metrics).
//
// This endpoint only serves local client state and is intended to be queried
// directly on each client agent, fanning scrape-target discovery out to the
// nodes instead of funneling it through the servers.
func (s *HTTPServer) ClientServiceDiscoveryRequest(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	if req.Method != http.MethodGet {
		return nil, CodedError(http.StatusMethodNotAllowed, ErrInvalidMethod)
	}

	client := s.agent.Client()
	if client == nil {
		return nil, clientNotRunning
	}

	// Listing every allocation on the node spans namespaces, so require
	// node:read like the other node-level client endpoints.
	aclObj, err := s.ResolveToken(req)
	if err != nil {
		return nil, err
	}
	if !aclObj.AllowNodeRead() {
		return nil, structs.ErrPermissionDenied
	}

	portFilter := req.URL.Query().Get("port")

	node := client.Node()
	nodeLabels := map[string]string{
		promSDMetaLabelPrefix + "node_id":         client.NodeID(),
		promSDMetaLabelPrefix + "node_name":       node.Name,
		promSDMetaLabelPrefix + "node_class":      node.NodeClass,
		promSDMetaLabelPrefix + "node_pool":       node.NodePool,
		promSDMetaLabelPrefix + "node_datacenter": node.Datacenter,
	}

	groups := make([]*PromSDTargetGroup, 0)
	for _, alloc := range client.Allocations() {
		if alloc.ClientStatus != structs.AllocClientStatusRunning {
			continue
		}
		groups = append(groups, allocPromSDTargetGroups(alloc, nodeLabels, portFilter)...)
	}

	// Sort for a deterministic response body.
	sort.Slice(groups, func(i, j int) bool {
		gi, gj := groups[i], groups[j]
		ai := gi.Labels[promSDMetaLabelPrefix+"alloc_id"]
		aj := gj.Labels[promSDMetaLabelPrefix+"alloc_id"]
		if ai != aj {
			return ai < aj
		}
		return gi.Labels[promSDMetaLabelPrefix+"port_label"] < gj.Labels[promSDMetaLabelPrefix+"port_label"]
	})

	return groups, nil
}

// allocPromSDTargetGroups builds one Prometheus SD target group per allocated
// port of the given allocation. When portFilter is non-empty only ports whose
// label matches are returned.
func allocPromSDTargetGroups(alloc *structs.Allocation, nodeLabels map[string]string, portFilter string) []*PromSDTargetGroup {
	if alloc.Job == nil || alloc.AllocatedResources == nil {
		return nil
	}

	baseLabels := map[string]string{
		promSDMetaLabelPrefix + "namespace":   alloc.Namespace,
		promSDMetaLabelPrefix + "job_id":      alloc.JobID,
		promSDMetaLabelPrefix + "job_name":    alloc.Job.Name,
		promSDMetaLabelPrefix + "task_group":  alloc.TaskGroup,
		promSDMetaLabelPrefix + "alloc_id":    alloc.ID,
		promSDMetaLabelPrefix + "alloc_name":  alloc.Name,
		promSDMetaLabelPrefix + "alloc_index": strconv.FormatUint(uint64(alloc.Index()), 10),
	}
	for k, v := range nodeLabels {
		baseLabels[k] = v
	}

	// Expose job and task group meta (group overrides job) so schedulers
	// embedding tenant information in meta can relabel on it.
	for k, v := range alloc.Job.Meta {
		baseLabels[promSDMetaLabelPrefix+"meta_"+promSDSafeLabelName(k)] = v
	}
	if tg := alloc.Job.LookupTaskGroup(alloc.TaskGroup); tg != nil {
		for k, v := range tg.Meta {
			baseLabels[promSDMetaLabelPrefix+"meta_"+promSDSafeLabelName(k)] = v
		}
	}

	var groups []*PromSDTargetGroup
	addPort := func(label, hostIP string, value int) {
		if portFilter != "" && label != portFilter {
			return
		}
		if hostIP == "" || value <= 0 {
			return
		}
		labels := make(map[string]string, len(baseLabels)+3)
		for k, v := range baseLabels {
			labels[k] = v
		}
		labels[promSDMetaLabelPrefix+"address"] = hostIP
		labels[promSDMetaLabelPrefix+"port_label"] = label
		labels[promSDMetaLabelPrefix+"port"] = strconv.Itoa(value)
		groups = append(groups, &PromSDTargetGroup{
			Targets: []string{fmt.Sprintf("%s:%d", hostIP, value)},
			Labels:  labels,
		})
	}

	if ports := alloc.AllocatedResources.Shared.Ports; len(ports) > 0 {
		// Modern group-level networking: ports live on Shared.Ports with
		// their bound host IP.
		for _, p := range ports {
			addPort(p.Label, p.HostIP, p.Value)
		}
		return groups
	}

	// Legacy task-level networking fallback.
	for _, task := range alloc.AllocatedResources.Tasks {
		for _, network := range task.Networks {
			for _, p := range network.DynamicPorts {
				addPort(p.Label, network.IP, p.Value)
			}
			for _, p := range network.ReservedPorts {
				addPort(p.Label, network.IP, p.Value)
			}
		}
	}
	return groups
}

// promSDSafeLabelName rewrites s so it is usable inside a Prometheus label
// name, replacing every invalid character with an underscore.
func promSDSafeLabelName(s string) string {
	return promSDInvalidLabelChars.ReplaceAllString(s, "_")
}

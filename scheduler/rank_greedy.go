// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package scheduler

import (
	"sort"

	"github.com/hashicorp/nomad/nomad/structs"
)

// Zero-cost greedy preemption helpers.
//
// These helpers implement the "zero-cost" half of greedy preemption: greedy
// allocs (Job.IsGreedy) are masked from BinPackIterator resource accounting
// (NetworkIndex, deviceAllocator, AllocsFit, consumedCores) so a non-greedy
// placement scores nodes as if greedy allocs weren't there. The masking step
// then derives PreemptedAllocs from the resources the new alloc actually
// claimed (specific device instance IDs, specific port values, specific
// reserved cores) and evicts only the greedy allocs holding them. Greedy
// allocs that don't conflict are left running.
//
// Feasibility iterators (host_volume, distinct_hosts, distinct_property)
// run upstream of BinPackIterator and intentionally still see greedy allocs:
// only resource accounting is masked.

// buildGreedyHeldDevices returns the set of device instance IDs held by the
// given greedy allocs, keyed by device tuple. Threaded into the device
// allocator under masking so it prefers truly-free instances over greedy-held
// ones; without this hint, masking strips greedy allocs from the accounter
// and a greedy-held GPU is indistinguishable from a truly-free GPU.
func buildGreedyHeldDevices(greedy []*structs.Allocation) map[structs.DeviceIdTuple]map[string]struct{} {
	held := make(map[structs.DeviceIdTuple]map[string]struct{})
	for _, g := range greedy {
		if g == nil || g.AllocatedResources == nil {
			continue
		}
		for _, tr := range g.AllocatedResources.Tasks {
			for _, dev := range tr.Devices {
				tuple := *dev.ID()
				set, ok := held[tuple]
				if !ok {
					set = make(map[string]struct{})
					held[tuple] = set
				}
				for _, id := range dev.DeviceIDs {
					set[id] = struct{}{}
				}
			}
		}
	}
	return held
}

// splitGreedy partitions allocs into (nonGreedy, greedy) by Job.IsGreedy.
func splitGreedy(allocs []*structs.Allocation) (nonGreedy, greedy []*structs.Allocation) {
	for _, a := range allocs {
		if a.IsGreedy() {
			greedy = append(greedy, a)
		} else {
			nonGreedy = append(nonGreedy, a)
		}
	}
	return
}

// claimedResources captures the specific node-level resources the new alloc
// is about to claim. Used by selectGreedyVictims to pick out exactly the
// greedy allocs whose resources need to be freed.
type claimedResources struct {
	// devices is keyed by the device tuple (Vendor/Type/Name) and lists the
	// specific instance IDs claimed within that group.
	devices map[structs.DeviceIdTuple]map[string]struct{}
	// ports is keyed by host IP and lists the port values claimed on that IP.
	// A claim with an empty HostIP is treated as matching ports without a
	// HostIP set on the holding alloc.
	ports map[string]map[int]struct{}
	// cores is the set of reserved logical cores claimed (CPU cores pinned
	// for exclusive use, separate from CPU shares).
	cores map[uint16]struct{}
}

// buildClaimedResources extracts the device instance IDs, ports, and
// reserved cores claimed across all tasks (per-task Networks + Devices +
// ReservedCores) plus shared resources (task-group level Ports/Networks).
func buildClaimedResources(total *structs.AllocatedResources) claimedResources {
	c := claimedResources{
		devices: make(map[structs.DeviceIdTuple]map[string]struct{}),
		ports:   make(map[string]map[int]struct{}),
		cores:   make(map[uint16]struct{}),
	}
	if total == nil {
		return c
	}

	for _, tr := range total.Tasks {
		for _, dev := range tr.Devices {
			tuple := *dev.ID()
			set, ok := c.devices[tuple]
			if !ok {
				set = make(map[string]struct{})
				c.devices[tuple] = set
			}
			for _, id := range dev.DeviceIDs {
				set[id] = struct{}{}
			}
		}
		for _, core := range tr.Cpu.ReservedCores {
			c.cores[core] = struct{}{}
		}
		for _, net := range tr.Networks {
			addPortsForIP(c.ports, net.IP, net.ReservedPorts, net.DynamicPorts)
		}
	}

	for _, p := range total.Shared.Ports {
		addPortValue(c.ports, p.HostIP, p.Value)
	}
	for _, net := range total.Shared.Networks {
		addPortsForIP(c.ports, net.IP, net.ReservedPorts, net.DynamicPorts)
	}

	return c
}

func addPortValue(m map[string]map[int]struct{}, ip string, port int) {
	set, ok := m[ip]
	if !ok {
		set = make(map[int]struct{})
		m[ip] = set
	}
	set[port] = struct{}{}
}

func addPortsForIP(m map[string]map[int]struct{}, ip string, reserved, dynamic []structs.Port) {
	for _, p := range reserved {
		addPortValue(m, ip, p.Value)
	}
	for _, p := range dynamic {
		addPortValue(m, ip, p.Value)
	}
}

// selectGreedyVictims returns the set of greedy allocs whose held resources
// overlap with the resources claimed by the new alloc. Greedy allocs with
// no overlap are not returned (they survive). The returned slice is
// deduplicated by alloc.ID.
//
// This handles device IDs, ports, and reserved cores by direct ID-style
// matching. Shared CPU/RAM/disk shortfall is not handled here — the caller
// runs Preemptor.PreemptForTaskGroup against a greedy-only candidate set
// for that case after a fit check on the kept-greedy set.
func selectGreedyVictims(greedy []*structs.Allocation, claimed claimedResources) []*structs.Allocation {
	if len(greedy) == 0 {
		return nil
	}
	if len(claimed.devices) == 0 && len(claimed.ports) == 0 && len(claimed.cores) == 0 {
		return nil
	}
	seen := make(map[string]struct{})
	var victims []*structs.Allocation
	for _, g := range greedy {
		if _, ok := seen[g.ID]; ok {
			continue
		}
		if greedyOverlapsClaim(g, claimed) {
			seen[g.ID] = struct{}{}
			victims = append(victims, g)
		}
	}
	return victims
}

func greedyOverlapsClaim(g *structs.Allocation, claimed claimedResources) bool {
	if g == nil || g.AllocatedResources == nil {
		return false
	}
	for _, tr := range g.AllocatedResources.Tasks {
		for _, dev := range tr.Devices {
			ids, ok := claimed.devices[*dev.ID()]
			if !ok {
				continue
			}
			for _, did := range dev.DeviceIDs {
				if _, hit := ids[did]; hit {
					return true
				}
			}
		}
		for _, core := range tr.Cpu.ReservedCores {
			if _, hit := claimed.cores[core]; hit {
				return true
			}
		}
		for _, net := range tr.Networks {
			if portsOverlapClaim(claimed.ports, net.IP, net.ReservedPorts, net.DynamicPorts) {
				return true
			}
		}
	}
	for _, p := range g.AllocatedResources.Shared.Ports {
		if ipSet, ok := claimed.ports[p.HostIP]; ok {
			if _, hit := ipSet[p.Value]; hit {
				return true
			}
		}
	}
	for _, net := range g.AllocatedResources.Shared.Networks {
		if portsOverlapClaim(claimed.ports, net.IP, net.ReservedPorts, net.DynamicPorts) {
			return true
		}
	}
	return false
}

func portsOverlapClaim(claimedPorts map[string]map[int]struct{}, ip string, reserved, dynamic []structs.Port) bool {
	ipSet, ok := claimedPorts[ip]
	if !ok {
		return false
	}
	for _, p := range reserved {
		if _, hit := ipSet[p.Value]; hit {
			return true
		}
	}
	for _, p := range dynamic {
		if _, hit := ipSet[p.Value]; hit {
			return true
		}
	}
	return false
}

// enforceNodeMaxAllocs ensures the post-placement alloc count on node
// honors node.NodeMaxAllocs by evicting additional greedy allocs when the
// resource-derived eviction step alone leaves the count over budget.
// AllocsFit's max-allocs check (nomad/structs/funcs.go:145) is a pure
// len(allocs) check that fires before resource accounting, so we must
// trim the count ourselves before the masked-set fit validation runs.
//
// Behavior:
//   - node.NodeMaxAllocs == 0 (unlimited): no-op.
//   - Picks additional victims from keptGreedy in lowest-Job.Priority
//     order; ties broken by CreateIndex ascending for determinism.
//   - Prefers non-terminal greedy as victims. Terminal greedy allocs
//     count toward AllocsFit's len() but evicting them is a wasted slot
//     (the planner already disposes of them); so we trim non-terminal
//     greedy first. If only terminal greedy remain, we'll fall back to
//     them.
//   - If even evicting every kept-greedy leaves the count over budget,
//     all of keptGreedy is returned as victims and the caller's
//     downstream AllocsFit reports "max allocation exceeded" — that's
//     genuine node exhaustion that greedy eviction cannot resolve.
func enforceNodeMaxAllocs(
	node *structs.Node,
	nonGreedy, keptGreedy []*structs.Allocation,
	incomingAllocs int,
) (kept []*structs.Allocation, extraVictims []*structs.Allocation) {
	if node == nil || node.NodeMaxAllocs == 0 {
		return keptGreedy, nil
	}
	total := len(nonGreedy) + len(keptGreedy) + incomingAllocs
	if total <= node.NodeMaxAllocs {
		return keptGreedy, nil
	}

	// Sort kept-greedy: non-terminal first, then lowest Job.Priority
	// first, then by CreateIndex ascending for determinism.
	sorted := make([]*structs.Allocation, len(keptGreedy))
	copy(sorted, keptGreedy)
	sort.SliceStable(sorted, func(i, j int) bool {
		ti := sorted[i].ClientTerminalStatus()
		tj := sorted[j].ClientTerminalStatus()
		if ti != tj {
			// Non-terminal (false) sorts before terminal (true) — pick
			// non-terminal as victims first.
			return !ti
		}
		pi := 0
		pj := 0
		if sorted[i].Job != nil {
			pi = sorted[i].Job.Priority
		}
		if sorted[j].Job != nil {
			pj = sorted[j].Job.Priority
		}
		if pi != pj {
			return pi < pj
		}
		return sorted[i].CreateIndex < sorted[j].CreateIndex
	})

	overBy := total - node.NodeMaxAllocs
	if overBy > len(sorted) {
		overBy = len(sorted)
	}
	extraVictims = sorted[:overBy]
	keptIDs := make(map[string]struct{}, overBy)
	for _, a := range extraVictims {
		keptIDs[a.ID] = struct{}{}
	}
	kept = make([]*structs.Allocation, 0, len(keptGreedy)-overBy)
	for _, a := range keptGreedy {
		if _, isVictim := keptIDs[a.ID]; isVictim {
			continue
		}
		kept = append(kept, a)
	}
	return kept, extraVictims
}

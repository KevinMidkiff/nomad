// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package scheduler

import (
	"testing"

	"github.com/hashicorp/nomad/ci"
	"github.com/hashicorp/nomad/helper/uuid"
	"github.com/hashicorp/nomad/nomad/structs"
	"github.com/shoenig/test/must"
)

// Direct unit tests for the helpers in rank_greedy.go. These exercise the
// derivation logic in isolation without spinning up a full scheduler harness.

func TestSplitGreedy(t *testing.T) {
	ci.Parallel(t)

	g1 := newGreedyTestAlloc(t, true, nil)
	g2 := newGreedyTestAlloc(t, true, nil)
	n1 := newGreedyTestAlloc(t, false, nil)
	n2 := newGreedyTestAlloc(t, false, nil)

	nonGreedy, greedy := splitGreedy([]*structs.Allocation{n1, g1, n2, g2})
	must.Len(t, 2, nonGreedy)
	must.Len(t, 2, greedy)
	for _, a := range nonGreedy {
		must.False(t, a.IsGreedy())
	}
	for _, a := range greedy {
		must.True(t, a.IsGreedy())
	}
}

func TestSplitGreedy_Empty(t *testing.T) {
	ci.Parallel(t)
	nonGreedy, greedy := splitGreedy(nil)
	must.Len(t, 0, nonGreedy)
	must.Len(t, 0, greedy)
}

func TestSplitGreedy_NilJob(t *testing.T) {
	ci.Parallel(t)
	// An alloc with no Job reference is treated as non-greedy (safe default).
	a := &structs.Allocation{ID: uuid.Generate(), AllocatedResources: &structs.AllocatedResources{}}
	nonGreedy, greedy := splitGreedy([]*structs.Allocation{a})
	must.Len(t, 1, nonGreedy)
	must.Len(t, 0, greedy)
}

func TestBuildClaimedResources_DevicesPortsCores(t *testing.T) {
	ci.Parallel(t)

	total := &structs.AllocatedResources{
		Tasks: map[string]*structs.AllocatedTaskResources{
			"web": {
				Cpu: structs.AllocatedCpuResources{
					ReservedCores: []uint16{2, 4, 6},
				},
				Devices: []*structs.AllocatedDeviceResource{
					{Vendor: "nvidia", Type: "gpu", Name: "h100", DeviceIDs: []string{"dev0", "dev1"}},
				},
				Networks: []*structs.NetworkResource{
					{
						IP:            "10.0.0.5",
						ReservedPorts: []structs.Port{{Label: "http", Value: 8080}},
						DynamicPorts:  []structs.Port{{Label: "metrics", Value: 9100}},
					},
				},
			},
		},
		Shared: structs.AllocatedSharedResources{
			Ports: structs.AllocatedPorts{
				{Label: "shared", Value: 7777, HostIP: "10.0.0.5"},
			},
		},
	}

	c := buildClaimedResources(total)

	gpuTuple := structs.DeviceIdTuple{Vendor: "nvidia", Type: "gpu", Name: "h100"}
	must.MapContainsKey(t, c.devices, gpuTuple)
	must.Eq(t, 2, len(c.devices[gpuTuple]))
	_, ok := c.devices[gpuTuple]["dev0"]
	must.True(t, ok)
	_, ok = c.devices[gpuTuple]["dev1"]
	must.True(t, ok)

	must.Eq(t, 3, len(c.cores))
	for _, want := range []uint16{2, 4, 6} {
		_, ok := c.cores[want]
		must.True(t, ok, must.Sprintf("core %d missing from claimed cores", want))
	}

	must.MapContainsKey(t, c.ports, "10.0.0.5")
	for _, want := range []int{8080, 9100, 7777} {
		_, ok := c.ports["10.0.0.5"][want]
		must.True(t, ok, must.Sprintf("port %d missing from claimed ports", want))
	}
}

func TestBuildClaimedResources_Nil(t *testing.T) {
	ci.Parallel(t)
	c := buildClaimedResources(nil)
	must.Eq(t, 0, len(c.devices))
	must.Eq(t, 0, len(c.ports))
	must.Eq(t, 0, len(c.cores))
}

func TestSelectGreedyVictims_NoClaim(t *testing.T) {
	ci.Parallel(t)

	g := newGreedyTestAlloc(t, true, &structs.AllocatedDeviceResource{
		Vendor: "nvidia", Type: "gpu", Name: "h100", DeviceIDs: []string{"dev0"},
	})

	c := claimedResources{
		devices: map[structs.DeviceIdTuple]map[string]struct{}{},
		ports:   map[string]map[int]struct{}{},
		cores:   map[uint16]struct{}{},
	}
	victims, reasons := selectGreedyVictims([]*structs.Allocation{g}, c)
	must.Len(t, 0, victims)
	must.Len(t, 0, reasons)
}

func TestSelectGreedyVictims_DeviceMatch(t *testing.T) {
	ci.Parallel(t)

	tuple := structs.DeviceIdTuple{Vendor: "nvidia", Type: "gpu", Name: "h100"}

	gMatch := newGreedyTestAlloc(t, true, &structs.AllocatedDeviceResource{
		Vendor: "nvidia", Type: "gpu", Name: "h100", DeviceIDs: []string{"dev0"},
	})
	gOther := newGreedyTestAlloc(t, true, &structs.AllocatedDeviceResource{
		Vendor: "nvidia", Type: "gpu", Name: "h100", DeviceIDs: []string{"dev1"},
	})
	gWrongTuple := newGreedyTestAlloc(t, true, &structs.AllocatedDeviceResource{
		Vendor: "amd", Type: "gpu", Name: "mi300", DeviceIDs: []string{"dev0"},
	})

	c := claimedResources{
		devices: map[structs.DeviceIdTuple]map[string]struct{}{
			tuple: {"dev0": struct{}{}},
		},
		ports: map[string]map[int]struct{}{},
		cores: map[uint16]struct{}{},
	}

	victims, reasons := selectGreedyVictims([]*structs.Allocation{gMatch, gOther, gWrongTuple}, c)
	must.Len(t, 1, victims)
	must.Eq(t, gMatch.ID, victims[0].ID)
	must.Eq(t, "device:nvidia/gpu/h100/dev0", reasons[0])
}

func TestSelectGreedyVictims_PortMatch(t *testing.T) {
	ci.Parallel(t)

	gMatch := newGreedyTestAllocWithPorts(t, true, "10.0.0.5", []int{8080})
	gOther := newGreedyTestAllocWithPorts(t, true, "10.0.0.5", []int{9090})

	c := claimedResources{
		devices: map[structs.DeviceIdTuple]map[string]struct{}{},
		ports: map[string]map[int]struct{}{
			"10.0.0.5": {8080: struct{}{}},
		},
		cores: map[uint16]struct{}{},
	}
	victims, reasons := selectGreedyVictims([]*structs.Allocation{gMatch, gOther}, c)
	must.Len(t, 1, victims)
	must.Eq(t, gMatch.ID, victims[0].ID)
	must.Eq(t, "port:10.0.0.5:8080", reasons[0])
}

func TestSelectGreedyVictims_PortMatch_DifferentIP(t *testing.T) {
	ci.Parallel(t)

	// Greedy holds port 8080 on 10.0.0.5; claim is on a different IP.
	g := newGreedyTestAllocWithPorts(t, true, "10.0.0.5", []int{8080})
	c := claimedResources{
		devices: map[structs.DeviceIdTuple]map[string]struct{}{},
		ports: map[string]map[int]struct{}{
			"10.0.0.6": {8080: struct{}{}},
		},
		cores: map[uint16]struct{}{},
	}
	victims, _ := selectGreedyVictims([]*structs.Allocation{g}, c)
	must.Len(t, 0, victims, must.Sprintf("ports on different IPs must not collide"))
}

func TestSelectGreedyVictims_CoreMatch(t *testing.T) {
	ci.Parallel(t)

	gMatch := newGreedyTestAlloc(t, true, nil)
	gMatch.AllocatedResources.Tasks["web"].Cpu.ReservedCores = []uint16{4, 5}
	gOther := newGreedyTestAlloc(t, true, nil)
	gOther.AllocatedResources.Tasks["web"].Cpu.ReservedCores = []uint16{6, 7}

	c := claimedResources{
		devices: map[structs.DeviceIdTuple]map[string]struct{}{},
		ports:   map[string]map[int]struct{}{},
		cores:   map[uint16]struct{}{5: struct{}{}},
	}
	victims, reasons := selectGreedyVictims([]*structs.Allocation{gMatch, gOther}, c)
	must.Len(t, 1, victims)
	must.Eq(t, gMatch.ID, victims[0].ID)
	must.Eq(t, "core:5", reasons[0])
}

func TestSelectGreedyVictims_Dedup_DeviceAndPort(t *testing.T) {
	ci.Parallel(t)

	tuple := structs.DeviceIdTuple{Vendor: "nvidia", Type: "gpu", Name: "h100"}

	// A single greedy alloc holds both a claimed device AND a claimed port.
	g := newGreedyTestAlloc(t, true, &structs.AllocatedDeviceResource{
		Vendor: "nvidia", Type: "gpu", Name: "h100", DeviceIDs: []string{"dev0"},
	})
	g.AllocatedResources.Tasks["web"].Networks = []*structs.NetworkResource{
		{IP: "10.0.0.5", ReservedPorts: []structs.Port{{Value: 8080}}},
	}

	c := claimedResources{
		devices: map[structs.DeviceIdTuple]map[string]struct{}{
			tuple: {"dev0": struct{}{}},
		},
		ports: map[string]map[int]struct{}{
			"10.0.0.5": {8080: struct{}{}},
		},
		cores: map[uint16]struct{}{},
	}
	victims, _ := selectGreedyVictims([]*structs.Allocation{g}, c)
	must.Len(t, 1, victims, must.Sprintf("a greedy alloc with multiple claim overlaps must appear once"))
}

func TestSelectGreedyVictims_Empty(t *testing.T) {
	ci.Parallel(t)
	// Empty greedy list.
	victims, _ := selectGreedyVictims(nil, claimedResources{})
	must.Len(t, 0, victims)
	// Empty claimed.
	g := newGreedyTestAlloc(t, true, &structs.AllocatedDeviceResource{
		Vendor: "nvidia", Type: "gpu", Name: "h100", DeviceIDs: []string{"dev0"},
	})
	victims, _ = selectGreedyVictims([]*structs.Allocation{g}, claimedResources{})
	must.Len(t, 0, victims)
}

// newGreedyTestAlloc builds a minimal *structs.Allocation suitable for the
// helper-level unit tests. greedy controls the meta.greedy tag on the Job.
// If dev is non-nil, it is set as the single allocated device on the "web"
// task.
func newGreedyTestAlloc(t *testing.T, greedy bool, dev *structs.AllocatedDeviceResource) *structs.Allocation {
	t.Helper()
	j := &structs.Job{
		ID:        uuid.Generate(),
		Namespace: structs.DefaultNamespace,
		Priority:  50,
	}
	if greedy {
		j.Meta = map[string]string{structs.JobMetaGreedy: "true"}
	}
	a := &structs.Allocation{
		ID:        uuid.Generate(),
		Job:       j,
		JobID:     j.ID,
		Namespace: j.Namespace,
		TaskGroup: "web",
		AllocatedResources: &structs.AllocatedResources{
			Tasks: map[string]*structs.AllocatedTaskResources{
				"web": {},
			},
		},
		DesiredStatus: structs.AllocDesiredStatusRun,
		ClientStatus:  structs.AllocClientStatusRunning,
	}
	if dev != nil {
		a.AllocatedResources.Tasks["web"].Devices = []*structs.AllocatedDeviceResource{dev}
	}
	return a
}

func newGreedyTestAllocWithPorts(t *testing.T, greedy bool, ip string, ports []int) *structs.Allocation {
	t.Helper()
	a := newGreedyTestAlloc(t, greedy, nil)
	var rp []structs.Port
	for _, p := range ports {
		rp = append(rp, structs.Port{Value: p})
	}
	a.AllocatedResources.Tasks["web"].Networks = []*structs.NetworkResource{
		{IP: ip, ReservedPorts: rp},
	}
	return a
}

// Tests for enforceNodeMaxAllocs.

func TestEnforceNodeMaxAllocs_NoLimit(t *testing.T) {
	ci.Parallel(t)
	node := &structs.Node{NodeMaxAllocs: 0}
	g1 := newGreedyTestAlloc(t, true, nil)
	g2 := newGreedyTestAlloc(t, true, nil)
	kept, extra := enforceNodeMaxAllocs(node, nil, []*structs.Allocation{g1, g2}, 1)
	must.Len(t, 2, kept)
	must.Len(t, 0, extra)
}

func TestEnforceNodeMaxAllocs_BudgetExactlyMet(t *testing.T) {
	ci.Parallel(t)
	// NodeMaxAllocs=4; nonGreedy=2, keptGreedy=1, incoming=1 → total 4 ≤ 4.
	node := &structs.Node{NodeMaxAllocs: 4}
	n1 := newGreedyTestAlloc(t, false, nil)
	n2 := newGreedyTestAlloc(t, false, nil)
	g := newGreedyTestAlloc(t, true, nil)
	kept, extra := enforceNodeMaxAllocs(node, []*structs.Allocation{n1, n2}, []*structs.Allocation{g}, 1)
	must.Len(t, 1, kept)
	must.Len(t, 0, extra)
}

func TestEnforceNodeMaxAllocs_PicksLowestPriority(t *testing.T) {
	ci.Parallel(t)
	// NodeMaxAllocs=2; nonGreedy=1, keptGreedy=3, incoming=1 → total 5,
	// over by 3. Helper should evict the 3 lowest-priority greedy allocs.
	// We have priorities 20, 40, 60 — all three get evicted.
	node := &structs.Node{NodeMaxAllocs: 2}
	n := newGreedyTestAlloc(t, false, nil)
	g20 := newGreedyTestAlloc(t, true, nil)
	g20.Job.Priority = 20
	g40 := newGreedyTestAlloc(t, true, nil)
	g40.Job.Priority = 40
	g60 := newGreedyTestAlloc(t, true, nil)
	g60.Job.Priority = 60
	kept, extra := enforceNodeMaxAllocs(node, []*structs.Allocation{n}, []*structs.Allocation{g60, g40, g20}, 1)
	must.Len(t, 0, kept)
	must.Len(t, 3, extra)

	// Now NodeMaxAllocs=4 — over by 1 — only the lowest-priority gets evicted.
	node = &structs.Node{NodeMaxAllocs: 4}
	kept, extra = enforceNodeMaxAllocs(node, []*structs.Allocation{n}, []*structs.Allocation{g60, g40, g20}, 1)
	must.Len(t, 2, kept)
	must.Len(t, 1, extra)
	must.Eq(t, g20.ID, extra[0].ID, must.Sprintf("lowest-priority greedy should be evicted first"))
}

func TestEnforceNodeMaxAllocs_TieByCreateIndex(t *testing.T) {
	ci.Parallel(t)
	// Two greedy allocs at equal priority; helper must evict the one with
	// the lower CreateIndex (the older one), deterministically.
	node := &structs.Node{NodeMaxAllocs: 1}
	n := newGreedyTestAlloc(t, false, nil)
	g1 := newGreedyTestAlloc(t, true, nil)
	g1.Job.Priority = 50
	g1.CreateIndex = 100
	g2 := newGreedyTestAlloc(t, true, nil)
	g2.Job.Priority = 50
	g2.CreateIndex = 200

	// Total = 1 non-greedy + 2 greedy + 1 incoming = 4; over by 3 with
	// NodeMaxAllocs=1. Helper caps eviction count at len(keptGreedy)=2 —
	// both greedy go, leaving the caller to surface "max allocation
	// exceeded" downstream.
	_, extra := enforceNodeMaxAllocs(node, []*structs.Allocation{n}, []*structs.Allocation{g2, g1}, 1)
	must.Len(t, 2, extra)
	// First eviction picked must be the lower CreateIndex.
	must.Eq(t, g1.ID, extra[0].ID, must.Sprintf("tie on priority should break by lower CreateIndex first"))

	// Now NodeMaxAllocs=3 — over by 1 — only g1 should be evicted.
	node = &structs.Node{NodeMaxAllocs: 3}
	kept, extra := enforceNodeMaxAllocs(node, []*structs.Allocation{n}, []*structs.Allocation{g2, g1}, 1)
	must.Len(t, 1, kept)
	must.Eq(t, g2.ID, kept[0].ID, must.Sprintf("higher-CreateIndex greedy should survive"))
	must.Len(t, 1, extra)
	must.Eq(t, g1.ID, extra[0].ID)
}

func TestEnforceNodeMaxAllocs_PrefersNonTerminal(t *testing.T) {
	ci.Parallel(t)
	// Two equal-priority greedy allocs, one terminal, one running. Helper
	// must pick the non-terminal one as the victim (evicting a terminal
	// is a wasted slot).
	node := &structs.Node{NodeMaxAllocs: 2}
	n := newGreedyTestAlloc(t, false, nil)
	gRunning := newGreedyTestAlloc(t, true, nil)
	gRunning.Job.Priority = 50
	gTerminal := newGreedyTestAlloc(t, true, nil)
	gTerminal.Job.Priority = 50
	gTerminal.ClientStatus = structs.AllocClientStatusComplete
	gTerminal.DesiredStatus = structs.AllocDesiredStatusStop

	// Total = 1 + 2 + 1 = 4; over by 2 with NodeMaxAllocs=2. Both get
	// evicted, but ordering matters when over by 1: confirm non-terminal
	// is picked first by changing NodeMaxAllocs.
	node = &structs.Node{NodeMaxAllocs: 3}
	_, extra := enforceNodeMaxAllocs(node, []*structs.Allocation{n}, []*structs.Allocation{gTerminal, gRunning}, 1)
	must.Len(t, 1, extra)
	must.Eq(t, gRunning.ID, extra[0].ID,
		must.Sprintf("non-terminal greedy should be preferred as victim over terminal"))
}

func TestEnforceNodeMaxAllocs_OverBudgetEvenAfterAllGreedy(t *testing.T) {
	ci.Parallel(t)
	// nonGreedy alone already exceeds NodeMaxAllocs. Helper evicts every
	// kept-greedy but the caller's downstream AllocsFit will still report
	// "max allocation exceeded" — that's genuine node exhaustion.
	node := &structs.Node{NodeMaxAllocs: 1}
	n1 := newGreedyTestAlloc(t, false, nil)
	n2 := newGreedyTestAlloc(t, false, nil)
	g := newGreedyTestAlloc(t, true, nil)
	kept, extra := enforceNodeMaxAllocs(node, []*structs.Allocation{n1, n2}, []*structs.Allocation{g}, 1)
	must.Len(t, 0, kept, must.Sprintf("all kept-greedy evicted when budget cannot be met"))
	must.Len(t, 1, extra)
}

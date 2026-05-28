// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

//go:build !ent

package scheduler

import (
	"cmp"
	"math/rand"
	"slices"

	"github.com/hashicorp/nomad/client/lib/idset"
	"github.com/hashicorp/nomad/client/lib/numalib"
	"github.com/hashicorp/nomad/client/lib/numalib/hw"
	"github.com/hashicorp/nomad/nomad/structs"
)

type coreSelector struct {
	topology         *numalib.Topology
	availableCores   *idset.Set[hw.CoreID]
	shuffle          func([]numalib.Core)
	deviceMemoryNode int

	// greedyHeld, when non-nil, marks cores held by surviving greedy allocs
	// on this node. Under zero-cost greedy masking those allocs are stripped
	// from proposed accounting, so their reserved cores appear in
	// availableCores and would otherwise be picked simply because they sort
	// first. Select uses this to prefer truly-free cores (availableCores
	// minus greedyHeld) and fall back to greedy-held cores only when the ask
	// exceeds the truly-free supply. Nil means "not under masking" — all
	// availableCores are truly free.
	greedyHeld *idset.Set[hw.CoreID]
}

// Select returns a set of CoreIDs that satisfy the requested core reservations,
// as well as the amount of CPU bandwidth represented by those specific cores.
//
// NUMA preference is available in ent only.
func (cs *coreSelector) Select(ask *structs.Resources) ([]uint16, hw.MHz) {
	// Two-pass assignment under greedy masking: prefer truly-free cores
	// first; fall back to greedy-held only if pass 1 doesn't satisfy the
	// ask. With nil/empty greedyHeld (no masking), pass 1 picks from the
	// full availableCores — identical to pre-fix behavior.
	want := ask.Cores
	var picked []hw.CoreID
	if cs.greedyHeld != nil && cs.greedyHeld.Size() > 0 {
		trulyFree := cs.availableCores.Difference(cs.greedyHeld).Slice()
		if len(trulyFree) >= want {
			picked = trulyFree[:want]
		} else {
			picked = trulyFree
			held := cs.availableCores.Intersect(cs.greedyHeld).Slice()
			need := want - len(picked)
			if len(held) > need {
				held = held[:need]
			}
			picked = append(picked, held...)
		}
	} else {
		picked = cs.availableCores.Slice()[:want]
	}

	mhz := hw.MHz(0)
	ids := make([]uint16, 0, ask.Cores)
	sortedTopologyCores := make([]numalib.Core, len(cs.topology.Cores))
	copy(sortedTopologyCores, cs.topology.Cores)
	slices.SortFunc(sortedTopologyCores, func(a, b numalib.Core) int { return cmp.Compare(a.ID, b.ID) })
	for _, core := range picked {
		if i, found := slices.BinarySearchFunc(sortedTopologyCores, core, func(c numalib.Core, id hw.CoreID) int { return cmp.Compare(c.ID, id) }); found {
			mhz += cs.topology.Cores[i].MHz()
			ids = append(ids, uint16(cs.topology.Cores[i].ID))
		}
	}
	return ids, mhz
}

// randomize the cores so we can at least try to mitigate PFNR problems
func randomizeCores(cores []numalib.Core) {
	rand.Shuffle(len(cores), func(x, y int) {
		cores[x], cores[y] = cores[y], cores[x]
	})
}

// candidateMemoryNodes return -1 on CE, indicating any memory node is acceptable
//
// (NUMA aware scheduling is an enterprise feature)
func (cs *coreSelector) candidateMemoryNodes(ask *structs.Resources) []int {
	return []int{-1}
}

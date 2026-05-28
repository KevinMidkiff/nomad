// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package scheduler

import (
	"testing"

	"github.com/hashicorp/nomad/ci"
	"github.com/hashicorp/nomad/client/lib/idset"
	"github.com/hashicorp/nomad/client/lib/numalib"
	"github.com/hashicorp/nomad/client/lib/numalib/hw"
	"github.com/hashicorp/nomad/nomad/structs"
	"github.com/shoenig/test/must"
)

func TestCoreSelectorSelect(t *testing.T) {
	var (
		totalCores = 46
		maxSpeed   = 100
		coreIds    = make([]uint16, totalCores)
		cores      = make([]numalib.Core, totalCores)
	)
	for i := 1; i < 24; i++ {
		coreIds[i-1] = uint16(i)
		cores[i-1] = numalib.Core{
			SocketID:   0,
			NodeID:     0,
			ID:         hw.CoreID(i),
			Grade:      false,
			Disable:    false,
			BaseSpeed:  0,
			MaxSpeed:   hw.MHz(maxSpeed),
			GuessSpeed: 0,
		}
	}
	for i := 25; i < 48; i++ {
		coreIds[i-2] = uint16(i)
		cores[i-2] = numalib.Core{
			SocketID:   0,
			NodeID:     0,
			ID:         hw.CoreID(i),
			Grade:      false,
			Disable:    false,
			BaseSpeed:  0,
			MaxSpeed:   hw.MHz(maxSpeed),
			GuessSpeed: 0,
		}
	}
	must.Eq(t, []uint16{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47}, coreIds)

	selector := &coreSelector{
		topology: &numalib.Topology{
			Cores: cores,
		},
		availableCores: idset.From[hw.CoreID](coreIds),
	}

	for _, test := range []struct {
		name        string
		resources   *structs.Resources
		expectedIds []uint16
		expectedMhz hw.MHz
	}{
		{
			name: "request all cores",
			resources: &structs.Resources{
				Cores: totalCores,
			},
			expectedIds: coreIds,
			expectedMhz: hw.MHz(totalCores * maxSpeed),
		},
		{
			name: "request half the cores",
			resources: &structs.Resources{
				Cores: 10,
			},
			expectedIds: coreIds[:10],
			expectedMhz: hw.MHz(10 * maxSpeed),
		},
		{
			name: "request one core",
			resources: &structs.Resources{
				Cores: 1,
			},
			expectedIds: coreIds[:1],
			expectedMhz: hw.MHz(1 * maxSpeed),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ids, mhz := selector.Select(test.resources)
			must.Eq(t, test.expectedIds, ids)
			must.Eq(t, test.expectedMhz, mhz)
		})
	}
}

// TestCoreSelectorSelect_GreedyHeld covers the two-pass behavior used under
// zero-cost greedy masking: truly-free cores (availableCores minus greedyHeld)
// are picked first, and greedy-held cores are only used to top up when the
// ask exceeds the truly-free supply. With nil greedyHeld the picker behaves
// exactly as in the non-masking case.
func TestCoreSelectorSelect_GreedyHeld(t *testing.T) {
	ci.Parallel(t)

	const maxSpeed = 100
	coreIds := []uint16{0, 1, 2, 3, 4, 5, 6, 7}
	cores := make([]numalib.Core, len(coreIds))
	for i, id := range coreIds {
		cores[i] = numalib.Core{
			ID:       hw.CoreID(id),
			MaxSpeed: hw.MHz(maxSpeed),
		}
	}
	topology := &numalib.Topology{Cores: cores}

	t.Run("prefers truly-free over greedy-held", func(t *testing.T) {
		// Greedy holds the lowest-numbered cores (0..3). Without the
		// two-pass, Select would pick them because availableCores.Slice()
		// is sorted and the first 4 cores are exactly the greedy-held
		// ones. The fix should instead pick 4..7.
		selector := &coreSelector{
			topology:       topology,
			availableCores: idset.From[hw.CoreID](coreIds),
			greedyHeld:     idset.From[hw.CoreID]([]uint16{0, 1, 2, 3}),
		}
		ids, mhz := selector.Select(&structs.Resources{Cores: 4})
		must.Eq(t, []uint16{4, 5, 6, 7}, ids)
		must.Eq(t, hw.MHz(4*maxSpeed), mhz)
	})

	t.Run("falls back to greedy-held when ask exceeds truly-free", func(t *testing.T) {
		// Greedy holds 4..7; truly-free is 0..3 (4 cores). Asking for 6
		// cores must consume all four truly-free and top up with two
		// greedy-held.
		selector := &coreSelector{
			topology:       topology,
			availableCores: idset.From[hw.CoreID](coreIds),
			greedyHeld:     idset.From[hw.CoreID]([]uint16{4, 5, 6, 7}),
		}
		ids, _ := selector.Select(&structs.Resources{Cores: 6})
		must.Len(t, 6, ids)
		got := make(map[uint16]struct{}, len(ids))
		for _, id := range ids {
			got[id] = struct{}{}
		}
		// All four truly-free must be present.
		for _, id := range []uint16{0, 1, 2, 3} {
			_, ok := got[id]
			must.True(t, ok, must.Sprintf("truly-free core %d must be picked first", id))
		}
		// Exactly two of the greedy-held cores must round it out.
		heldPicked := 0
		for _, id := range []uint16{4, 5, 6, 7} {
			if _, ok := got[id]; ok {
				heldPicked++
			}
		}
		must.Eq(t, 2, heldPicked)
	})

	t.Run("nil greedyHeld preserves pre-fix behavior", func(t *testing.T) {
		selector := &coreSelector{
			topology:       topology,
			availableCores: idset.From[hw.CoreID](coreIds),
		}
		ids, mhz := selector.Select(&structs.Resources{Cores: 3})
		must.Eq(t, []uint16{0, 1, 2}, ids)
		must.Eq(t, hw.MHz(3*maxSpeed), mhz)
	})

	t.Run("empty greedyHeld preserves pre-fix behavior", func(t *testing.T) {
		selector := &coreSelector{
			topology:       topology,
			availableCores: idset.From[hw.CoreID](coreIds),
			greedyHeld:     idset.Empty[hw.CoreID](),
		}
		ids, mhz := selector.Select(&structs.Resources{Cores: 3})
		must.Eq(t, []uint16{0, 1, 2}, ids)
		must.Eq(t, hw.MHz(3*maxSpeed), mhz)
	})
}

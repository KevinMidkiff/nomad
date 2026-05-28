// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package structs

import (
	"math"
	"testing"

	"github.com/hashicorp/nomad/ci"
	"github.com/hashicorp/nomad/helper/pointer"
	"github.com/shoenig/test/must"
)

func TestSchedulerConfiguration_WithNodePool(t *testing.T) {
	ci.Parallel(t)

	testCases := []struct {
		name        string
		schedConfig *SchedulerConfiguration
		pool        *NodePool
		expected    *SchedulerConfiguration
	}{
		{
			name: "nil pool returns same config",
			schedConfig: &SchedulerConfiguration{
				MemoryOversubscriptionEnabled: false,
				SchedulerAlgorithm:            SchedulerAlgorithmSpread,
			},
			pool: nil,
			expected: &SchedulerConfiguration{
				MemoryOversubscriptionEnabled: false,
				SchedulerAlgorithm:            SchedulerAlgorithmSpread,
			},
		},
		{
			name: "nil pool scheduler config returns same config",
			schedConfig: &SchedulerConfiguration{
				MemoryOversubscriptionEnabled: false,
				SchedulerAlgorithm:            SchedulerAlgorithmSpread,
			},
			pool: &NodePool{},
			expected: &SchedulerConfiguration{
				MemoryOversubscriptionEnabled: false,
				SchedulerAlgorithm:            SchedulerAlgorithmSpread,
			},
		},
		{
			name: "pool with memory oversubscription overwrites config",
			schedConfig: &SchedulerConfiguration{
				MemoryOversubscriptionEnabled: false,
			},
			pool: &NodePool{
				SchedulerConfiguration: &NodePoolSchedulerConfiguration{
					MemoryOversubscriptionEnabled: pointer.Of(true),
				},
			},
			expected: &SchedulerConfiguration{
				MemoryOversubscriptionEnabled: true,
			},
		},
		{
			name: "pool with scheduler algorithm overwrites config",
			schedConfig: &SchedulerConfiguration{
				SchedulerAlgorithm: SchedulerAlgorithmBinpack,
			},
			pool: &NodePool{
				SchedulerConfiguration: &NodePoolSchedulerConfiguration{
					SchedulerAlgorithm: SchedulerAlgorithmSpread,
				},
			},
			expected: &SchedulerConfiguration{
				SchedulerAlgorithm: SchedulerAlgorithmSpread,
			},
		},
		{
			name: "pool without memory oversubscription does not modify config",
			schedConfig: &SchedulerConfiguration{
				MemoryOversubscriptionEnabled: false,
			},
			pool: &NodePool{
				SchedulerConfiguration: &NodePoolSchedulerConfiguration{},
			},
			expected: &SchedulerConfiguration{
				MemoryOversubscriptionEnabled: false,
			},
		},
		{
			name: "pool without scheduler algorithm does not modify config",
			schedConfig: &SchedulerConfiguration{
				SchedulerAlgorithm: SchedulerAlgorithmSpread,
			},
			pool: &NodePool{
				SchedulerConfiguration: &NodePoolSchedulerConfiguration{},
			},
			expected: &SchedulerConfiguration{
				SchedulerAlgorithm: SchedulerAlgorithmSpread,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.schedConfig.WithNodePool(tc.pool)
			must.Eq(t, tc.expected, got)
			must.NotEqOp(t, tc.schedConfig, got)
		})
	}
}

func TestSchedulerConfiguration_ScoreWeights(t *testing.T) {
	ci.Parallel(t)

	var nilConfig *SchedulerConfiguration
	must.Eq(t, DefaultBinpackScoreWeight, nilConfig.EffectiveBinpackScoreWeight())
	must.Eq(t, DefaultDeviceAffinityScoreWeight, nilConfig.EffectiveDeviceAffinityScoreWeight())

	zero := 0.0
	half := 0.5
	config := &SchedulerConfiguration{
		BinpackScoreWeight:        pointer.Of(zero),
		DeviceAffinityScoreWeight: pointer.Of(half),
	}
	must.Eq(t, zero, config.EffectiveBinpackScoreWeight())
	must.Eq(t, half, config.EffectiveDeviceAffinityScoreWeight())

	copied := config.Copy()
	must.Eq(t, config, copied)
	must.NotEqOp(t, config.BinpackScoreWeight, copied.BinpackScoreWeight)
	must.NotEqOp(t, config.DeviceAffinityScoreWeight, copied.DeviceAffinityScoreWeight)

	defaulted := &SchedulerConfiguration{}
	defaulted.Canonicalize()
	must.Eq(t, DefaultBinpackScoreWeight, defaulted.EffectiveBinpackScoreWeight())
	must.Eq(t, DefaultDeviceAffinityScoreWeight, defaulted.EffectiveDeviceAffinityScoreWeight())
	must.NotNil(t, defaulted.BinpackScoreWeight)
	must.NotNil(t, defaulted.DeviceAffinityScoreWeight)

	must.NoError(t, (&SchedulerConfiguration{
		BinpackScoreWeight:        pointer.Of(0.0),
		DeviceAffinityScoreWeight: pointer.Of(2.0),
	}).Validate())
	must.ErrorContains(t, (&SchedulerConfiguration{
		BinpackScoreWeight: pointer.Of(-0.1),
	}).Validate(), "binpack_score_weight")
	must.ErrorContains(t, (&SchedulerConfiguration{
		DeviceAffinityScoreWeight: pointer.Of(math.Inf(1)),
	}).Validate(), "device_affinity_score_weight")
	must.ErrorContains(t, (&SchedulerConfiguration{
		DeviceAffinityScoreWeight: pointer.Of(math.NaN()),
	}).Validate(), "device_affinity_score_weight")
}

func TestSchedulerConfiguration_Validate_GPUResourceReservation(t *testing.T) {
	ci.Parallel(t)

	testCases := []struct {
		name   string
		config *SchedulerConfiguration
	}{
		{
			name: "negative device cpu",
			config: &SchedulerConfiguration{
				GPUResourceReservation: SchedulerGPUResourceReservation{
					DeviceReservations: []*SchedulerGPUResourceReservationDevice{
						{
							Vendor:   "nvidia",
							Name:     "a100",
							CPUCores: -1,
						},
					},
				},
			},
		},
		{
			name: "negative device memory",
			config: &SchedulerConfiguration{
				GPUResourceReservation: SchedulerGPUResourceReservation{
					DeviceReservations: []*SchedulerGPUResourceReservationDevice{
						{
							Vendor:   "nvidia",
							Name:     "a100",
							MemoryMB: -1,
						},
					},
				},
			},
		},
		{
			name: "non gpu device type",
			config: &SchedulerConfiguration{
				GPUResourceReservation: SchedulerGPUResourceReservation{
					DeviceReservations: []*SchedulerGPUResourceReservationDevice{
						{
							Type:     "fpga",
							CPUCores: 1,
						},
					},
				},
			},
		},
		{
			name: "duplicate device",
			config: &SchedulerConfiguration{
				GPUResourceReservation: SchedulerGPUResourceReservation{
					DeviceReservations: []*SchedulerGPUResourceReservationDevice{
						{
							Vendor:   "nvidia",
							Type:     "gpu",
							Name:     "a100",
							CPUCores: 1,
						},
						{
							Vendor:   "nvidia",
							Type:     "gpu",
							Name:     "a100",
							MemoryMB: 1024,
						},
					},
				},
			},
		},
		{
			name: "ambiguous equal specificity overlapping devices",
			config: &SchedulerConfiguration{
				GPUResourceReservation: SchedulerGPUResourceReservation{
					DeviceReservations: []*SchedulerGPUResourceReservationDevice{
						{
							Vendor:   "nvidia",
							Type:     "gpu",
							CPUCores: 1,
						},
						{
							Type:     "gpu",
							Name:     "a100",
							MemoryMB: 1024,
						},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			must.Error(t, tc.config.Validate())
		})
	}

	valid := &SchedulerConfiguration{
		GPUResourceReservation: SchedulerGPUResourceReservation{
			DeviceReservations: []*SchedulerGPUResourceReservationDevice{
				{
					Type:     "gpu",
					CPUCores: 1,
					MemoryMB: 16384,
				},
				{
					Vendor:   "nvidia",
					Type:     "gpu",
					Name:     "a100",
					CPUCores: 2,
					MemoryMB: 32768,
				},
			},
		},
	}
	must.NoError(t, valid.Validate())
}

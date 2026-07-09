// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package scheduler

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/hashicorp/nomad/ci"
	"github.com/hashicorp/nomad/helper/uuid"
	"github.com/hashicorp/nomad/nomad/mock"
	"github.com/hashicorp/nomad/nomad/structs"
	psstructs "github.com/hashicorp/nomad/plugins/shared/structs"
	"github.com/shoenig/test/must"
)

func TestResourceDistance(t *testing.T) {
	ci.Parallel(t)

	resourceAsk := &structs.ComparableResources{
		Flattened: structs.AllocatedTaskResources{
			Cpu: structs.AllocatedCpuResources{
				CpuShares: 2048,
			},
			Memory: structs.AllocatedMemoryResources{
				MemoryMB: 512,
			},
			Networks: []*structs.NetworkResource{
				{
					Device: "eth0",
					MBits:  1024,
				},
			},
		},
		Shared: structs.AllocatedSharedResources{
			DiskMB: 4096,
		},
	}

	type testCase struct {
		allocResource    *structs.ComparableResources
		expectedDistance string
	}

	testCases := []*testCase{
		{
			&structs.ComparableResources{
				Flattened: structs.AllocatedTaskResources{
					Cpu: structs.AllocatedCpuResources{
						CpuShares: 2048,
					},
					Memory: structs.AllocatedMemoryResources{
						MemoryMB: 512,
					},
					Networks: []*structs.NetworkResource{
						{
							Device: "eth0",
							MBits:  1024,
						},
					},
				},
				Shared: structs.AllocatedSharedResources{
					DiskMB: 4096,
				},
			},
			"0.000",
		},
		{
			&structs.ComparableResources{
				Flattened: structs.AllocatedTaskResources{
					Cpu: structs.AllocatedCpuResources{
						CpuShares: 1024,
					},
					Memory: structs.AllocatedMemoryResources{
						MemoryMB: 400,
					},
					Networks: []*structs.NetworkResource{
						{
							Device: "eth0",
							MBits:  1024,
						},
					},
				},
				Shared: structs.AllocatedSharedResources{
					DiskMB: 1024,
				},
			},
			"0.928",
		},
		{
			&structs.ComparableResources{
				Flattened: structs.AllocatedTaskResources{
					Cpu: structs.AllocatedCpuResources{
						CpuShares: 8192,
					},
					Memory: structs.AllocatedMemoryResources{
						MemoryMB: 200,
					},
					Networks: []*structs.NetworkResource{
						{
							Device: "eth0",
							MBits:  512,
						},
					},
				},
				Shared: structs.AllocatedSharedResources{
					DiskMB: 1024,
				},
			},
			"3.152",
		},
		{
			&structs.ComparableResources{
				Flattened: structs.AllocatedTaskResources{
					Cpu: structs.AllocatedCpuResources{
						CpuShares: 2048,
					},
					Memory: structs.AllocatedMemoryResources{
						MemoryMB: 500,
					},
					Networks: []*structs.NetworkResource{
						{
							Device: "eth0",
							MBits:  1024,
						},
					},
				},
				Shared: structs.AllocatedSharedResources{
					DiskMB: 4096,
				},
			},
			"0.023",
		},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			actualDistance := fmt.Sprintf("%3.3f", basicResourceDistance(resourceAsk, tc.allocResource))
			must.Eq(t, tc.expectedDistance, actualDistance)
		})

	}

}

func makeDeviceInstance(instanceID, busID string) *structs.NodeDevice {
	return &structs.NodeDevice{
		ID:      instanceID,
		Healthy: true,
		Locality: &structs.NodeDeviceLocality{
			PciBusID: busID,
		},
	}
}

func TestPreemption_Normal(t *testing.T) {
	ci.Parallel(t)

	type testCase struct {
		desc                 string
		currentAllocations   []*structs.Allocation
		nodeReservedCapacity *structs.NodeReservedResources
		nodeCapacity         *structs.NodeResources
		resourceAsk          *structs.Resources
		jobPriority          int
		currentPreemptions   []*structs.Allocation
		preemptedAllocIDs    map[string]struct{}
	}

	highPrioJob := mock.Job()
	highPrioJob.Priority = 100

	lowPrioJob := mock.Job()
	lowPrioJob.Priority = 30

	lowPrioJob2 := mock.Job()
	lowPrioJob2.Priority = 40

	// Create some persistent alloc ids to use in test cases
	allocIDs := []string{uuid.Generate(), uuid.Generate(), uuid.Generate(), uuid.Generate(), uuid.Generate(), uuid.Generate()}

	var deviceIDs []string
	for i := 0; i < 10; i++ {
		deviceIDs = append(deviceIDs, "dev"+strconv.Itoa(i))
	}

	legacyCpuResources, processorResources := cpuResources(4000)

	defaultNodeResources := &structs.NodeResources{
		Processors: processorResources,
		Cpu:        legacyCpuResources,

		Memory: structs.NodeMemoryResources{
			MemoryMB: 8192,
		},
		Disk: structs.NodeDiskResources{
			DiskMB: 100 * 1024,
		},
		Networks: []*structs.NetworkResource{
			{
				Device: "eth0",
				CIDR:   "192.168.0.100/32",
				MBits:  1000,
			},
		},
		Devices: []*structs.NodeDeviceResource{
			{
				Type:   "gpu",
				Vendor: "nvidia",
				Name:   "1080ti",
				Attributes: map[string]*psstructs.Attribute{
					"memory":           psstructs.NewIntAttribute(11, psstructs.UnitGiB),
					"cuda_cores":       psstructs.NewIntAttribute(3584, ""),
					"graphics_clock":   psstructs.NewIntAttribute(1480, psstructs.UnitMHz),
					"memory_bandwidth": psstructs.NewIntAttribute(11, psstructs.UnitGBPerS),
				},
				Instances: []*structs.NodeDevice{
					makeDeviceInstance(deviceIDs[0], "0000:00:00.0"),
					makeDeviceInstance(deviceIDs[1], "0000:00:01.0"),
					makeDeviceInstance(deviceIDs[2], "0000:00:02.0"),
					makeDeviceInstance(deviceIDs[3], "0000:00:03.0"),
				},
			},
			{
				Type:   "gpu",
				Vendor: "nvidia",
				Name:   "2080ti",
				Attributes: map[string]*psstructs.Attribute{
					"memory":           psstructs.NewIntAttribute(11, psstructs.UnitGiB),
					"cuda_cores":       psstructs.NewIntAttribute(3584, ""),
					"graphics_clock":   psstructs.NewIntAttribute(1480, psstructs.UnitMHz),
					"memory_bandwidth": psstructs.NewIntAttribute(11, psstructs.UnitGBPerS),
				},
				Instances: []*structs.NodeDevice{
					makeDeviceInstance(deviceIDs[4], "0000:00:04.0"),
					makeDeviceInstance(deviceIDs[5], "0000:00:05.0"),
					makeDeviceInstance(deviceIDs[6], "0000:00:06.0"),
					makeDeviceInstance(deviceIDs[7], "0000:00:07.0"),
					makeDeviceInstance(deviceIDs[8], "0000:00:08.0"),
				},
			},
			{
				Type:   "fpga",
				Vendor: "intel",
				Name:   "F100",
				Attributes: map[string]*psstructs.Attribute{
					"memory": psstructs.NewIntAttribute(4, psstructs.UnitGiB),
				},
				Instances: []*structs.NodeDevice{
					makeDeviceInstance("fpga1", "0000:01:00.0"),
					makeDeviceInstance("fpga2", "0000:02:01.0"),
				},
			},
		},
	}

	reservedNodeResources := &structs.NodeReservedResources{
		Cpu: structs.NodeReservedCpuResources{
			CpuShares: 100,
		},
		Memory: structs.NodeReservedMemoryResources{
			MemoryMB: 256,
		},
		Disk: structs.NodeReservedDiskResources{
			DiskMB: 4 * 1024,
		},
	}

	testCases := []testCase{
		{
			desc: "No preemption because existing allocs are not low priority",
			currentAllocations: []*structs.Allocation{
				createAlloc(allocIDs[0], highPrioJob, &structs.Resources{
					CPU:      3200,
					MemoryMB: 7256,
					DiskMB:   4 * 1024,
					Networks: []*structs.NetworkResource{
						{
							Device: "eth0",
							IP:     "192.168.0.100",
							MBits:  50,
						},
					},
				})},
			nodeReservedCapacity: reservedNodeResources,
			nodeCapacity:         defaultNodeResources,
			jobPriority:          100,
			resourceAsk: &structs.Resources{
				CPU:      2000,
				MemoryMB: 256,
				DiskMB:   4 * 1024,
				Networks: []*structs.NetworkResource{
					{
						Device:        "eth0",
						IP:            "192.168.0.100",
						ReservedPorts: []structs.Port{{Label: "ssh", Value: 22}},
						MBits:         1,
					},
				},
			},
		},
		{
			desc: "Preempting low priority allocs not enough to meet resource ask",
			currentAllocations: []*structs.Allocation{
				createAlloc(allocIDs[0], lowPrioJob, &structs.Resources{
					CPU:      3200,
					MemoryMB: 7256,
					DiskMB:   4 * 1024,
					Networks: []*structs.NetworkResource{
						{
							Device: "eth0",
							IP:     "192.168.0.100",
							MBits:  50,
						},
					},
				})},
			nodeReservedCapacity: reservedNodeResources,
			nodeCapacity:         defaultNodeResources,
			jobPriority:          100,
			resourceAsk: &structs.Resources{
				CPU:      4000,
				MemoryMB: 8192,
				DiskMB:   4 * 1024,
				Networks: []*structs.NetworkResource{
					{
						Device:        "eth0",
						IP:            "192.168.0.100",
						ReservedPorts: []structs.Port{{Label: "ssh", Value: 22}},
						MBits:         1,
					},
				},
			},
		},
		{
			desc: "preemption impossible - static port needed is used by higher priority alloc",
			currentAllocations: []*structs.Allocation{
				createAlloc(allocIDs[0], highPrioJob, &structs.Resources{
					CPU:      1200,
					MemoryMB: 2256,
					DiskMB:   4 * 1024,
					Networks: []*structs.NetworkResource{
						{
							Device: "eth0",
							IP:     "192.168.0.100",
							MBits:  150,
						},
					},
				}),
				createAlloc(allocIDs[1], highPrioJob, &structs.Resources{
					CPU:      200,
					MemoryMB: 256,
					DiskMB:   4 * 1024,
					Networks: []*structs.NetworkResource{
						{
							Device: "eth0",
							IP:     "192.168.0.200",
							MBits:  600,
							ReservedPorts: []structs.Port{
								{
									Label: "db",
									Value: 88,
								},
							},
						},
					},
				}),
			},
			nodeReservedCapacity: reservedNodeResources,
			nodeCapacity:         defaultNodeResources,
			jobPriority:          100,
			resourceAsk: &structs.Resources{
				CPU:      600,
				MemoryMB: 1000,
				DiskMB:   25 * 1024,
				Networks: []*structs.NetworkResource{
					{
						Device: "eth0",
						IP:     "192.168.0.100",
						MBits:  700,
						ReservedPorts: []structs.Port{
							{
								Label: "db",
								Value: 88,
							},
						},
					},
				},
			},
		},
		{
			desc: "preempt only from device that has allocation with unused reserved port",
			currentAllocations: []*structs.Allocation{
				createAlloc(allocIDs[0], highPrioJob, &structs.Resources{
					CPU:      1200,
					MemoryMB: 2256,
					DiskMB:   4 * 1024,
					Networks: []*structs.NetworkResource{
						{
							Device: "eth0",
							IP:     "192.168.0.100",
							MBits:  150,
						},
					},
				}),
				createAlloc(allocIDs[1], highPrioJob, &structs.Resources{
					CPU:      200,
					MemoryMB: 256,
					DiskMB:   4 * 1024,
					Networks: []*structs.NetworkResource{
						{
							Device: "eth1",
							IP:     "192.168.0.200",
							MBits:  600,
							ReservedPorts: []structs.Port{
								{
									Label: "db",
									Value: 88,
								},
							},
						},
					},
				}),
				createAlloc(allocIDs[2], lowPrioJob, &structs.Resources{
					CPU:      200,
					MemoryMB: 256,
					DiskMB:   4 * 1024,
					Networks: []*structs.NetworkResource{
						{
							Device: "eth0",
							IP:     "192.168.0.200",
							MBits:  600,
						},
					},
				}),
			},
			nodeReservedCapacity: reservedNodeResources,
			// This test sets up a node with two NICs

			nodeCapacity: &structs.NodeResources{
				Processors: processorResources,
				Cpu:        legacyCpuResources,
				Memory: structs.NodeMemoryResources{
					MemoryMB: 8192,
				},
				Disk: structs.NodeDiskResources{
					DiskMB: 100 * 1024,
				},
				Networks: []*structs.NetworkResource{
					{
						Device: "eth0",
						CIDR:   "192.168.0.100/32",
						MBits:  1000,
					},
					{
						Device: "eth1",
						CIDR:   "192.168.1.100/32",
						MBits:  1000,
					},
				},
			},
			jobPriority: 100,
			resourceAsk: &structs.Resources{
				CPU:      600,
				MemoryMB: 1000,
				DiskMB:   25 * 1024,
				Networks: []*structs.NetworkResource{
					{
						IP:    "192.168.0.100",
						MBits: 700,
						ReservedPorts: []structs.Port{
							{
								Label: "db",
								Value: 88,
							},
						},
					},
				},
			},
			preemptedAllocIDs: map[string]struct{}{
				allocIDs[2]: {},
			},
		},
		{
			desc: "Combination of high/low priority allocs, without static ports",
			currentAllocations: []*structs.Allocation{
				createAlloc(allocIDs[0], highPrioJob, &structs.Resources{
					CPU:      2800,
					MemoryMB: 2256,
					DiskMB:   4 * 1024,
					Networks: []*structs.NetworkResource{
						{
							Device: "eth0",
							IP:     "192.168.0.100",
							MBits:  150,
						},
					},
				}),
				createAllocWithTaskgroupNetwork(allocIDs[1], lowPrioJob, &structs.Resources{
					CPU:      200,
					MemoryMB: 256,
					DiskMB:   4 * 1024,
					Networks: []*structs.NetworkResource{
						{
							Device: "eth0",
							IP:     "192.168.0.200",
							MBits:  200,
						},
					},
				}, &structs.NetworkResource{
					Device: "eth0",
					IP:     "192.168.0.201",
					MBits:  300,
				}),
				createAlloc(allocIDs[2], lowPrioJob, &structs.Resources{
					CPU:      200,
					MemoryMB: 256,
					DiskMB:   4 * 1024,
					Networks: []*structs.NetworkResource{
						{
							Device: "eth0",
							IP:     "192.168.0.100",
							MBits:  300,
						},
					},
				}),
				createAlloc(allocIDs[3], lowPrioJob, &structs.Resources{
					CPU:      700,
					MemoryMB: 256,
					DiskMB:   4 * 1024,
				}),
			},
			nodeReservedCapacity: reservedNodeResources,
			nodeCapacity:         defaultNodeResources,
			jobPriority:          100,
			resourceAsk: &structs.Resources{
				CPU:      1100,
				MemoryMB: 1000,
				DiskMB:   25 * 1024,
				Networks: []*structs.NetworkResource{
					{
						Device: "eth0",
						IP:     "192.168.0.100",
						MBits:  840,
					},
				},
			},
			preemptedAllocIDs: map[string]struct{}{
				allocIDs[1]: {},
				allocIDs[2]: {},
				allocIDs[3]: {},
			},
		},
		{
			desc: "preempt allocs with network devices",
			currentAllocations: []*structs.Allocation{
				createAlloc(allocIDs[0], lowPrioJob, &structs.Resources{
					CPU:      2800,
					MemoryMB: 2256,
					DiskMB:   4 * 1024,
				}),
				createAlloc(allocIDs[1], lowPrioJob, &structs.Resources{
					CPU:      200,
					MemoryMB: 256,
					DiskMB:   4 * 1024,
					Networks: []*structs.NetworkResource{
						{
							Device: "eth0",
							IP:     "192.168.0.200",
							MBits:  800,
						},
					},
				}),
			},
			nodeReservedCapacity: reservedNodeResources,
			nodeCapacity:         defaultNodeResources,
			jobPriority:          100,
			resourceAsk: &structs.Resources{
				CPU:      1100,
				MemoryMB: 1000,
				DiskMB:   25 * 1024,
				Networks: []*structs.NetworkResource{
					{
						Device: "eth0",
						IP:     "192.168.0.100",
						MBits:  840,
					},
				},
			},
			preemptedAllocIDs: map[string]struct{}{
				allocIDs[1]: {},
			},
		},
		{
			desc: "ignore allocs with close enough priority for network devices",
			currentAllocations: []*structs.Allocation{
				createAlloc(allocIDs[0], lowPrioJob, &structs.Resources{
					CPU:      2800,
					MemoryMB: 2256,
					DiskMB:   4 * 1024,
				}),
				createAlloc(allocIDs[1], lowPrioJob, &structs.Resources{
					CPU:      200,
					MemoryMB: 256,
					DiskMB:   4 * 1024,
					Networks: []*structs.NetworkResource{
						{
							Device: "eth0",
							IP:     "192.168.0.200",
							MBits:  800,
						},
					},
				}),
			},
			nodeReservedCapacity: reservedNodeResources,
			nodeCapacity:         defaultNodeResources,
			jobPriority:          lowPrioJob.Priority + 5,
			resourceAsk: &structs.Resources{
				CPU:      1100,
				MemoryMB: 1000,
				DiskMB:   25 * 1024,
				Networks: []*structs.NetworkResource{
					{
						Device: "eth0",
						IP:     "192.168.0.100",
						MBits:  840,
					},
				},
			},
			preemptedAllocIDs: nil,
		},
		{
			desc: "Preemption needed for all resources except network",
			currentAllocations: []*structs.Allocation{
				createAlloc(allocIDs[0], highPrioJob, &structs.Resources{
					CPU:      2800,
					MemoryMB: 2256,
					DiskMB:   40 * 1024,
					Networks: []*structs.NetworkResource{
						{
							Device: "eth0",
							IP:     "192.168.0.100",
							MBits:  150,
						},
					},
				}),
				createAlloc(allocIDs[1], lowPrioJob, &structs.Resources{
					CPU:      200,
					MemoryMB: 256,
					DiskMB:   4 * 1024,
					Networks: []*structs.NetworkResource{
						{
							Device: "eth0",
							IP:     "192.168.0.200",
							MBits:  50,
						},
					},
				}),
				createAlloc(allocIDs[2], lowPrioJob, &structs.Resources{
					CPU:      200,
					MemoryMB: 512,
					DiskMB:   25 * 1024,
				}),
				createAlloc(allocIDs[3], lowPrioJob, &structs.Resources{
					CPU:      700,
					MemoryMB: 276,
					DiskMB:   20 * 1024,
				}),
			},
			nodeReservedCapacity: reservedNodeResources,
			nodeCapacity:         defaultNodeResources,
			jobPriority:          100,
			resourceAsk: &structs.Resources{
				CPU:      1000,
				MemoryMB: 3000,
				DiskMB:   50 * 1024,
				Networks: []*structs.NetworkResource{
					{
						Device: "eth0",
						IP:     "192.168.0.100",
						MBits:  50,
					},
				},
			},
			preemptedAllocIDs: map[string]struct{}{
				allocIDs[1]: {},
				allocIDs[2]: {},
				allocIDs[3]: {},
			},
		},
		{
			desc: "Only one low priority alloc needs to be preempted",
			currentAllocations: []*structs.Allocation{
				createAlloc(allocIDs[0], highPrioJob, &structs.Resources{
					CPU:      1200,
					MemoryMB: 2256,
					DiskMB:   4 * 1024,
					Networks: []*structs.NetworkResource{
						{
							Device: "eth0",
							IP:     "192.168.0.100",
							MBits:  150,
						},
					},
				}),
				createAlloc(allocIDs[1], lowPrioJob, &structs.Resources{
					CPU:      200,
					MemoryMB: 256,
					DiskMB:   4 * 1024,
					Networks: []*structs.NetworkResource{
						{
							Device: "eth0",
							IP:     "192.168.0.100",
							MBits:  500,
						},
					},
				}),
				createAlloc(allocIDs[2], lowPrioJob, &structs.Resources{
					CPU:      200,
					MemoryMB: 256,
					DiskMB:   4 * 1024,
					Networks: []*structs.NetworkResource{
						{
							Device: "eth0",
							IP:     "192.168.0.200",
							MBits:  320,
						},
					},
				}),
			},
			nodeReservedCapacity: reservedNodeResources,
			nodeCapacity:         defaultNodeResources,
			jobPriority:          100,
			resourceAsk: &structs.Resources{
				CPU:      300,
				MemoryMB: 500,
				DiskMB:   5 * 1024,
				Networks: []*structs.NetworkResource{
					{
						Device: "eth0",
						IP:     "192.168.0.100",
						MBits:  320,
					},
				},
			},
			preemptedAllocIDs: map[string]struct{}{
				allocIDs[2]: {},
			},
		},
		{
			desc: "one alloc meets static port need, another meets remaining mbits needed",
			currentAllocations: []*structs.Allocation{
				createAlloc(allocIDs[0], highPrioJob, &structs.Resources{
					CPU:      1200,
					MemoryMB: 2256,
					DiskMB:   4 * 1024,
					Networks: []*structs.NetworkResource{
						{
							Device: "eth0",
							IP:     "192.168.0.100",
							MBits:  150,
						},
					},
				}),
				createAlloc(allocIDs[1], lowPrioJob, &structs.Resources{
					CPU:      200,
					MemoryMB: 256,
					DiskMB:   4 * 1024,
					Networks: []*structs.NetworkResource{
						{
							Device: "eth0",
							IP:     "192.168.0.200",
							MBits:  500,
							ReservedPorts: []structs.Port{
								{
									Label: "db",
									Value: 88,
								},
							},
						},
					},
				}),
				createAlloc(allocIDs[2], lowPrioJob, &structs.Resources{
					CPU:      200,
					MemoryMB: 256,
					DiskMB:   4 * 1024,
					Networks: []*structs.NetworkResource{
						{
							Device: "eth0",
							IP:     "192.168.0.100",
							MBits:  200,
						},
					},
				}),
			},
			nodeReservedCapacity: reservedNodeResources,
			nodeCapacity:         defaultNodeResources,
			jobPriority:          100,
			resourceAsk: &structs.Resources{
				CPU:      2700,
				MemoryMB: 1000,
				DiskMB:   25 * 1024,
				Networks: []*structs.NetworkResource{
					{
						Device: "eth0",
						IP:     "192.168.0.100",
						MBits:  800,
						ReservedPorts: []structs.Port{
							{
								Label: "db",
								Value: 88,
							},
						},
					},
				},
			},
			preemptedAllocIDs: map[string]struct{}{
				allocIDs[1]: {},
				allocIDs[2]: {},
			},
		},
		{
			desc: "alloc that meets static port need also meets other needs",
			currentAllocations: []*structs.Allocation{
				createAlloc(allocIDs[0], highPrioJob, &structs.Resources{
					CPU:      1200,
					MemoryMB: 2256,
					DiskMB:   4 * 1024,
					Networks: []*structs.NetworkResource{
						{
							Device: "eth0",
							IP:     "192.168.0.100",
							MBits:  150,
						},
					},
				}),
				createAlloc(allocIDs[1], lowPrioJob, &structs.Resources{
					CPU:      200,
					MemoryMB: 256,
					DiskMB:   4 * 1024,
					Networks: []*structs.NetworkResource{
						{
							Device: "eth0",
							IP:     "192.168.0.200",
							MBits:  600,
							ReservedPorts: []structs.Port{
								{
									Label: "db",
									Value: 88,
								},
							},
						},
					},
				}),
				createAlloc(allocIDs[2], lowPrioJob, &structs.Resources{
					CPU:      200,
					MemoryMB: 256,
					DiskMB:   4 * 1024,
					Networks: []*structs.NetworkResource{
						{
							Device: "eth0",
							IP:     "192.168.0.100",
							MBits:  100,
						},
					},
				}),
			},
			nodeReservedCapacity: reservedNodeResources,
			nodeCapacity:         defaultNodeResources,
			jobPriority:          100,
			resourceAsk: &structs.Resources{
				CPU:      600,
				MemoryMB: 1000,
				DiskMB:   25 * 1024,
				Networks: []*structs.NetworkResource{
					{
						Device: "eth0",
						IP:     "192.168.0.100",
						MBits:  700,
						ReservedPorts: []structs.Port{
							{
								Label: "db",
								Value: 88,
							},
						},
					},
				},
			},
			preemptedAllocIDs: map[string]struct{}{
				allocIDs[1]: {},
			},
		},
		{
			desc: "alloc from job that has existing evictions not chosen for preemption",
			currentAllocations: []*structs.Allocation{
				createAlloc(allocIDs[0], highPrioJob, &structs.Resources{
					CPU:      1200,
					MemoryMB: 2256,
					DiskMB:   4 * 1024,
					Networks: []*structs.NetworkResource{
						{
							Device: "eth0",
							IP:     "192.168.0.100",
							MBits:  150,
						},
					},
				}),
				createAlloc(allocIDs[1], lowPrioJob, &structs.Resources{
					CPU:      200,
					MemoryMB: 256,
					DiskMB:   4 * 1024,
					Networks: []*structs.NetworkResource{
						{
							Device: "eth0",
							IP:     "192.168.0.200",
							MBits:  500,
						},
					},
				}),
				createAlloc(allocIDs[2], lowPrioJob2, &structs.Resources{
					CPU:      200,
					MemoryMB: 256,
					DiskMB:   4 * 1024,
					Networks: []*structs.NetworkResource{
						{
							Device: "eth0",
							IP:     "192.168.0.100",
							MBits:  300,
						},
					},
				}),
			},
			nodeReservedCapacity: reservedNodeResources,
			nodeCapacity:         defaultNodeResources,
			jobPriority:          100,
			resourceAsk: &structs.Resources{
				CPU:      300,
				MemoryMB: 500,
				DiskMB:   5 * 1024,
				Networks: []*structs.NetworkResource{
					{
						Device: "eth0",
						IP:     "192.168.0.100",
						MBits:  320,
					},
				},
			},
			currentPreemptions: []*structs.Allocation{
				createAlloc(allocIDs[4], lowPrioJob2, &structs.Resources{
					CPU:      200,
					MemoryMB: 256,
					DiskMB:   4 * 1024,
					Networks: []*structs.NetworkResource{
						{
							Device: "eth0",
							IP:     "192.168.0.100",
							MBits:  300,
						},
					},
				}),
			},
			preemptedAllocIDs: map[string]struct{}{
				allocIDs[1]: {},
			},
		},
		{
			desc: "Preemption with one device instance per alloc",
			// Add allocations that use two device instances
			currentAllocations: []*structs.Allocation{
				createAllocWithDevice(allocIDs[0], lowPrioJob, &structs.Resources{
					CPU:      500,
					MemoryMB: 512,
					DiskMB:   4 * 1024,
				}, &structs.AllocatedDeviceResource{
					Type:      "gpu",
					Vendor:    "nvidia",
					Name:      "1080ti",
					DeviceIDs: []string{deviceIDs[0]},
				}),
				createAllocWithDevice(allocIDs[1], lowPrioJob, &structs.Resources{
					CPU:      200,
					MemoryMB: 512,
					DiskMB:   4 * 1024,
				}, &structs.AllocatedDeviceResource{
					Type:      "gpu",
					Vendor:    "nvidia",
					Name:      "1080ti",
					DeviceIDs: []string{deviceIDs[1]},
				})},
			nodeReservedCapacity: reservedNodeResources,
			nodeCapacity:         defaultNodeResources,
			jobPriority:          100,
			resourceAsk: &structs.Resources{
				CPU:      1000,
				MemoryMB: 512,
				DiskMB:   4 * 1024,
				Devices: []*structs.RequestedDevice{
					{
						Name:  "nvidia/gpu/1080ti",
						Count: 4,
					},
				},
			},
			preemptedAllocIDs: map[string]struct{}{
				allocIDs[0]: {},
				allocIDs[1]: {},
			},
		},
		{
			desc: "Preemption multiple devices used",
			currentAllocations: []*structs.Allocation{
				createAllocWithDevice(allocIDs[0], lowPrioJob, &structs.Resources{
					CPU:      500,
					MemoryMB: 512,
					DiskMB:   4 * 1024,
				}, &structs.AllocatedDeviceResource{
					Type:      "gpu",
					Vendor:    "nvidia",
					Name:      "1080ti",
					DeviceIDs: []string{deviceIDs[0], deviceIDs[1], deviceIDs[2], deviceIDs[3]},
				}),
				createAllocWithDevice(allocIDs[1], lowPrioJob, &structs.Resources{
					CPU:      200,
					MemoryMB: 512,
					DiskMB:   4 * 1024,
				}, &structs.AllocatedDeviceResource{
					Type:      "fpga",
					Vendor:    "intel",
					Name:      "F100",
					DeviceIDs: []string{"fpga1"},
				})},
			nodeReservedCapacity: reservedNodeResources,
			nodeCapacity:         defaultNodeResources,
			jobPriority:          100,
			resourceAsk: &structs.Resources{
				CPU:      1000,
				MemoryMB: 512,
				DiskMB:   4 * 1024,
				Devices: []*structs.RequestedDevice{
					{
						Name:  "nvidia/gpu/1080ti",
						Count: 4,
					},
				},
			},
			preemptedAllocIDs: map[string]struct{}{
				allocIDs[0]: {},
			},
		},
		{
			// This test cases creates allocations across two GPUs
			// Both GPUs are eligible for the task, but only allocs sharing the
			// same device should be chosen for preemption
			desc: "Preemption with allocs across multiple devices that match",
			currentAllocations: []*structs.Allocation{
				createAllocWithDevice(allocIDs[0], lowPrioJob, &structs.Resources{
					CPU:      500,
					MemoryMB: 512,
					DiskMB:   4 * 1024,
				}, &structs.AllocatedDeviceResource{
					Type:      "gpu",
					Vendor:    "nvidia",
					Name:      "1080ti",
					DeviceIDs: []string{deviceIDs[0], deviceIDs[1]},
				}),
				createAllocWithDevice(allocIDs[1], highPrioJob, &structs.Resources{
					CPU:      200,
					MemoryMB: 100,
					DiskMB:   4 * 1024,
				}, &structs.AllocatedDeviceResource{
					Type:      "gpu",
					Vendor:    "nvidia",
					Name:      "1080ti",
					DeviceIDs: []string{deviceIDs[2]},
				}),
				createAllocWithDevice(allocIDs[2], lowPrioJob, &structs.Resources{
					CPU:      200,
					MemoryMB: 256,
					DiskMB:   4 * 1024,
				}, &structs.AllocatedDeviceResource{
					Type:      "gpu",
					Vendor:    "nvidia",
					Name:      "2080ti",
					DeviceIDs: []string{deviceIDs[4], deviceIDs[5]},
				}),
				createAllocWithDevice(allocIDs[3], lowPrioJob, &structs.Resources{
					CPU:      100,
					MemoryMB: 256,
					DiskMB:   4 * 1024,
				}, &structs.AllocatedDeviceResource{
					Type:      "gpu",
					Vendor:    "nvidia",
					Name:      "2080ti",
					DeviceIDs: []string{deviceIDs[6], deviceIDs[7]},
				}),
				createAllocWithDevice(allocIDs[4], lowPrioJob, &structs.Resources{
					CPU:      200,
					MemoryMB: 512,
					DiskMB:   4 * 1024,
				}, &structs.AllocatedDeviceResource{
					Type:      "fpga",
					Vendor:    "intel",
					Name:      "F100",
					DeviceIDs: []string{"fpga1"},
				})},
			nodeReservedCapacity: reservedNodeResources,
			nodeCapacity:         defaultNodeResources,
			jobPriority:          100,
			resourceAsk: &structs.Resources{
				CPU:      1000,
				MemoryMB: 512,
				DiskMB:   4 * 1024,
				Devices: []*structs.RequestedDevice{
					{
						Name:  "gpu",
						Count: 4,
					},
				},
			},
			preemptedAllocIDs: map[string]struct{}{
				allocIDs[2]: {},
				allocIDs[3]: {},
			},
		},
		{
			// This test cases creates allocations across two GPUs
			// Both GPUs are eligible for the task, but only allocs with the lower
			// priority are chosen
			desc: "Preemption with lower/higher priority combinations",
			currentAllocations: []*structs.Allocation{
				createAllocWithDevice(allocIDs[0], lowPrioJob, &structs.Resources{
					CPU:      500,
					MemoryMB: 512,
					DiskMB:   4 * 1024,
				}, &structs.AllocatedDeviceResource{
					Type:      "gpu",
					Vendor:    "nvidia",
					Name:      "1080ti",
					DeviceIDs: []string{deviceIDs[0], deviceIDs[1]},
				}),
				createAllocWithDevice(allocIDs[1], lowPrioJob2, &structs.Resources{
					CPU:      200,
					MemoryMB: 100,
					DiskMB:   4 * 1024,
				}, &structs.AllocatedDeviceResource{
					Type:      "gpu",
					Vendor:    "nvidia",
					Name:      "1080ti",
					DeviceIDs: []string{deviceIDs[2], deviceIDs[3]},
				}),
				createAllocWithDevice(allocIDs[2], lowPrioJob, &structs.Resources{
					CPU:      200,
					MemoryMB: 256,
					DiskMB:   4 * 1024,
				}, &structs.AllocatedDeviceResource{
					Type:      "gpu",
					Vendor:    "nvidia",
					Name:      "2080ti",
					DeviceIDs: []string{deviceIDs[4], deviceIDs[5]},
				}),
				createAllocWithDevice(allocIDs[3], lowPrioJob, &structs.Resources{
					CPU:      100,
					MemoryMB: 256,
					DiskMB:   4 * 1024,
				}, &structs.AllocatedDeviceResource{
					Type:      "gpu",
					Vendor:    "nvidia",
					Name:      "2080ti",
					DeviceIDs: []string{deviceIDs[6], deviceIDs[7]},
				}),
				createAllocWithDevice(allocIDs[4], lowPrioJob, &structs.Resources{
					CPU:      100,
					MemoryMB: 256,
					DiskMB:   4 * 1024,
				}, &structs.AllocatedDeviceResource{
					Type:      "gpu",
					Vendor:    "nvidia",
					Name:      "2080ti",
					DeviceIDs: []string{deviceIDs[8]},
				}),
				createAllocWithDevice(allocIDs[5], lowPrioJob, &structs.Resources{
					CPU:      200,
					MemoryMB: 512,
					DiskMB:   4 * 1024,
				}, &structs.AllocatedDeviceResource{
					Type:      "fpga",
					Vendor:    "intel",
					Name:      "F100",
					DeviceIDs: []string{"fpga1"},
				})},
			nodeReservedCapacity: reservedNodeResources,
			nodeCapacity:         defaultNodeResources,
			jobPriority:          100,
			resourceAsk: &structs.Resources{
				CPU:      1000,
				MemoryMB: 512,
				DiskMB:   4 * 1024,
				Devices: []*structs.RequestedDevice{
					{
						Name:  "gpu",
						Count: 4,
					},
				},
			},
			preemptedAllocIDs: map[string]struct{}{
				allocIDs[2]: {},
				allocIDs[3]: {},
			},
		},
		{
			desc: "Device preemption not possible due to more instances needed than available",
			currentAllocations: []*structs.Allocation{
				createAllocWithDevice(allocIDs[0], lowPrioJob, &structs.Resources{
					CPU:      500,
					MemoryMB: 512,
					DiskMB:   4 * 1024,
				}, &structs.AllocatedDeviceResource{
					Type:      "gpu",
					Vendor:    "nvidia",
					Name:      "1080ti",
					DeviceIDs: []string{deviceIDs[0], deviceIDs[1], deviceIDs[2], deviceIDs[3]},
				}),
				createAllocWithDevice(allocIDs[1], lowPrioJob, &structs.Resources{
					CPU:      200,
					MemoryMB: 512,
					DiskMB:   4 * 1024,
				}, &structs.AllocatedDeviceResource{
					Type:      "fpga",
					Vendor:    "intel",
					Name:      "F100",
					DeviceIDs: []string{"fpga1"},
				})},
			nodeReservedCapacity: reservedNodeResources,
			nodeCapacity:         defaultNodeResources,
			jobPriority:          100,
			resourceAsk: &structs.Resources{
				CPU:      1000,
				MemoryMB: 512,
				DiskMB:   4 * 1024,
				Devices: []*structs.RequestedDevice{
					{
						Name:  "gpu",
						Count: 6,
					},
				},
			},
		},
		// This test case exercises the code path for a final filtering step that tries to
		// minimize the number of preemptible allocations
		{
			desc: "Filter out allocs whose resource usage superset is also in the preemption list",
			currentAllocations: []*structs.Allocation{
				createAlloc(allocIDs[0], highPrioJob, &structs.Resources{
					CPU:      1800,
					MemoryMB: 2256,
					DiskMB:   4 * 1024,
					Networks: []*structs.NetworkResource{
						{
							Device: "eth0",
							IP:     "192.168.0.100",
							MBits:  150,
						},
					},
				}),
				createAlloc(allocIDs[1], lowPrioJob, &structs.Resources{
					CPU:      1500,
					MemoryMB: 256,
					DiskMB:   5 * 1024,
					Networks: []*structs.NetworkResource{
						{
							Device: "eth0",
							IP:     "192.168.0.100",
							MBits:  100,
						},
					},
				}),
				createAlloc(allocIDs[2], lowPrioJob, &structs.Resources{
					CPU:      600,
					MemoryMB: 256,
					DiskMB:   5 * 1024,
					Networks: []*structs.NetworkResource{
						{
							Device: "eth0",
							IP:     "192.168.0.200",
							MBits:  300,
						},
					},
				}),
			},
			nodeReservedCapacity: reservedNodeResources,
			nodeCapacity:         defaultNodeResources,
			jobPriority:          100,
			resourceAsk: &structs.Resources{
				CPU:      1000,
				MemoryMB: 256,
				DiskMB:   5 * 1024,
				Networks: []*structs.NetworkResource{
					{
						Device: "eth0",
						IP:     "192.168.0.100",
						MBits:  50,
					},
				},
			},
			preemptedAllocIDs: map[string]struct{}{
				allocIDs[1]: {},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			node := mock.Node()
			node.NodeResources = tc.nodeCapacity
			node.ReservedResources = tc.nodeReservedCapacity

			state, ctx := testContext(t)

			nodes := []*RankedNode{
				{
					Node: node,
				},
			}
			state.UpsertNode(structs.MsgTypeTestSetup, 1000, node)
			for _, alloc := range tc.currentAllocations {
				alloc.NodeID = node.ID
			}
			err := state.UpsertAllocs(structs.MsgTypeTestSetup, 1001, tc.currentAllocations)

			must.NoError(t, err)
			if tc.currentPreemptions != nil {
				ctx.plan.NodePreemptions[node.ID] = tc.currentPreemptions
			}
			static := NewStaticRankIterator(ctx, nodes)
			binPackIter := NewBinPackIterator(ctx, static, true, tc.jobPriority)
			job := mock.Job()
			job.Priority = tc.jobPriority
			binPackIter.SetJob(job)
			binPackIter.SetSchedulerConfiguration(testSchedulerConfig)

			taskGroup := &structs.TaskGroup{
				EphemeralDisk: &structs.EphemeralDisk{},
				Tasks: []*structs.Task{
					{
						Name:      "web",
						Resources: tc.resourceAsk,
					},
				},
			}

			binPackIter.SetTaskGroup(taskGroup)
			option := binPackIter.Next()
			if tc.preemptedAllocIDs == nil {
				must.Nil(t, option)
			} else {
				must.NotNil(t, option)
				preemptedAllocs := option.PreemptedAllocs
				must.Eq(t, len(tc.preemptedAllocIDs), len(preemptedAllocs))
				for _, alloc := range preemptedAllocs {
					_, ok := tc.preemptedAllocIDs[alloc.ID]
					must.True(t, ok, must.Sprintf("alloc %s was preempted unexpectedly", alloc.ID))
				}
			}
		})
	}
}

// TestPreemptionMultiple tests evicting multiple allocations in the same time
func TestPreemptionMultiple(t *testing.T) {
	ci.Parallel(t)

	// The test setup:
	//  * a node with 4 GPUs
	//  * a low priority job with 4 allocs, each is using 1 GPU
	//
	// Then schedule a high priority job needing 2 allocs, using 2 GPUs each.
	// Expectation:
	// All low priority allocs should preempted to accomodate the high priority job
	h := NewHarness(t)

	legacyCpuResources, processorResources := cpuResources(4000)

	// node with 4 GPUs
	node := mock.Node()
	node.NodeResources = &structs.NodeResources{
		Processors: processorResources,
		Cpu:        legacyCpuResources,
		Memory: structs.NodeMemoryResources{
			MemoryMB: 8192,
		},
		Disk: structs.NodeDiskResources{
			DiskMB: 100 * 1024,
		},
		Networks: []*structs.NetworkResource{
			{
				Device: "eth0",
				CIDR:   "192.168.0.100/32",
				MBits:  1000,
			},
		},
		Devices: []*structs.NodeDeviceResource{
			{
				Type:   "gpu",
				Vendor: "nvidia",
				Name:   "1080ti",
				Attributes: map[string]*psstructs.Attribute{
					"memory":           psstructs.NewIntAttribute(11, psstructs.UnitGiB),
					"cuda_cores":       psstructs.NewIntAttribute(3584, ""),
					"graphics_clock":   psstructs.NewIntAttribute(1480, psstructs.UnitMHz),
					"memory_bandwidth": psstructs.NewIntAttribute(11, psstructs.UnitGBPerS),
				},
				Instances: []*structs.NodeDevice{
					{
						ID:      "dev0",
						Healthy: true,
					},
					{
						ID:      "dev1",
						Healthy: true,
					},
					{
						ID:      "dev2",
						Healthy: true,
					},
					{
						ID:      "dev3",
						Healthy: true,
					},
				},
			},
		},
	}

	must.NoError(t, h.State.UpsertNode(structs.MsgTypeTestSetup, h.NextIndex(), node))

	// low priority job with 4 allocs using all 4 GPUs
	lowPrioJob := mock.Job()
	lowPrioJob.Priority = 5
	lowPrioJob.TaskGroups[0].Count = 4
	lowPrioJob.TaskGroups[0].Networks = nil
	lowPrioJob.TaskGroups[0].Tasks[0].Services = nil
	lowPrioJob.TaskGroups[0].Tasks[0].Resources.Networks = nil
	lowPrioJob.TaskGroups[0].Tasks[0].Resources.Devices = structs.ResourceDevices{{
		Name:  "gpu",
		Count: 1,
	}}
	must.NoError(t, h.State.UpsertJob(structs.MsgTypeTestSetup, h.NextIndex(), nil, lowPrioJob))

	allocs := []*structs.Allocation{}
	allocIDs := map[string]struct{}{}
	for i := 0; i < 4; i++ {
		alloc := createAllocWithDevice(uuid.Generate(), lowPrioJob, lowPrioJob.TaskGroups[0].Tasks[0].Resources, &structs.AllocatedDeviceResource{
			Type:      "gpu",
			Vendor:    "nvidia",
			Name:      "1080ti",
			DeviceIDs: []string{fmt.Sprintf("dev%d", i)},
		})
		alloc.NodeID = node.ID

		allocs = append(allocs, alloc)
		allocIDs[alloc.ID] = struct{}{}
	}
	must.NoError(t, h.State.UpsertAllocs(structs.MsgTypeTestSetup, h.NextIndex(), allocs))

	// new high priority job with 2 allocs, each using 2 GPUs
	highPrioJob := mock.Job()
	highPrioJob.Priority = 100
	highPrioJob.TaskGroups[0].Count = 2
	highPrioJob.TaskGroups[0].Networks = nil
	highPrioJob.TaskGroups[0].Tasks[0].Services = nil
	highPrioJob.TaskGroups[0].Tasks[0].Resources.Networks = nil
	highPrioJob.TaskGroups[0].Tasks[0].Resources.Devices = structs.ResourceDevices{{
		Name:  "gpu",
		Count: 2,
	}}
	must.NoError(t, h.State.UpsertJob(structs.MsgTypeTestSetup, h.NextIndex(), nil, highPrioJob))

	// schedule
	eval := &structs.Evaluation{
		Namespace:   structs.DefaultNamespace,
		ID:          uuid.Generate(),
		Priority:    highPrioJob.Priority,
		TriggeredBy: structs.EvalTriggerJobRegister,
		JobID:       highPrioJob.ID,
		Status:      structs.EvalStatusPending,
	}
	must.NoError(t, h.State.UpsertEvals(structs.MsgTypeTestSetup, h.NextIndex(), []*structs.Evaluation{eval}))

	// Process the evaluation
	must.NoError(t, h.Process(NewServiceScheduler, eval))
	must.Len(t, 1, h.Plans)
	must.MapContainsKey(t, h.Plans[0].NodePreemptions, node.ID)

	preempted := map[string]struct{}{}
	for _, alloc := range h.Plans[0].NodePreemptions[node.ID] {
		preempted[alloc.ID] = struct{}{}
	}
	must.Eq(t, allocIDs, preempted)
}

// helper method to create allocations with given jobs and resources
func createAlloc(id string, job *structs.Job, resource *structs.Resources) *structs.Allocation {
	return createAllocInner(id, job, resource, nil, nil)
}

// helper method to create allocation with network at the task group level
func createAllocWithTaskgroupNetwork(id string, job *structs.Job, resource *structs.Resources, tgNet *structs.NetworkResource) *structs.Allocation {
	return createAllocInner(id, job, resource, nil, tgNet)
}

func createAllocWithDevice(id string, job *structs.Job, resource *structs.Resources, allocatedDevices *structs.AllocatedDeviceResource) *structs.Allocation {
	return createAllocInner(id, job, resource, allocatedDevices, nil)
}

func createAllocInner(id string, job *structs.Job, resource *structs.Resources, allocatedDevices *structs.AllocatedDeviceResource, tgNetwork *structs.NetworkResource) *structs.Allocation {
	alloc := &structs.Allocation{
		ID:    id,
		Job:   job,
		JobID: job.ID,
		TaskResources: map[string]*structs.Resources{
			"web": resource,
		},
		Namespace:     structs.DefaultNamespace,
		EvalID:        uuid.Generate(),
		DesiredStatus: structs.AllocDesiredStatusRun,
		ClientStatus:  structs.AllocClientStatusRunning,
		TaskGroup:     "web",
		AllocatedResources: &structs.AllocatedResources{
			Tasks: map[string]*structs.AllocatedTaskResources{
				"web": {
					Cpu: structs.AllocatedCpuResources{
						CpuShares: int64(resource.CPU),
					},
					Memory: structs.AllocatedMemoryResources{
						MemoryMB: int64(resource.MemoryMB),
					},
					Networks: resource.Networks,
				},
			},
		},
	}

	if allocatedDevices != nil {
		alloc.AllocatedResources.Tasks["web"].Devices = []*structs.AllocatedDeviceResource{allocatedDevices}
	}

	if tgNetwork != nil {
		alloc.AllocatedResources.Shared = structs.AllocatedSharedResources{
			Networks: []*structs.NetworkResource{tgNetwork},
		}
	}
	return alloc
}

// greedyGPUNode returns a node with `gpuCount` GPUs of type "gpu/nvidia/1080ti".
func greedyGPUNode(t *testing.T, gpuCount int) *structs.Node {
	t.Helper()
	legacyCpuResources, processorResources := cpuResources(4000)
	instances := make([]*structs.NodeDevice, gpuCount)
	for i := 0; i < gpuCount; i++ {
		instances[i] = &structs.NodeDevice{ID: fmt.Sprintf("dev%d", i), Healthy: true}
	}
	node := mock.Node()
	node.NodeResources = &structs.NodeResources{
		Processors: processorResources,
		Cpu:        legacyCpuResources,
		Memory:     structs.NodeMemoryResources{MemoryMB: 8192},
		Disk:       structs.NodeDiskResources{DiskMB: 100 * 1024},
		Networks: []*structs.NetworkResource{
			{Device: "eth0", CIDR: "192.168.0.100/32", MBits: 1000},
		},
		Devices: []*structs.NodeDeviceResource{{
			Type:      "gpu",
			Vendor:    "nvidia",
			Name:      "1080ti",
			Instances: instances,
		}},
	}
	return node
}

// greedyGPUJob builds a 1-GPU job (count=1). If greedy is true, sets meta.greedy="true".
func greedyGPUJob(priority int, greedy bool) *structs.Job {
	j := mock.Job()
	j.Priority = priority
	j.TaskGroups[0].Count = 1
	j.TaskGroups[0].Networks = nil
	j.TaskGroups[0].Tasks[0].Services = nil
	j.TaskGroups[0].Tasks[0].Resources.Networks = nil
	j.TaskGroups[0].Tasks[0].Resources.Devices = structs.ResourceDevices{{
		Name:  "gpu",
		Count: 1,
	}}
	if greedy {
		if j.Meta == nil {
			j.Meta = map[string]string{}
		}
		j.Meta[structs.JobMetaGreedy] = "true"
	}
	return j
}

// runJobEval submits the job + a pending JobRegister eval and runs the service scheduler.
func runJobEval(t *testing.T, h *Harness, j *structs.Job) {
	t.Helper()
	must.NoError(t, h.State.UpsertJob(structs.MsgTypeTestSetup, h.NextIndex(), nil, j))
	eval := &structs.Evaluation{
		Namespace:   structs.DefaultNamespace,
		ID:          uuid.Generate(),
		Priority:    j.Priority,
		TriggeredBy: structs.EvalTriggerJobRegister,
		JobID:       j.ID,
		Status:      structs.EvalStatusPending,
	}
	must.NoError(t, h.State.UpsertEvals(structs.MsgTypeTestSetup, h.NextIndex(), []*structs.Evaluation{eval}))
	must.NoError(t, h.Process(NewServiceScheduler, eval))
}

// enableGreedyPreemption sets PreemptionConfig.GreedyPreemptionEnabled = true.
// Combine extra options (e.g. ServiceSchedulerEnabled) by passing them in.
func enableGreedyPreemption(t *testing.T, h *Harness, extra structs.PreemptionConfig) {
	t.Helper()
	extra.GreedyPreemptionEnabled = true
	must.NoError(t, h.State.SchedulerSetConfig(h.NextIndex(), &structs.SchedulerConfiguration{
		PreemptionConfig: extra,
	}))
}

// TestPreemption_Greedy_DeviceEvictionWhenGeneralPreemptionDisabled verifies
// the third pass: when general preemption is off (default for service jobs),
// an incoming non-greedy job still evicts a greedy alloc to claim its GPU.
func TestPreemption_Greedy_DeviceEvictionWhenGeneralPreemptionDisabled(t *testing.T) {
	ci.Parallel(t)
	h := NewHarness(t)

	node := greedyGPUNode(t, 1)
	must.NoError(t, h.State.UpsertNode(structs.MsgTypeTestSetup, h.NextIndex(), node))
	enableGreedyPreemption(t, h, structs.PreemptionConfig{})

	greedyJob := greedyGPUJob(50, true)
	must.NoError(t, h.State.UpsertJob(structs.MsgTypeTestSetup, h.NextIndex(), nil, greedyJob))
	greedyAlloc := createAllocWithDevice(uuid.Generate(), greedyJob, greedyJob.TaskGroups[0].Tasks[0].Resources,
		&structs.AllocatedDeviceResource{Type: "gpu", Vendor: "nvidia", Name: "1080ti", DeviceIDs: []string{"dev0"}})
	greedyAlloc.NodeID = node.ID
	must.NoError(t, h.State.UpsertAllocs(structs.MsgTypeTestSetup, h.NextIndex(), []*structs.Allocation{greedyAlloc}))

	// New non-greedy job at same priority — would be impossible under normal
	// preemption (delta < 10), AND general preemption is disabled by default.
	newJob := greedyGPUJob(50, false)
	runJobEval(t, h, newJob)

	must.Len(t, 1, h.Plans)
	must.MapContainsKey(t, h.Plans[0].NodePreemptions, node.ID)
	preempted := h.Plans[0].NodePreemptions[node.ID]
	must.Len(t, 1, preempted)
	must.Eq(t, greedyAlloc.ID, preempted[0].ID)
	must.StrContains(t, preempted[0].DesiredDescription, "Greedy alloc evicted")
}

// TestPreemption_Greedy_DoesNotEvictNonGreedyAllocs verifies the third pass
// is greedy-only — a non-greedy alloc holding the GPU is left alone and the
// new job ends up blocked.
func TestPreemption_Greedy_DoesNotEvictNonGreedyAllocs(t *testing.T) {
	ci.Parallel(t)
	h := NewHarness(t)

	node := greedyGPUNode(t, 1)
	must.NoError(t, h.State.UpsertNode(structs.MsgTypeTestSetup, h.NextIndex(), node))
	enableGreedyPreemption(t, h, structs.PreemptionConfig{})

	holder := greedyGPUJob(50, false) // no meta.greedy
	must.NoError(t, h.State.UpsertJob(structs.MsgTypeTestSetup, h.NextIndex(), nil, holder))
	holderAlloc := createAllocWithDevice(uuid.Generate(), holder, holder.TaskGroups[0].Tasks[0].Resources,
		&structs.AllocatedDeviceResource{Type: "gpu", Vendor: "nvidia", Name: "1080ti", DeviceIDs: []string{"dev0"}})
	holderAlloc.NodeID = node.ID
	must.NoError(t, h.State.UpsertAllocs(structs.MsgTypeTestSetup, h.NextIndex(), []*structs.Allocation{holderAlloc}))

	newJob := greedyGPUJob(50, false)
	runJobEval(t, h, newJob)

	// No preemption should have occurred.
	if len(h.Plans) > 0 {
		must.Eq(t, 0, len(h.Plans[0].NodePreemptions),
			must.Sprintf("expected no preemption, got %+v", h.Plans[0].NodePreemptions))
	}
}

// TestPreemption_Greedy_DoesNotEvictWhenIncomingJobIsGreedy guards against
// greedy-evicts-greedy thrash.
func TestPreemption_Greedy_DoesNotEvictWhenIncomingJobIsGreedy(t *testing.T) {
	ci.Parallel(t)
	h := NewHarness(t)

	node := greedyGPUNode(t, 1)
	must.NoError(t, h.State.UpsertNode(structs.MsgTypeTestSetup, h.NextIndex(), node))
	enableGreedyPreemption(t, h, structs.PreemptionConfig{})

	existing := greedyGPUJob(50, true)
	must.NoError(t, h.State.UpsertJob(structs.MsgTypeTestSetup, h.NextIndex(), nil, existing))
	existingAlloc := createAllocWithDevice(uuid.Generate(), existing, existing.TaskGroups[0].Tasks[0].Resources,
		&structs.AllocatedDeviceResource{Type: "gpu", Vendor: "nvidia", Name: "1080ti", DeviceIDs: []string{"dev0"}})
	existingAlloc.NodeID = node.ID
	must.NoError(t, h.State.UpsertAllocs(structs.MsgTypeTestSetup, h.NextIndex(), []*structs.Allocation{existingAlloc}))

	newJob := greedyGPUJob(50, true) // also greedy
	runJobEval(t, h, newJob)

	if len(h.Plans) > 0 {
		must.Eq(t, 0, len(h.Plans[0].NodePreemptions),
			must.Sprintf("greedy job must not evict another greedy alloc; got %+v", h.Plans[0].NodePreemptions))
	}
}

// TestPreemption_Greedy_IgnoresPriorityDelta verifies the third pass bypasses
// the priority-delta-of-10 rule — greedy is evictable even when the incoming
// job has equal or lower priority.
func TestPreemption_Greedy_IgnoresPriorityDelta(t *testing.T) {
	ci.Parallel(t)
	h := NewHarness(t)

	node := greedyGPUNode(t, 1)
	must.NoError(t, h.State.UpsertNode(structs.MsgTypeTestSetup, h.NextIndex(), node))
	enableGreedyPreemption(t, h, structs.PreemptionConfig{})

	// Greedy alloc at priority 50; incoming non-greedy at priority 1.
	// Delta is -49, far inside the "skip" range — but greedy-only mode
	// ignores it.
	greedyJob := greedyGPUJob(50, true)
	must.NoError(t, h.State.UpsertJob(structs.MsgTypeTestSetup, h.NextIndex(), nil, greedyJob))
	greedyAlloc := createAllocWithDevice(uuid.Generate(), greedyJob, greedyJob.TaskGroups[0].Tasks[0].Resources,
		&structs.AllocatedDeviceResource{Type: "gpu", Vendor: "nvidia", Name: "1080ti", DeviceIDs: []string{"dev0"}})
	greedyAlloc.NodeID = node.ID
	must.NoError(t, h.State.UpsertAllocs(structs.MsgTypeTestSetup, h.NextIndex(), []*structs.Allocation{greedyAlloc}))

	newJob := greedyGPUJob(1, false)
	runJobEval(t, h, newJob)

	must.Len(t, 1, h.Plans)
	must.MapContainsKey(t, h.Plans[0].NodePreemptions, node.ID)
}

// TestPreemption_Greedy_PreferredOverGeneralWhenBothApply verifies that
// under zero-cost greedy masking, when both general preemption and greedy
// masking are enabled and either could satisfy the new alloc, the greedy
// alloc is evicted in preference to the low-priority non-greedy alloc:
// greedy eviction is free (no follow-up eval, no reschedule), so it should
// always win over a non-greedy preemption that would incur a follow-up eval.
//
// The masking step in BinPackIterator.Next derives greedy victims from the
// device IDs the new alloc actually claims, *before* the trailing fallback
// runs PreemptForTaskGroup against non-greedy candidates.
func TestPreemption_Greedy_PreferredOverGeneralWhenBothApply(t *testing.T) {
	ci.Parallel(t)
	h := NewHarness(t)

	node := greedyGPUNode(t, 2)
	must.NoError(t, h.State.UpsertNode(structs.MsgTypeTestSetup, h.NextIndex(), node))

	enableGreedyPreemption(t, h, structs.PreemptionConfig{ServiceSchedulerEnabled: true})

	// One greedy alloc at priority 50 holding dev0, one low-priority
	// non-greedy alloc holding dev1.
	greedyJob := greedyGPUJob(50, true)
	must.NoError(t, h.State.UpsertJob(structs.MsgTypeTestSetup, h.NextIndex(), nil, greedyJob))
	greedyAlloc := createAllocWithDevice(uuid.Generate(), greedyJob, greedyJob.TaskGroups[0].Tasks[0].Resources,
		&structs.AllocatedDeviceResource{Type: "gpu", Vendor: "nvidia", Name: "1080ti", DeviceIDs: []string{"dev0"}})
	greedyAlloc.NodeID = node.ID

	lowJob := greedyGPUJob(1, false)
	must.NoError(t, h.State.UpsertJob(structs.MsgTypeTestSetup, h.NextIndex(), nil, lowJob))
	lowAlloc := createAllocWithDevice(uuid.Generate(), lowJob, lowJob.TaskGroups[0].Tasks[0].Resources,
		&structs.AllocatedDeviceResource{Type: "gpu", Vendor: "nvidia", Name: "1080ti", DeviceIDs: []string{"dev1"}})
	lowAlloc.NodeID = node.ID

	must.NoError(t, h.State.UpsertAllocs(structs.MsgTypeTestSetup, h.NextIndex(), []*structs.Allocation{greedyAlloc, lowAlloc}))

	// New non-greedy job at priority 50. Either dev0 (via greedy eviction)
	// or dev1 (via general preemption of lowJob) would satisfy the GPU.
	// Greedy must win — it's free; the low-priority alloc must survive.
	newJob := greedyGPUJob(50, false)
	runJobEval(t, h, newJob)

	must.Len(t, 1, h.Plans)
	preempted := h.Plans[0].NodePreemptions[node.ID]
	must.Len(t, 1, preempted)
	must.Eq(t, greedyAlloc.ID, preempted[0].ID,
		must.Sprintf("zero-cost greedy must be preferred over low-priority non-greedy preemption"))
}

// TestPreemption_Greedy_DisabledByConfig verifies that when
// PreemptionConfig.GreedyPreemptionEnabled is false (default), the third pass
// does not fire and a greedy alloc is left alone — even when no other
// preemption mode is enabled.
func TestPreemption_Greedy_DisabledByConfig(t *testing.T) {
	ci.Parallel(t)
	h := NewHarness(t)

	node := greedyGPUNode(t, 1)
	must.NoError(t, h.State.UpsertNode(structs.MsgTypeTestSetup, h.NextIndex(), node))
	// Note: no enableGreedyPreemption — feature is OFF.

	greedyJob := greedyGPUJob(50, true)
	must.NoError(t, h.State.UpsertJob(structs.MsgTypeTestSetup, h.NextIndex(), nil, greedyJob))
	greedyAlloc := createAllocWithDevice(uuid.Generate(), greedyJob, greedyJob.TaskGroups[0].Tasks[0].Resources,
		&structs.AllocatedDeviceResource{Type: "gpu", Vendor: "nvidia", Name: "1080ti", DeviceIDs: []string{"dev0"}})
	greedyAlloc.NodeID = node.ID
	must.NoError(t, h.State.UpsertAllocs(structs.MsgTypeTestSetup, h.NextIndex(), []*structs.Allocation{greedyAlloc}))

	newJob := greedyGPUJob(50, false)
	runJobEval(t, h, newJob)

	if len(h.Plans) > 0 {
		must.Eq(t, 0, len(h.Plans[0].NodePreemptions),
			must.Sprintf("greedy preemption is disabled by config, expected no preemption; got %+v", h.Plans[0].NodePreemptions))
	}
}

// TestPreemption_Greedy_MultipleAllocsOnOneNode verifies multi-GPU eviction:
// 4 greedy allocs on a 4-GPU node, incoming alloc needs 2 GPUs → evict
// exactly 2, and the 2 evicted greedy allocs must be the ones holding the
// device instances the new alloc actually got assigned (not just "any 2").
func TestPreemption_Greedy_MultipleAllocsOnOneNode(t *testing.T) {
	ci.Parallel(t)
	h := NewHarness(t)

	node := greedyGPUNode(t, 4)
	must.NoError(t, h.State.UpsertNode(structs.MsgTypeTestSetup, h.NextIndex(), node))
	enableGreedyPreemption(t, h, structs.PreemptionConfig{})

	greedyJob := greedyGPUJob(50, true)
	greedyJob.TaskGroups[0].Count = 4
	must.NoError(t, h.State.UpsertJob(structs.MsgTypeTestSetup, h.NextIndex(), nil, greedyJob))
	allocByDeviceID := map[string]*structs.Allocation{}
	var greedyAllocs []*structs.Allocation
	for i := 0; i < 4; i++ {
		devID := fmt.Sprintf("dev%d", i)
		a := createAllocWithDevice(uuid.Generate(), greedyJob, greedyJob.TaskGroups[0].Tasks[0].Resources,
			&structs.AllocatedDeviceResource{Type: "gpu", Vendor: "nvidia", Name: "1080ti", DeviceIDs: []string{devID}})
		a.NodeID = node.ID
		greedyAllocs = append(greedyAllocs, a)
		allocByDeviceID[devID] = a
	}
	must.NoError(t, h.State.UpsertAllocs(structs.MsgTypeTestSetup, h.NextIndex(), greedyAllocs))

	// Incoming job wants 2 GPUs.
	newJob := greedyGPUJob(50, false)
	newJob.TaskGroups[0].Tasks[0].Resources.Devices[0].Count = 2
	runJobEval(t, h, newJob)

	must.Len(t, 1, h.Plans)
	preempted := h.Plans[0].NodePreemptions[node.ID]
	must.Len(t, 2, preempted,
		must.Sprintf("expected exactly 2 GPUs freed, got %d preemptions", len(preempted)))

	// The 2 evicted allocs must be exactly the holders of the 2 device IDs
	// the new alloc claimed.
	placed := h.Plans[0].NodeAllocation[node.ID]
	must.Len(t, 1, placed)
	var claimedDeviceIDs []string
	for _, tr := range placed[0].AllocatedResources.Tasks {
		for _, dev := range tr.Devices {
			claimedDeviceIDs = append(claimedDeviceIDs, dev.DeviceIDs...)
		}
	}
	must.Len(t, 2, claimedDeviceIDs)

	expectedVictims := map[string]struct{}{}
	for _, did := range claimedDeviceIDs {
		victim, ok := allocByDeviceID[did]
		must.True(t, ok, must.Sprintf("claimed device %s has no greedy holder", did))
		expectedVictims[victim.ID] = struct{}{}
	}
	actualVictims := map[string]struct{}{}
	for _, a := range preempted {
		actualVictims[a.ID] = struct{}{}
	}
	must.Eq(t, expectedVictims, actualVictims,
		must.Sprintf("evicted allocs must be the holders of the claimed device IDs"))
}

// TestPreemption_Greedy_PrefersGreedyNodeAsBestFit verifies the core zero-cost
// premise: greedy presence on a node does not push placement to a worse-fit
// alternative. With binpack scoring, a node with heavier non-greedy
// utilization (post-placement) scores higher; the greedy-occupied node
// should win on score and evict its greedy alloc instead of placing on an
// empty alternative that fits without preemption.
//
// Without zero-cost masking (v1 fallback), the empty node A would have won
// because it satisfied the request without any preemption.
func TestPreemption_Greedy_PrefersGreedyNodeAsBestFit(t *testing.T) {
	ci.Parallel(t)
	h := NewHarness(t)

	nodeA := greedyGPUNode(t, 1)
	nodeB := greedyGPUNode(t, 1)
	must.NoError(t, h.State.UpsertNode(structs.MsgTypeTestSetup, h.NextIndex(), nodeA))
	must.NoError(t, h.State.UpsertNode(structs.MsgTypeTestSetup, h.NextIndex(), nodeB))
	enableGreedyPreemption(t, h, structs.PreemptionConfig{})

	// Heavy non-greedy CPU consumer on B drives B's post-placement
	// utilization up. This non-greedy is NOT a preemption candidate (same
	// priority as the new job, no general preemption configured).
	cpuHogJob := nonGPUJob(50, false, 3000, 1024)
	must.NoError(t, h.State.UpsertJob(structs.MsgTypeTestSetup, h.NextIndex(), nil, cpuHogJob))
	cpuHogAlloc := createAlloc(uuid.Generate(), cpuHogJob, cpuHogJob.TaskGroups[0].Tasks[0].Resources)
	cpuHogAlloc.NodeID = nodeB.ID

	// Greedy alloc holding B's GPU. Under masking this is invisible to
	// device accounting on B.
	greedyJob := greedyGPUJob(50, true)
	must.NoError(t, h.State.UpsertJob(structs.MsgTypeTestSetup, h.NextIndex(), nil, greedyJob))
	greedyAlloc := createAllocWithDevice(uuid.Generate(), greedyJob, greedyJob.TaskGroups[0].Tasks[0].Resources,
		&structs.AllocatedDeviceResource{Type: "gpu", Vendor: "nvidia", Name: "1080ti", DeviceIDs: []string{"dev0"}})
	greedyAlloc.NodeID = nodeB.ID

	must.NoError(t, h.State.UpsertAllocs(structs.MsgTypeTestSetup, h.NextIndex(), []*structs.Allocation{cpuHogAlloc, greedyAlloc}))

	newJob := greedyGPUJob(50, false)
	runJobEval(t, h, newJob)

	must.Len(t, 1, h.Plans)
	plan := h.Plans[0]
	must.MapContainsKey(t, plan.NodeAllocation, nodeB.ID,
		must.Sprintf("expected placement on node B (better binpack fit), but plan landed on %v",
			mapKeys(plan.NodeAllocation)))
	preempted := plan.NodePreemptions[nodeB.ID]
	must.Len(t, 1, preempted)
	must.Eq(t, greedyAlloc.ID, preempted[0].ID)

	// Plan-applier validation: kept-non-greedy + new alloc must fit on B.
	mustPlanApplierFit(t, nodeB, plan, []*structs.Allocation{cpuHogAlloc, greedyAlloc})
}

// TestPreemption_Greedy_DoesNotEvictNonConflictingGreedy verifies that
// greedy allocs whose resources the new alloc does not claim are left
// intact. The node has two greedy allocs: A holds the only GPU; B is
// CPU-only and holds no device. A new GPU-requesting alloc must evict A
// (whose GPU it claims) but leave B running — B's resources do not overlap
// any claim, even though B is greedy.
//
// Note: we cannot reliably test "free GPU available, greedy GPU untouched"
// because the masked device allocator considers all instances free and
// may pick any of them — under zero-cost, that's by design (the greedy
// holder of whichever device gets picked is then evicted at zero cost).
func TestPreemption_Greedy_DoesNotEvictNonConflictingGreedy(t *testing.T) {
	ci.Parallel(t)
	h := NewHarness(t)

	node := greedyGPUNode(t, 1)
	must.NoError(t, h.State.UpsertNode(structs.MsgTypeTestSetup, h.NextIndex(), node))
	enableGreedyPreemption(t, h, structs.PreemptionConfig{})

	gpuJob := greedyGPUJob(50, true)
	must.NoError(t, h.State.UpsertJob(structs.MsgTypeTestSetup, h.NextIndex(), nil, gpuJob))
	gpuGreedy := createAllocWithDevice(uuid.Generate(), gpuJob, gpuJob.TaskGroups[0].Tasks[0].Resources,
		&structs.AllocatedDeviceResource{Type: "gpu", Vendor: "nvidia", Name: "1080ti", DeviceIDs: []string{"dev0"}})
	gpuGreedy.NodeID = node.ID

	// Second greedy alloc has no device and consumes a small slice of CPU/RAM.
	// The new alloc claims neither this alloc's CPU shares (they're not
	// reserved cores) nor any port — so cpuGreedy must survive.
	cpuJob := nonGPUJob(50, true, 500, 256)
	must.NoError(t, h.State.UpsertJob(structs.MsgTypeTestSetup, h.NextIndex(), nil, cpuJob))
	cpuGreedy := createAlloc(uuid.Generate(), cpuJob, cpuJob.TaskGroups[0].Tasks[0].Resources)
	cpuGreedy.NodeID = node.ID

	must.NoError(t, h.State.UpsertAllocs(structs.MsgTypeTestSetup, h.NextIndex(), []*structs.Allocation{gpuGreedy, cpuGreedy}))

	newJob := greedyGPUJob(50, false)
	runJobEval(t, h, newJob)

	must.Len(t, 1, h.Plans)
	plan := h.Plans[0]
	preempted := plan.NodePreemptions[node.ID]
	must.Len(t, 1, preempted, must.Sprintf("expected exactly one greedy eviction (the GPU holder)"))
	must.Eq(t, gpuGreedy.ID, preempted[0].ID,
		must.Sprintf("expected the GPU-holding greedy alloc evicted; the CPU-only greedy must survive"))

	mustPlanApplierFit(t, node, plan, []*structs.Allocation{gpuGreedy, cpuGreedy})
}

// TestPreemption_Greedy_SharedCPUResourceShortfall verifies the
// greedy-shortfall step: when claimed devices/ports/cores don't cover the
// shortfall but the new alloc still doesn't fit on the masked node,
// PreemptForTaskGroup runs against a greedy-only candidate set and evicts
// the smallest covering set of greedy allocs.
func TestPreemption_Greedy_SharedCPUResourceShortfall(t *testing.T) {
	ci.Parallel(t)
	h := NewHarness(t)

	node := greedyGPUNode(t, 0)
	must.NoError(t, h.State.UpsertNode(structs.MsgTypeTestSetup, h.NextIndex(), node))
	enableGreedyPreemption(t, h, structs.PreemptionConfig{})

	// 3 greedy allocs, each consuming 1000 CPU. Node total = 4000.
	greedyJob := nonGPUJob(50, true, 1000, 256)
	greedyJob.TaskGroups[0].Count = 3
	must.NoError(t, h.State.UpsertJob(structs.MsgTypeTestSetup, h.NextIndex(), nil, greedyJob))
	var greedyAllocs []*structs.Allocation
	for i := 0; i < 3; i++ {
		a := createAlloc(uuid.Generate(), greedyJob, greedyJob.TaskGroups[0].Tasks[0].Resources)
		a.NodeID = node.ID
		greedyAllocs = append(greedyAllocs, a)
	}
	must.NoError(t, h.State.UpsertAllocs(structs.MsgTypeTestSetup, h.NextIndex(), greedyAllocs))

	// Incoming non-greedy needs 2500 CPU. Free (non-greedy) on node is 4000;
	// no devices or ports are claimed. After kept-greedy is added back to
	// the fit check, total = 3000 (greedy) + 2500 (new) = 5500 > 4000 →
	// AllocsFit fails → greedy-shortfall step evicts exactly 2 of the 3
	// greedy allocs.
	newJob := nonGPUJob(50, false, 2500, 256)
	runJobEval(t, h, newJob)

	must.Len(t, 1, h.Plans)
	plan := h.Plans[0]
	must.MapContainsKey(t, plan.NodeAllocation, node.ID)
	preempted := plan.NodePreemptions[node.ID]
	must.Len(t, 2, preempted,
		must.Sprintf("expected exactly 2 greedy evictions to cover the CPU shortfall"))

	mustPlanApplierFit(t, node, plan, greedyAllocs)
}

// TestPreemption_Greedy_FallbackToGeneralPreemptionWhenNoGreedyCovers
// verifies that when greedy alone can't cover the demand (device + RAM),
// general preemption runs to evict a delta-eligible non-greedy victim AND
// the greedy victim from the masking step is still included.
func TestPreemption_Greedy_FallbackToGeneralPreemptionWhenNoGreedyCovers(t *testing.T) {
	ci.Parallel(t)
	h := NewHarness(t)

	node := greedyGPUNode(t, 1)
	must.NoError(t, h.State.UpsertNode(structs.MsgTypeTestSetup, h.NextIndex(), node))
	// Both feature flags on — general preemption is required to evict
	// the low-priority non-greedy alloc.
	enableGreedyPreemption(t, h, structs.PreemptionConfig{ServiceSchedulerEnabled: true})

	greedyJob := greedyGPUJob(50, true)
	must.NoError(t, h.State.UpsertJob(structs.MsgTypeTestSetup, h.NextIndex(), nil, greedyJob))
	greedyAlloc := createAllocWithDevice(uuid.Generate(), greedyJob, greedyJob.TaskGroups[0].Tasks[0].Resources,
		&structs.AllocatedDeviceResource{Type: "gpu", Vendor: "nvidia", Name: "1080ti", DeviceIDs: []string{"dev0"}})
	greedyAlloc.NodeID = node.ID

	// Low-priority non-greedy hogging most of the RAM. New alloc's RAM ask
	// is larger than what greedy alone holds, so general preemption must
	// also evict this one.
	lowPriJob := nonGPUJob(1, false, 1000, 6000)
	must.NoError(t, h.State.UpsertJob(structs.MsgTypeTestSetup, h.NextIndex(), nil, lowPriJob))
	lowPriAlloc := createAlloc(uuid.Generate(), lowPriJob, lowPriJob.TaskGroups[0].Tasks[0].Resources)
	lowPriAlloc.NodeID = node.ID

	must.NoError(t, h.State.UpsertAllocs(structs.MsgTypeTestSetup, h.NextIndex(), []*structs.Allocation{greedyAlloc, lowPriAlloc}))

	// New alloc wants GPU + 5000 MB RAM. Greedy holds 256 MB; lowPri holds
	// 6000 MB. Without lowPri eviction, RAM doesn't fit.
	newJob := greedyGPUJob(50, false)
	newJob.TaskGroups[0].Tasks[0].Resources.MemoryMB = 5000
	runJobEval(t, h, newJob)

	must.Len(t, 1, h.Plans)
	plan := h.Plans[0]
	preempted := plan.NodePreemptions[node.ID]
	must.Len(t, 2, preempted,
		must.Sprintf("expected greedy + low-priority both evicted; got %d", len(preempted)))
	ids := map[string]struct{}{}
	for _, a := range preempted {
		ids[a.ID] = struct{}{}
	}
	must.MapContainsKey(t, ids, greedyAlloc.ID,
		must.Sprintf("greedy must be evicted (it holds the GPU)"))
	must.MapContainsKey(t, ids, lowPriAlloc.ID,
		must.Sprintf("low-priority non-greedy must be evicted via general preemption fallback"))

	mustPlanApplierFit(t, node, plan, []*structs.Allocation{greedyAlloc, lowPriAlloc})
}

// TestPreemption_Greedy_ScoringNotAffectedByGreedyPresence verifies the
// "zero-cost" invariant on scoring: a node carrying a greedy alloc must
// score the same (on the "binpack" component) as an identical node without
// any greedy alloc, because masking removes greedy from resource accounting
// before scoring.
func TestPreemption_Greedy_ScoringNotAffectedByGreedyPresence(t *testing.T) {
	ci.Parallel(t)
	h := NewHarness(t)

	nodeA := greedyGPUNode(t, 1)
	nodeB := greedyGPUNode(t, 1)
	must.NoError(t, h.State.UpsertNode(structs.MsgTypeTestSetup, h.NextIndex(), nodeA))
	must.NoError(t, h.State.UpsertNode(structs.MsgTypeTestSetup, h.NextIndex(), nodeB))
	enableGreedyPreemption(t, h, structs.PreemptionConfig{})

	// Only B has a greedy alloc holding its GPU.
	greedyJob := greedyGPUJob(50, true)
	must.NoError(t, h.State.UpsertJob(structs.MsgTypeTestSetup, h.NextIndex(), nil, greedyJob))
	greedyAlloc := createAllocWithDevice(uuid.Generate(), greedyJob, greedyJob.TaskGroups[0].Tasks[0].Resources,
		&structs.AllocatedDeviceResource{Type: "gpu", Vendor: "nvidia", Name: "1080ti", DeviceIDs: []string{"dev0"}})
	greedyAlloc.NodeID = nodeB.ID
	must.NoError(t, h.State.UpsertAllocs(structs.MsgTypeTestSetup, h.NextIndex(), []*structs.Allocation{greedyAlloc}))

	newJob := greedyGPUJob(50, false)
	runJobEval(t, h, newJob)

	must.Len(t, 1, h.Plans)
	plan := h.Plans[0]

	// Find the placed alloc to inspect its Metrics.
	placed := firstPlacedAlloc(plan)
	must.NotNil(t, placed)
	must.NotNil(t, placed.Metrics)

	scoreA, hasA := binpackScoreForNode(placed.Metrics, nodeA.ID)
	scoreB, hasB := binpackScoreForNode(placed.Metrics, nodeB.ID)
	must.True(t, hasA, must.Sprintf("expected score metadata for node A; got %+v", placed.Metrics.ScoreMetaData))
	must.True(t, hasB, must.Sprintf("expected score metadata for node B; got %+v", placed.Metrics.ScoreMetaData))
	must.Eq(t, scoreA, scoreB,
		must.Sprintf("greedy presence on B must not change its binpack score relative to identical-but-empty A"))
}

// TestPreemption_Greedy_DistinctHostsStillBlocksPlacement verifies that
// masking applies only to resource accounting — feasibility iterators
// (host_volume, distinct_hosts, distinct_property) still see greedy
// allocs. A non-greedy job with distinct_hosts is blocked from placing a
// second alloc on a node where ANY alloc from the same job already exists,
// greedy or not.
func TestPreemption_Greedy_DistinctHostsStillBlocksPlacement(t *testing.T) {
	ci.Parallel(t)
	h := NewHarness(t)

	// Two GPU nodes. A non-greedy job with count=2 + distinct_hosts wants
	// both allocs to land. With masking only and no enforcement of
	// feasibility, both might try to land on the same node. Feasibility
	// should split them.
	nodeA := greedyGPUNode(t, 1)
	nodeB := greedyGPUNode(t, 1)
	must.NoError(t, h.State.UpsertNode(structs.MsgTypeTestSetup, h.NextIndex(), nodeA))
	must.NoError(t, h.State.UpsertNode(structs.MsgTypeTestSetup, h.NextIndex(), nodeB))
	enableGreedyPreemption(t, h, structs.PreemptionConfig{})

	newJob := greedyGPUJob(50, false)
	newJob.TaskGroups[0].Count = 2
	newJob.Constraints = append(newJob.Constraints, &structs.Constraint{
		Operand: structs.ConstraintDistinctHosts,
	})
	runJobEval(t, h, newJob)

	must.Len(t, 1, h.Plans)
	plan := h.Plans[0]
	placedOnA := len(plan.NodeAllocation[nodeA.ID])
	placedOnB := len(plan.NodeAllocation[nodeB.ID])
	must.Eq(t, 1, placedOnA, must.Sprintf("distinct_hosts: expected exactly one alloc on A"))
	must.Eq(t, 1, placedOnB, must.Sprintf("distinct_hosts: expected exactly one alloc on B"))
}

// TestPreemption_Greedy_ScoringIgnoresKeptGreedyCPU verifies the zero-cost
// invariant on scoring even when greedy allocs SURVIVE masking. Two
// identical empty nodes A and B. Node B carries a CPU-only greedy alloc
// (no device, no port — guaranteed non-conflicting with the new GPU
// alloc). When the new alloc lands, the CPU-only greedy is kept, but its
// CPU/RAM must NOT inflate node B's binpack score.
//
// Without Fix 1, the kept-greedy resources are appended to `proposed`
// before AllocsFit and the returned `util` (used for scoreFit) includes
// them — making B score higher than A and breaking placement neutrality.
func TestPreemption_Greedy_ScoringIgnoresKeptGreedyCPU(t *testing.T) {
	ci.Parallel(t)
	h := NewHarness(t)

	nodeA := greedyGPUNode(t, 1)
	nodeB := greedyGPUNode(t, 1)
	must.NoError(t, h.State.UpsertNode(structs.MsgTypeTestSetup, h.NextIndex(), nodeA))
	must.NoError(t, h.State.UpsertNode(structs.MsgTypeTestSetup, h.NextIndex(), nodeB))
	enableGreedyPreemption(t, h, structs.PreemptionConfig{})

	// CPU-only greedy on B with substantial CPU/RAM — large enough that
	// without masking, it would clearly tilt the binpack score.
	cpuGreedy := nonGPUJob(50, true, 2000, 4096)
	must.NoError(t, h.State.UpsertJob(structs.MsgTypeTestSetup, h.NextIndex(), nil, cpuGreedy))
	cpuGreedyAlloc := createAlloc(uuid.Generate(), cpuGreedy, cpuGreedy.TaskGroups[0].Tasks[0].Resources)
	cpuGreedyAlloc.NodeID = nodeB.ID
	must.NoError(t, h.State.UpsertAllocs(structs.MsgTypeTestSetup, h.NextIndex(), []*structs.Allocation{cpuGreedyAlloc}))

	newJob := greedyGPUJob(50, false)
	runJobEval(t, h, newJob)

	must.Len(t, 1, h.Plans)
	plan := h.Plans[0]

	// Find the placed alloc to inspect its Metrics.
	placed := firstPlacedAlloc(plan)
	must.NotNil(t, placed)
	must.NotNil(t, placed.Metrics)

	scoreA, hasA := binpackScoreForNode(placed.Metrics, nodeA.ID)
	scoreB, hasB := binpackScoreForNode(placed.Metrics, nodeB.ID)
	must.True(t, hasA, must.Sprintf("expected score metadata for node A"))
	must.True(t, hasB, must.Sprintf("expected score metadata for node B"))
	must.Eq(t, scoreA, scoreB,
		must.Sprintf("kept-greedy CPU/RAM on B must not change its binpack score relative to identical empty A"))

	// Sanity: cpuGreedy must be in the surviving set (not evicted), since
	// it holds no resource the new alloc claims.
	for _, victim := range plan.NodePreemptions[nodeB.ID] {
		must.NotEq(t, cpuGreedyAlloc.ID, victim.ID,
			must.Sprintf("CPU-only greedy must survive — the new alloc didn't claim its CPU"))
	}
}

// TestPreemption_Greedy_NodeMaxAllocsEvictsLowestPriority verifies that
// when NodeMaxAllocs would be exceeded by the kept-greedy + new alloc
// count, the masking step evicts additional greedy allocs (lowest
// priority first) to free slots.
//
// Critical to this test: the count must be over by *more than one*. The
// greedy-shortfall fallback (PreemptForTaskGroup) will opportunistically
// evict ONE greedy alloc as a side effect of its algorithm even when no
// resource shortfall actually exists; that masks any over-by-1 bug. Here
// we set up over-by-2 so that without the enforceNodeMaxAllocs step,
// greedy-shortfall would evict 1 (and still leave the count over), and
// the node would be marked exhausted.
func TestPreemption_Greedy_NodeMaxAllocsEvictsLowestPriority(t *testing.T) {
	ci.Parallel(t)
	h := NewHarness(t)

	node := greedyGPUNode(t, 0)
	node.NodeMaxAllocs = 3
	must.NoError(t, h.State.UpsertNode(structs.MsgTypeTestSetup, h.NextIndex(), node))
	enableGreedyPreemption(t, h, structs.PreemptionConfig{})

	// 1 non-greedy + 3 greedy already on node. New non-greedy would push
	// count to 5; the limit is 3, so over by 2. None claim overlapping
	// device/port/core resources — the count is the only blocker.
	nonGreedyJob := nonGPUJob(50, false, 500, 256)
	must.NoError(t, h.State.UpsertJob(structs.MsgTypeTestSetup, h.NextIndex(), nil, nonGreedyJob))
	nonGreedyAlloc := createAlloc(uuid.Generate(), nonGreedyJob, nonGreedyJob.TaskGroups[0].Tasks[0].Resources)
	nonGreedyAlloc.NodeID = node.ID

	g30Job := nonGPUJob(30, true, 500, 256)
	must.NoError(t, h.State.UpsertJob(structs.MsgTypeTestSetup, h.NextIndex(), nil, g30Job))
	g30Alloc := createAlloc(uuid.Generate(), g30Job, g30Job.TaskGroups[0].Tasks[0].Resources)
	g30Alloc.NodeID = node.ID

	g50Job := nonGPUJob(50, true, 500, 256)
	must.NoError(t, h.State.UpsertJob(structs.MsgTypeTestSetup, h.NextIndex(), nil, g50Job))
	g50Alloc := createAlloc(uuid.Generate(), g50Job, g50Job.TaskGroups[0].Tasks[0].Resources)
	g50Alloc.NodeID = node.ID

	g70Job := nonGPUJob(70, true, 500, 256)
	must.NoError(t, h.State.UpsertJob(structs.MsgTypeTestSetup, h.NextIndex(), nil, g70Job))
	g70Alloc := createAlloc(uuid.Generate(), g70Job, g70Job.TaskGroups[0].Tasks[0].Resources)
	g70Alloc.NodeID = node.ID

	must.NoError(t, h.State.UpsertAllocs(structs.MsgTypeTestSetup, h.NextIndex(),
		[]*structs.Allocation{nonGreedyAlloc, g30Alloc, g50Alloc, g70Alloc}))

	newJob := nonGPUJob(50, false, 500, 256)
	runJobEval(t, h, newJob)

	must.Len(t, 1, h.Plans)
	plan := h.Plans[0]
	must.MapContainsKey(t, plan.NodeAllocation, node.ID,
		must.Sprintf("new alloc should be placed on the node after enforcing NodeMaxAllocs"))
	preempted := plan.NodePreemptions[node.ID]
	must.Len(t, 2, preempted, must.Sprintf("expected exactly 2 greedy victims to free 2 slots (count over by 2)"))
	ids := map[string]struct{}{}
	for _, a := range preempted {
		ids[a.ID] = struct{}{}
	}
	// The two lowest-priority greedy allocs must be the victims.
	must.MapContainsKey(t, ids, g30Alloc.ID, must.Sprintf("priority-30 greedy must be evicted"))
	must.MapContainsKey(t, ids, g50Alloc.ID, must.Sprintf("priority-50 greedy must be evicted"))

	mustPlanApplierFit(t, node, plan, []*structs.Allocation{nonGreedyAlloc, g30Alloc, g50Alloc, g70Alloc})
}

// TestPreemption_Greedy_NodeMaxAllocsAllGreedyEvictedWhenStillOver
// verifies that when non-greedy occupancy alone exceeds NodeMaxAllocs,
// the helper still evicts every kept-greedy (a defensible best effort)
// but the downstream AllocsFit reports genuine exhaustion.
func TestPreemption_Greedy_NodeMaxAllocsAllGreedyEvictedWhenStillOver(t *testing.T) {
	ci.Parallel(t)
	h := NewHarness(t)

	node := greedyGPUNode(t, 0)
	node.NodeMaxAllocs = 2
	must.NoError(t, h.State.UpsertNode(structs.MsgTypeTestSetup, h.NextIndex(), node))
	enableGreedyPreemption(t, h, structs.PreemptionConfig{})

	// 2 non-greedy + 2 greedy already on node. Non-greedy alone equals
	// NodeMaxAllocs; new alloc cannot land even with all greedy evicted.
	n1Job := nonGPUJob(50, false, 500, 256)
	must.NoError(t, h.State.UpsertJob(structs.MsgTypeTestSetup, h.NextIndex(), nil, n1Job))
	n1 := createAlloc(uuid.Generate(), n1Job, n1Job.TaskGroups[0].Tasks[0].Resources)
	n1.NodeID = node.ID
	n2Job := nonGPUJob(50, false, 500, 256)
	must.NoError(t, h.State.UpsertJob(structs.MsgTypeTestSetup, h.NextIndex(), nil, n2Job))
	n2 := createAlloc(uuid.Generate(), n2Job, n2Job.TaskGroups[0].Tasks[0].Resources)
	n2.NodeID = node.ID

	g1Job := nonGPUJob(50, true, 500, 256)
	must.NoError(t, h.State.UpsertJob(structs.MsgTypeTestSetup, h.NextIndex(), nil, g1Job))
	g1 := createAlloc(uuid.Generate(), g1Job, g1Job.TaskGroups[0].Tasks[0].Resources)
	g1.NodeID = node.ID
	g2Job := nonGPUJob(50, true, 500, 256)
	must.NoError(t, h.State.UpsertJob(structs.MsgTypeTestSetup, h.NextIndex(), nil, g2Job))
	g2 := createAlloc(uuid.Generate(), g2Job, g2Job.TaskGroups[0].Tasks[0].Resources)
	g2.NodeID = node.ID
	must.NoError(t, h.State.UpsertAllocs(structs.MsgTypeTestSetup, h.NextIndex(),
		[]*structs.Allocation{n1, n2, g1, g2}))

	newJob := nonGPUJob(50, false, 500, 256)
	runJobEval(t, h, newJob)

	// Placement must NOT happen on this node — non-greedy alone is at the
	// alloc-count limit. The scheduler may still create a plan with the
	// new alloc somewhere else (no other nodes here) or no plan at all.
	for _, plan := range h.Plans {
		must.Eq(t, 0, len(plan.NodeAllocation[node.ID]),
			must.Sprintf("must not place on a node whose non-greedy count already meets NodeMaxAllocs"))
	}
}

// TestPreemption_Greedy_EvictionEarnsNoPreemptionReward verifies the zero-cost
// invariant on the preemption scorer: evicting a masked greedy alloc must not
// add a preemption reward to the node's score. The upstream
// PreemptionScoringIterator rewards low-priority evictions with a score near
// 1, which — because greedy masking runs in the first scheduling pass rather
// than a separate preemption fallback — would structurally bias placement
// toward greedy-occupied nodes. A greedy-only eviction must therefore record
// no "preemption" score at all.
func TestPreemption_Greedy_EvictionEarnsNoPreemptionReward(t *testing.T) {
	ci.Parallel(t)
	h := NewHarness(t)

	node := greedyGPUNode(t, 1)
	must.NoError(t, h.State.UpsertNode(structs.MsgTypeTestSetup, h.NextIndex(), node))
	enableGreedyPreemption(t, h, structs.PreemptionConfig{})

	// Greedy alloc (priority 1) holds the only GPU. Under masking it is
	// invisible to device accounting, so the incoming alloc claims the GPU
	// and evicts this greedy holder.
	greedyJob := greedyGPUJob(1, true)
	must.NoError(t, h.State.UpsertJob(structs.MsgTypeTestSetup, h.NextIndex(), nil, greedyJob))
	greedyAlloc := createAllocWithDevice(uuid.Generate(), greedyJob, greedyJob.TaskGroups[0].Tasks[0].Resources,
		&structs.AllocatedDeviceResource{Type: "gpu", Vendor: "nvidia", Name: "1080ti", DeviceIDs: []string{"dev0"}})
	greedyAlloc.NodeID = node.ID
	must.NoError(t, h.State.UpsertAllocs(structs.MsgTypeTestSetup, h.NextIndex(), []*structs.Allocation{greedyAlloc}))

	newJob := greedyGPUJob(50, false)
	runJobEval(t, h, newJob)

	must.Len(t, 1, h.Plans)
	plan := h.Plans[0]

	// The greedy holder must have been evicted to free its GPU.
	preempted := plan.NodePreemptions[node.ID]
	must.Len(t, 1, preempted)
	must.Eq(t, greedyAlloc.ID, preempted[0].ID)

	// But that eviction must not have earned a preemption reward.
	placed := firstPlacedAlloc(plan)
	must.NotNil(t, placed)
	must.NotNil(t, placed.Metrics)
	_, hasPreempt := scoreForNode(placed.Metrics, node.ID, "preemption")
	must.False(t, hasPreempt,
		must.Sprintf("greedy eviction must not record a preemption reward; scores: %+v",
			placed.Metrics.ScoreMetaData))
}

// TestPreemption_Greedy_NeutralPrefersEvictionFreeNode verifies the
// behavioral consequence of zero-cost scoring: given a node with a free GPU
// (no eviction needed) and a node whose only GPU is held by a greedy alloc,
// the scheduler prefers the eviction-free node rather than being lured into
// evicting greedy by the preemption reward. Node A carries a small non-greedy
// filler so it is a marginally better bin-pack fit; without the fix, node B's
// preemption reward overpowers that edge and placement lands on B.
func TestPreemption_Greedy_NeutralPrefersEvictionFreeNode(t *testing.T) {
	ci.Parallel(t)
	h := NewHarness(t)

	nodeA := greedyGPUNode(t, 1) // free GPU, eviction-free
	nodeB := greedyGPUNode(t, 1) // GPU held by a greedy alloc
	must.NoError(t, h.State.UpsertNode(structs.MsgTypeTestSetup, h.NextIndex(), nodeA))
	must.NoError(t, h.State.UpsertNode(structs.MsgTypeTestSetup, h.NextIndex(), nodeB))
	enableGreedyPreemption(t, h, structs.PreemptionConfig{})

	// Small non-greedy filler on A makes it a marginally denser (better)
	// bin-pack fit. It is not a preemption candidate (same priority, no
	// general preemption configured).
	fillerJob := nonGPUJob(50, false, 1000, 512)
	must.NoError(t, h.State.UpsertJob(structs.MsgTypeTestSetup, h.NextIndex(), nil, fillerJob))
	fillerAlloc := createAlloc(uuid.Generate(), fillerJob, fillerJob.TaskGroups[0].Tasks[0].Resources)
	fillerAlloc.NodeID = nodeA.ID

	// Greedy alloc (priority 1) holds B's GPU.
	greedyJob := greedyGPUJob(1, true)
	must.NoError(t, h.State.UpsertJob(structs.MsgTypeTestSetup, h.NextIndex(), nil, greedyJob))
	greedyAlloc := createAllocWithDevice(uuid.Generate(), greedyJob, greedyJob.TaskGroups[0].Tasks[0].Resources,
		&structs.AllocatedDeviceResource{Type: "gpu", Vendor: "nvidia", Name: "1080ti", DeviceIDs: []string{"dev0"}})
	greedyAlloc.NodeID = nodeB.ID

	must.NoError(t, h.State.UpsertAllocs(structs.MsgTypeTestSetup, h.NextIndex(), []*structs.Allocation{fillerAlloc, greedyAlloc}))

	newJob := greedyGPUJob(50, false)
	runJobEval(t, h, newJob)

	must.Len(t, 1, h.Plans)
	plan := h.Plans[0]
	must.MapContainsKey(t, plan.NodeAllocation, nodeA.ID,
		must.Sprintf("expected placement on eviction-free node A, but plan landed on %v",
			mapKeys(plan.NodeAllocation)))
	must.Len(t, 0, plan.NodePreemptions[nodeB.ID],
		must.Sprintf("must not evict greedy on B when an eviction-free node is available"))
}

// nonGPUJob builds a job (greedy or non-greedy) that asks only for CPU and
// memory, with no devices or networks. Used to build filler non-greedy
// load, low-priority preemption victims, and CPU-only greedy allocs in
// the new test scenarios.
func nonGPUJob(priority int, greedy bool, cpu, memoryMB int) *structs.Job {
	j := mock.Job()
	j.Priority = priority
	j.TaskGroups[0].Count = 1
	j.TaskGroups[0].Networks = nil
	j.TaskGroups[0].Tasks[0].Services = nil
	j.TaskGroups[0].Tasks[0].Resources.Networks = nil
	j.TaskGroups[0].Tasks[0].Resources.CPU = cpu
	j.TaskGroups[0].Tasks[0].Resources.MemoryMB = memoryMB
	if greedy {
		if j.Meta == nil {
			j.Meta = map[string]string{}
		}
		j.Meta[structs.JobMetaGreedy] = "true"
	}
	return j
}

// mustPlanApplierFit asserts the result of an AllocsFit check that mirrors
// what plan_apply.go validates: the node, minus preempted allocs, plus the
// new placements, must fit under (and not collide on devices).
func mustPlanApplierFit(t *testing.T, node *structs.Node, plan *structs.Plan, existing []*structs.Allocation) {
	t.Helper()
	preempted := map[string]struct{}{}
	for _, a := range plan.NodePreemptions[node.ID] {
		preempted[a.ID] = struct{}{}
	}
	var allocs []*structs.Allocation
	for _, a := range existing {
		if _, evicted := preempted[a.ID]; evicted {
			continue
		}
		allocs = append(allocs, a)
	}
	allocs = append(allocs, plan.NodeAllocation[node.ID]...)
	fit, dim, _, err := structs.AllocsFit(node, allocs, nil, true)
	must.NoError(t, err)
	must.True(t, fit,
		must.Sprintf("post-plan AllocsFit failed on dimension %q for node %s — masking under-evicted",
			dim, node.ID))
}

// binpackScoreForNode returns the binpack score for the given node from a
// metric's ScoreMetaData, plus a bool indicating whether the entry exists.
// scoreForNode returns the named scorer's value for a node and whether that
// scorer recorded a score (the bool reflects key presence).
func scoreForNode(m *structs.AllocMetric, nodeID, key string) (float64, bool) {
	for _, sm := range m.ScoreMetaData {
		if sm.NodeID == nodeID {
			v, ok := sm.Scores[key]
			return v, ok
		}
	}
	return 0, false
}

func binpackScoreForNode(m *structs.AllocMetric, nodeID string) (float64, bool) {
	return scoreForNode(m, nodeID, "binpack")
}

// firstPlacedAlloc returns any one placed alloc from the plan, for inspecting
// its scoring Metrics.
func firstPlacedAlloc(plan *structs.Plan) *structs.Allocation {
	for _, allocs := range plan.NodeAllocation {
		if len(allocs) > 0 {
			return allocs[0]
		}
	}
	return nil
}

// mapKeys returns the keys of a map (useful for diagnostic must.Sprintf
// output).
func mapKeys[K comparable, V any](m map[K]V) []K {
	out := make([]K, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

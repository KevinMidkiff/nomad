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
		j.Meta[JobMetaGreedy] = "true"
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

// TestPreemption_Greedy_GeneralPreemptionTakesPrecedence verifies ordering of
// passes: when general preemption is enabled AND there's a delta-eligible
// non-greedy victim, the general pass wins before greedy mode is even tried.
func TestPreemption_Greedy_GeneralPreemptionTakesPrecedence(t *testing.T) {
	ci.Parallel(t)
	h := NewHarness(t)

	node := greedyGPUNode(t, 2)
	must.NoError(t, h.State.UpsertNode(structs.MsgTypeTestSetup, h.NextIndex(), node))

	enableGreedyPreemption(t, h, structs.PreemptionConfig{ServiceSchedulerEnabled: true})

	// One greedy alloc at priority 50, one low-priority non-greedy alloc.
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

	// New job at priority 50 — delta of 49 vs lowJob makes the non-greedy
	// alloc preemptible via the general pass. Greedy alloc must be untouched.
	newJob := greedyGPUJob(50, false)
	runJobEval(t, h, newJob)

	must.Len(t, 1, h.Plans)
	preempted := h.Plans[0].NodePreemptions[node.ID]
	must.Len(t, 1, preempted)
	must.Eq(t, lowAlloc.ID, preempted[0].ID,
		must.Sprintf("general preemption should evict the low-priority alloc, not the greedy one"))
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
// 4 greedy allocs on a 4-GPU node, incoming alloc needs 2 GPUs → evict 2.
func TestPreemption_Greedy_MultipleAllocsOnOneNode(t *testing.T) {
	ci.Parallel(t)
	h := NewHarness(t)

	node := greedyGPUNode(t, 4)
	must.NoError(t, h.State.UpsertNode(structs.MsgTypeTestSetup, h.NextIndex(), node))
	enableGreedyPreemption(t, h, structs.PreemptionConfig{})

	greedyJob := greedyGPUJob(50, true)
	greedyJob.TaskGroups[0].Count = 4
	must.NoError(t, h.State.UpsertJob(structs.MsgTypeTestSetup, h.NextIndex(), nil, greedyJob))
	var greedyAllocs []*structs.Allocation
	for i := 0; i < 4; i++ {
		a := createAllocWithDevice(uuid.Generate(), greedyJob, greedyJob.TaskGroups[0].Tasks[0].Resources,
			&structs.AllocatedDeviceResource{Type: "gpu", Vendor: "nvidia", Name: "1080ti", DeviceIDs: []string{fmt.Sprintf("dev%d", i)}})
		a.NodeID = node.ID
		greedyAllocs = append(greedyAllocs, a)
	}
	must.NoError(t, h.State.UpsertAllocs(structs.MsgTypeTestSetup, h.NextIndex(), greedyAllocs))

	// Incoming job wants 2 GPUs.
	newJob := greedyGPUJob(50, false)
	newJob.TaskGroups[0].Tasks[0].Resources.Devices[0].Count = 2
	runJobEval(t, h, newJob)

	must.Len(t, 1, h.Plans)
	preempted := h.Plans[0].NodePreemptions[node.ID]
	must.Eq(t, 2, len(preempted),
		must.Sprintf("expected exactly 2 GPUs freed, got %d preemptions", len(preempted)))
}

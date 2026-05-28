// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package scheduler

import (
	"github.com/hashicorp/nomad/nomad/structs"
)

const (
	gpuDeviceType = "gpu"

	gpuReservedCPUExhaustion    = "gpu reserved cpu"
	gpuReservedMemoryExhaustion = "gpu reserved memory"
)

type gpuReservationRequirement struct {
	cpuCores int
	memoryMB int64
	freeGPUs int
}

func taskGroupRequestsGPUDevice(tg *structs.TaskGroup) bool {
	if tg == nil {
		return false
	}

	for _, task := range tg.Tasks {
		if task == nil || task.Resources == nil {
			continue
		}

		for _, device := range task.Resources.Devices {
			if device == nil {
				continue
			}
			id := device.ID()
			if id != nil && id.Type == gpuDeviceType {
				return true
			}
		}
	}

	return false
}

// AllocationUsesGPUDevice reports whether an allocation has an assigned GPU
// device. It is used by the plan applier, where the task group request is no
// longer the most direct source of truth.
func AllocationUsesGPUDevice(alloc *structs.Allocation) bool {
	if alloc == nil || alloc.AllocatedResources == nil {
		return false
	}

	for _, task := range alloc.AllocatedResources.Tasks {
		if task == nil {
			continue
		}
		for _, device := range task.Devices {
			if device == nil {
				continue
			}
			id := device.ID()
			if id != nil && id.Type == gpuDeviceType {
				return true
			}
		}
	}

	return false
}

// AllocationsContainCPUOnlyPlacement reports whether a placement set includes
// any non-terminal allocation that does not use a GPU device.
func AllocationsContainCPUOnlyPlacement(allocs []*structs.Allocation) bool {
	for _, alloc := range allocs {
		if alloc == nil || alloc.ClientTerminalStatus() {
			continue
		}
		if !AllocationUsesGPUDevice(alloc) {
			return true
		}
	}

	return false
}

func remainingHealthyGPUCount(node *structs.Node, allocs []*structs.Allocation) int {
	return gpuReservationRequired(node, allocs, structs.SchedulerGPUResourceReservation{}).freeGPUs
}

func gpuReservationRequired(
	node *structs.Node,
	allocs []*structs.Allocation,
	cfg structs.SchedulerGPUResourceReservation,
) gpuReservationRequirement {
	var required gpuReservationRequirement
	if node == nil || node.NodeResources == nil {
		return required
	}

	accounter := structs.NewDeviceAccounter(node)
	accounter.AddAllocs(allocs)

	for id, device := range accounter.Devices {
		if id.Type != gpuDeviceType {
			continue
		}
		reservation := gpuReservationForDevice(id, cfg)
		for _, uses := range device.Instances {
			if uses == 0 {
				required.freeGPUs++
				required.cpuCores += reservation.CPUCores
				required.memoryMB += int64(reservation.MemoryMB)
			}
		}
	}

	return required
}

func gpuReservationForDevice(
	id structs.DeviceIdTuple,
	cfg structs.SchedulerGPUResourceReservation,
) structs.SchedulerGPUResourceReservationDevice {
	reservation := structs.SchedulerGPUResourceReservationDevice{
		Type: gpuDeviceType,
	}
	bestSpecificity := -1

	for _, deviceReservation := range cfg.DeviceReservations {
		if deviceReservation == nil {
			continue
		}
		ruleID := deviceReservation.ID()
		if !id.Matches(ruleID) {
			continue
		}
		specificity := deviceReservation.Specificity()
		if specificity > bestSpecificity {
			bestSpecificity = specificity
			reservation = *deviceReservation
		}
	}

	return reservation
}

func gpuReservationCPUShares(
	node *structs.Node,
	available *structs.ComparableResources,
	coresPerGPU, freeGPUs int,
) int64 {
	if coresPerGPU <= 0 || freeGPUs <= 0 {
		return 0
	}
	if node == nil || node.NodeResources == nil ||
		node.NodeResources.Processors.Topology == nil || available == nil {
		return -1
	}

	if available.Flattened.Cpu.CpuShares <= 0 {
		return -1
	}

	// NodeResources.Comparable stores the node's reservable topology cores in
	// ReservedCores. After subtracting NodeReservedResources, this field holds
	// the schedulable core set despite the allocation-oriented field name.
	schedulableCoreCount := int64(len(available.Flattened.Cpu.ReservedCores))
	if schedulableCoreCount == 0 {
		return -1
	}

	requiredCores := int64(coresPerGPU * freeGPUs)
	availableCPU := available.Flattened.Cpu.CpuShares

	return (availableCPU*requiredCores + schedulableCoreCount - 1) / schedulableCoreCount
}

// GPUReservationViolated reports whether the final allocation set consumes
// CPU or memory capacity reserved for remaining healthy free GPUs.
func GPUReservationViolated(
	node *structs.Node,
	finalAllocs []*structs.Allocation,
	cfg structs.SchedulerGPUResourceReservation,
) (bool, string) {
	if cfg.IsZero() {
		return false, ""
	}

	required := gpuReservationRequired(node, finalAllocs, cfg)
	if required.cpuCores == 0 && required.memoryMB == 0 {
		return false, ""
	}

	used := gpuReservationResourcesUsed(finalAllocs)

	if required.cpuCores > 0 {
		available := gpuReservationAvailableComparable(node)
		reservedCPU := gpuReservationCPUShares(node, available, required.cpuCores, 1)
		if reservedCPU < 0 {
			return true, gpuReservedCPUExhaustion
		}

		remainingCPU := available.Flattened.Cpu.CpuShares - used.Flattened.Cpu.CpuShares
		if remainingCPU < reservedCPU {
			return true, gpuReservedCPUExhaustion
		}
	}

	if required.memoryMB > 0 {
		availableMemory, ok := gpuReservationAvailableMemoryMB(node)
		if !ok {
			return true, gpuReservedMemoryExhaustion
		}

		remainingMemory := availableMemory - used.Flattened.Memory.MemoryMB
		if remainingMemory < required.memoryMB {
			return true, gpuReservedMemoryExhaustion
		}
	}

	return false, ""
}

func gpuReservationCannotCompute(
	node *structs.Node,
	finalAllocs []*structs.Allocation,
	cfg structs.SchedulerGPUResourceReservation,
) (bool, string) {
	if cfg.IsZero() {
		return false, ""
	}

	required := gpuReservationRequired(node, finalAllocs, cfg)
	if required.cpuCores > 0 {
		available := gpuReservationAvailableComparable(node)
		if gpuReservationCPUShares(node, available, required.cpuCores, 1) < 0 {
			return true, gpuReservedCPUExhaustion
		}
	}

	return false, ""
}

func gpuReservationAvailableComparable(node *structs.Node) *structs.ComparableResources {
	if node == nil || node.NodeResources == nil ||
		node.NodeResources.Processors.Topology == nil {
		return nil
	}

	available := node.NodeResources.Comparable()
	if available == nil {
		return nil
	}
	available.Subtract(node.ReservedResources.Comparable())

	return available
}

func gpuReservationAvailableMemoryMB(node *structs.Node) (int64, bool) {
	if node == nil || node.NodeResources == nil {
		return 0, false
	}

	available := node.NodeResources.Memory.MemoryMB
	if node.ReservedResources != nil {
		available -= node.ReservedResources.Memory.MemoryMB
	}

	return available, true
}

func gpuReservationResourcesUsed(allocs []*structs.Allocation) *structs.ComparableResources {
	used := new(structs.ComparableResources)
	for _, alloc := range allocs {
		if alloc == nil || alloc.ClientTerminalStatus() ||
			alloc.AllocatedResources == nil {
			continue
		}

		used.Add(alloc.AllocatedResources.Comparable())
	}

	return used
}

// Copyright (c) 2017 MSO4SC - javier.carnero@atos.net
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// sinfo Slurm auxiliary collector

package slurm

import (
	"crypto/sha256"
	"encoding/base64"
	"hpc_exporter/ssh"
	"regexp"
	"strconv"
	"strings"
	"time"

	stats "github.com/montanaflynn/stats"
	log "github.com/sirupsen/logrus"
)

const (
	qsiPARTITION = iota
	qsiSTATE
	qsiAVAILABLE
	qsiCORES
	qsiCPUS
	qsiCPUSLOAD
	qsiALLOCMEM
	qsiNODES
	qsiFREEMEM
	qsiMEMORY
	qsiNODEAIOT
	qsiSIZE
	qsiTIME
	qsiFIELDS
)

const (
	qsqPARTITION = iota
	qsqSTATE
	qsqNUMCPUS
	qsqMINCPUS
	qsqMAXCPUS
	qsqNUMNODES
	qsqMAXNODES
	qsqMINMEM
	qsqPENDINGTIME
	qsqLEFTTIME
	qsqUSEDTIME
	qsqFIELDS
)

const (
	qsqePARTITION = iota
	qsqeSTATE
	qsqeNUMCPUS
	qsqeMINCPUS
	qsqeMAXCPUS
	qsqeNUMNODES
	qsqeMAXNODES
	qsqeMINMEM
	qsqePENDINGTIME
	qsqeLEFTTIME
	qsqeUSEDTIME
	qsqeJOBID
	qsqeENDTIME
	qsqePRIORITY
	qsqeSTARTTIME
	qsqeSUBMITTIME
	qsqeTIMELIMIT
	qsqeFIELDS
)

func (sc *SlurmCollector) collectPartitionMetricsUsingSInfo(partitionMetrics map[string](map[string]([]float64))) bool {
	var metricsAvailable bool = false

	infoCommand := "sinfo -h -O \"Partition,StateLong,Available,Cores,CPUs,CPUsLoad,AllocMem,Nodes,FreeMem,Memory,NodeAIOT,Size,Time\" | uniq"
	session := ssh.ExecuteSSHCommand(infoCommand, sc.sshClient)
	if session != nil {
		defer session.CloseSession()
	} else {
		return false
	}

	// wait for stdout to fill (it is being filled async by ssh)
	time.Sleep(1000 * time.Millisecond)

	nextLine := nextLineIterator(session.OutBuffer, sinfoLineParser)
	for fields, err := nextLine(); err == nil; fields, err = nextLine() {
		// check the line is correctly parsed
		if err != nil {
			log.Warnln(err.Error())
			continue
		}
		partition := processPartition(fields[qsiPARTITION])
		state := fields[qsiSTATE]

		// Collecting stats only from nodes in valid state for allocation
		if state == "idle" || state == "allocated" || state == "mixed" {
			metricsAvailable = true
			sc.pLabels["partition"][partition] = partition

			// Partition Availability
			availability := 0.0
			if fields[qsiAVAILABLE] == "up" {
				availability = 1.0
			}
			_, mapContainsKeyPartitionAvailable := partitionMetrics["PartitionAvailable"][partition]
			if !mapContainsKeyPartitionAvailable {
				partitionMetrics["PartitionAvailable"][partition] = []float64{}
			}
			partitionMetrics["PartitionAvailable"][partition] = append(partitionMetrics["PartitionAvailable"][partition], availability)

			// Partition cores per node
			_, mapContainsKeyPartitionCores := partitionMetrics["PartitionCores"][partition]
			if !mapContainsKeyPartitionCores {
				partitionMetrics["PartitionCores"][partition] = []float64{}
			}
			var cores, _ = strconv.ParseFloat(fields[qsiCORES], 64)
			partitionMetrics["PartitionCores"][partition] = append(partitionMetrics["PartitionCores"][partition], cores)

			// Partition cpus per core
			_, mapContainsKeyPartitionCpus := partitionMetrics["PartitionCpus"][partition]
			if !mapContainsKeyPartitionCpus {
				partitionMetrics["PartitionCpus"][partition] = []float64{}
			}
			var cpus, _ = strconv.ParseFloat(fields[qsiCPUS], 64)
			partitionMetrics["PartitionCpus"][partition] = append(partitionMetrics["PartitionCpus"][partition], cpus)

			// CPUs Load
			// Parse range
			_, mapContainsKeyPartitionCpusLoadLower := partitionMetrics["PartitionAvgCpusLoadLower"][partition]
			if !mapContainsKeyPartitionCpusLoadLower {
				partitionMetrics["PartitionAvgCpusLoadLower"][partition] = []float64{}
			}
			_, mapContainsKeyPartitionCpusLoadUpper := partitionMetrics["PartitionAvgCpusLoadUpper"][partition]
			if !mapContainsKeyPartitionCpusLoadUpper {
				partitionMetrics["PartitionAvgCpusLoadUpper"][partition] = []float64{}
			}

			var cpusLoadLower, cpusLoadUppper float64
			if strings.Contains(fields[qsiCPUSLOAD], "-") {
				cpusLoad := strings.Split(fields[qsiCPUSLOAD], "-")
				cpusLoadLower, _ = strconv.ParseFloat(cpusLoad[0], 64)
				cpusLoadUppper, _ = strconv.ParseFloat(cpusLoad[1], 64)
			} else {
				cpusLoadLower, _ = strconv.ParseFloat(fields[qsiCPUSLOAD], 64)
				cpusLoadUppper, _ = strconv.ParseFloat(fields[qsiCPUSLOAD], 64)
			}

			partitionMetrics["PartitionAvgCpusLoadLower"][partition] = append(partitionMetrics["PartitionAvgCpusLoadLower"][partition], cpusLoadLower)
			partitionMetrics["PartitionAvgCpusLoadUpper"][partition] = append(partitionMetrics["PartitionAvgCpusLoadUpper"][partition], cpusLoadUppper)

			// Alloc Mem
			_, mapContainsKeyPartitionAllocMem := partitionMetrics["PartitionAvgAllocMem"][partition]
			if !mapContainsKeyPartitionAllocMem {
				partitionMetrics["PartitionAvgAllocMem"][partition] = []float64{}
			}
			var allocMem, _ = strconv.ParseFloat(fields[qsiALLOCMEM], 64)
			partitionMetrics["PartitionAvgAllocMem"][partition] = append(partitionMetrics["PartitionAvgAllocMem"][partition], allocMem)

			// Nodes
			_, mapContainsKeyPartitionNodes := partitionMetrics["PartitionNodes"][partition]
			if !mapContainsKeyPartitionNodes {
				partitionMetrics["PartitionNodes"][partition] = []float64{}
			}
			var nodes, _ = strconv.ParseFloat(fields[qsiNODES], 64)
			partitionMetrics["PartitionNodes"][partition] = append(partitionMetrics["PartitionNodes"][partition], nodes)

			// Parse range
			_, mapContainsKeyPartitionFreeMemLower := partitionMetrics["PartitionAvgFreeMemLower"][partition]
			if !mapContainsKeyPartitionFreeMemLower {
				partitionMetrics["PartitionAvgFreeMemLower"][partition] = []float64{}
			}
			_, mapContainsKeyPartitionFreeMemUpper := partitionMetrics["PartitionAvgFreeMemUpper"][partition]
			if !mapContainsKeyPartitionFreeMemUpper {
				partitionMetrics["PartitionAvgFreeMemUpper"][partition] = []float64{}
			}

			var freeMemLower, freeMemUppper float64
			if strings.Contains(fields[qsiFREEMEM], "-") {
				freeMem := strings.Split(fields[qsiFREEMEM], "-")
				freeMemLower, _ = strconv.ParseFloat(freeMem[0], 64)
				freeMemUppper, _ = strconv.ParseFloat(freeMem[1], 64)
			} else {
				freeMemLower, _ = strconv.ParseFloat(fields[qsiFREEMEM], 64)
				freeMemUppper, _ = strconv.ParseFloat(fields[qsiFREEMEM], 64)
			}
			partitionMetrics["PartitionAvgFreeMemLower"][partition] = append(partitionMetrics["PartitionAvgFreeMemLower"][partition], freeMemLower)
			partitionMetrics["PartitionAvgFreeMemUpper"][partition] = append(partitionMetrics["PartitionAvgFreeMemUpper"][partition], freeMemUppper)

			// Memory
			_, mapContainsKeyPartitionMemory := partitionMetrics["PartitionAvgMemory"][partition]
			if !mapContainsKeyPartitionMemory {
				partitionMetrics["PartitionAvgMemory"][partition] = []float64{}
			}
			var memory, _ = strconv.ParseFloat(fields[qsiMEMORY], 64)
			partitionMetrics["PartitionAvgMemory"][partition] = append(partitionMetrics["PartitionAvgMemory"][partition], memory)

			//Node AIOT
			_, mapContainsKeyPartitionNodeAlloc := partitionMetrics["PartitionNodeAlloc"][partition]
			if !mapContainsKeyPartitionNodeAlloc {
				partitionMetrics["PartitionNodeAlloc"][partition] = []float64{}
			}
			_, mapContainsKeyPartitionNodeIdle := partitionMetrics["PartitionNodeIdle"][partition]
			if !mapContainsKeyPartitionNodeIdle {
				partitionMetrics["PartitionNodeIdle"][partition] = []float64{}
			}
			_, mapContainsKeyPartitionNodeOther := partitionMetrics["PartitionNodeOther"][partition]
			if !mapContainsKeyPartitionNodeOther {
				partitionMetrics["PartitionNodeOther"][partition] = []float64{}
			}
			_, mapContainsKeyPartitionNodeTotal := partitionMetrics["PartitionNodeTotal"][partition]
			if !mapContainsKeyPartitionNodeTotal {
				partitionMetrics["PartitionNodeTotal"][partition] = []float64{}
			}
			nodeaiot := strings.Split(fields[qsiNODEAIOT], "/")
			nodealloc, _ := strconv.ParseFloat(nodeaiot[0], 64)
			nodeidle, _ := strconv.ParseFloat(nodeaiot[1], 64)
			nodeother, _ := strconv.ParseFloat(nodeaiot[2], 64)
			nodetotal, _ := strconv.ParseFloat(nodeaiot[3], 64)
			partitionMetrics["PartitionNodeAlloc"][partition] = append(partitionMetrics["PartitionNodeAlloc"][partition], nodealloc)
			partitionMetrics["PartitionNodeIdle"][partition] = append(partitionMetrics["PartitionNodeIdle"][partition], nodeidle)
			partitionMetrics["PartitionNodeOther"][partition] = append(partitionMetrics["PartitionNodeOther"][partition], nodeother)
			partitionMetrics["PartitionNodeTotal"][partition] = append(partitionMetrics["PartitionNodeTotal"][partition], nodetotal)

			// Size
			// Parse range
			_, mapContainsKeyPartitionSizeLower := partitionMetrics["PartitionAvgJobSizeLower"][partition]
			if !mapContainsKeyPartitionSizeLower {
				partitionMetrics["PartitionAvgJobSizeLower"][partition] = []float64{}
			}
			_, mapContainsKeyPartitionSizeUpper := partitionMetrics["PartitionAvgJobSizeUpper"][partition]
			if !mapContainsKeyPartitionSizeUpper {
				partitionMetrics["PartitionAvgJobSizeUpper"][partition] = []float64{}
			}
			var sizeLower, sizeUpper float64
			if strings.Contains(fields[qsiSIZE], "-") {
				size := strings.Split(fields[qsiSIZE], "-")
				sizeLower, _ = strconv.ParseFloat(size[0], 64)
				sizeUpper, _ = strconv.ParseFloat(size[1], 64)
			} else {
				sizeLower, _ = strconv.ParseFloat(fields[qsiSIZE], 64)
				sizeUpper, _ = strconv.ParseFloat(fields[qsiSIZE], 64)
			}
			partitionMetrics["PartitionAvgJobSizeLower"][partition] = append(partitionMetrics["PartitionAvgJobSizeLower"][partition], sizeLower)
			partitionMetrics["PartitionAvgJobSizeUpper"][partition] = append(partitionMetrics["PartitionAvgJobSizeUpper"][partition], sizeUpper)

			// Time
			_, mapContainsKeyPartitionTimeLimit := partitionMetrics["PartitionAvgTimeLimit"][partition]
			if !mapContainsKeyPartitionTimeLimit {
				partitionMetrics["PartitionAvgTimeLimit"][partition] = []float64{}
			}
			// Parse time (day-hours:minute:second)
			partitionMetrics["PartitionAvgTimeLimit"][partition] = append(partitionMetrics["PartitionAvgTimeLimit"][partition], parseTime(fields[qsiTIME]))
		}
	}

	return metricsAvailable
}

func (sc *SlurmCollector) collectAveragedPartitionMetricsUsingSQueue(partitionMetrics map[string](map[string]([]float64))) bool {
	var metricsAvailable bool = false

	queueCommand := "squeue -h -O \"Partition,State,NumCPUs,MinCpus,MaxCPUs,NumNodes,MaxNodes,MinMemory,PendingTime,TimeLeft,TimeUsed\""
	session := ssh.ExecuteSSHCommand(queueCommand, sc.sshClient)
	if session != nil {
		defer session.CloseSession()
	} else {
		return metricsAvailable
	}

	// wait for stdout to fill (it is being filled async by ssh)
	time.Sleep(1000 * time.Millisecond)

	nextLine := nextLineIterator(session.OutBuffer, squeueLineParser2)
	for fields, err := nextLine(); err == nil; fields, err = nextLine() {
		// check the line is correctly parsed
		if err != nil {
			log.Warnln(err.Error())
			continue
		}

		partition := fields[qsqPARTITION]
		sc.pLabels["partition"][partition] = partition
		metricsAvailable = true

		// Job State (Pending|Running)
		state := 0.0
		if fields[qsqSTATE] == "RUNNING" {
			state = 1.0
		}

		//Number of Pending and running jobs in partition
		_, mapContainsKeyPartitionRunningJobs := partitionMetrics["PartitionRunningJobs"][partition]
		if !mapContainsKeyPartitionRunningJobs {
			partitionMetrics["PartitionRunningJobs"][partition] = []float64{}
		}
		_, mapContainsKeyPartitionPendingJobs := partitionMetrics["PartitionPendingJobs"][partition]
		if !mapContainsKeyPartitionPendingJobs {
			partitionMetrics["PartitionPendingJobs"][partition] = []float64{}
		}

		if fields[qsqSTATE] == "RUNNING" {
			partitionMetrics["PartitionRunningJobs"][partition] = append(partitionMetrics["PartitionRunningJobs"][partition], 1.0)
		} else {
			partitionMetrics["PartitionPendingJobs"][partition] = append(partitionMetrics["PartitionPendingJobs"][partition], 1.0)
		}

		// Average number of CPUS requested (state: pending)/allocated (state: running) per job
		_, mapContainsKeyPartitionRequestedCPUsPerJob := partitionMetrics["PartitionAvgRequestedCPUsPerJob"][partition]
		if !mapContainsKeyPartitionRequestedCPUsPerJob {
			partitionMetrics["PartitionAvgRequestedCPUsPerJob"][partition] = []float64{}
		}
		_, mapContainsKeyPartitionAllocatedCPUsPerJob := partitionMetrics["PartitionAvgAllocatedCPUsPerJob"][partition]
		if !mapContainsKeyPartitionAllocatedCPUsPerJob {
			partitionMetrics["PartitionAvgAllocatedCPUsPerJob"][partition] = []float64{}
		}
		var cpus, _ = strconv.ParseFloat(fields[qsqNUMCPUS], 64)
		if state == 0.0 {
			partitionMetrics["PartitionAvgRequestedCPUsPerJob"][partition] = append(partitionMetrics["PartitionAvgRequestedCPUsPerJob"][partition], cpus)
		} else {
			partitionMetrics["PartitionAvgAllocatedCPUsPerJob"][partition] = append(partitionMetrics["PartitionAvgAllocatedCPUsPerJob"][partition], cpus)
		}

		// Average minimum number of requested CPUs per job
		_, mapContainsKeyPartitionMinimumRequestedCPUsPerJob := partitionMetrics["PartitionAvgMinimumRequestedCPUsPerJob"][partition]
		if !mapContainsKeyPartitionMinimumRequestedCPUsPerJob {
			partitionMetrics["PartitionAvgMinimumRequestedCPUsPerJob"][partition] = []float64{}
		}
		cpus, _ = strconv.ParseFloat(fields[qsqMINCPUS], 64)
		partitionMetrics["PartitionAvgMinimumRequestedCPUsPerJob"][partition] = append(partitionMetrics["PartitionAvgMinimumRequestedCPUsPerJob"][partition], cpus)

		// Average maximum number of allocated CPUs per job (state: running)
		_, mapContainsKeyPartitionMaximumAllocatedCPUsPerJob := partitionMetrics["PartitionAvgMaximumAllocatedCPUsPerJob"][partition]
		if !mapContainsKeyPartitionMaximumAllocatedCPUsPerJob {
			partitionMetrics["PartitionAvgMaximumAllocatedCPUsPerJob"][partition] = []float64{}
		}
		cpus, _ = strconv.ParseFloat(fields[qsqMAXCPUS], 64)
		partitionMetrics["PartitionAvgMaximumAllocatedCPUsPerJob"][partition] = append(partitionMetrics["PartitionAvgMaximumAllocatedCPUsPerJob"][partition], cpus)

		// Average number of nodes allocated (state: running) or minimum number of requested (state: pending) per job
		_, mapContainsKeyPartitionMinimumRequestedNodesPerJob := partitionMetrics["PartitionAvgMinimumRequestedNodesPerJob"][partition]
		if !mapContainsKeyPartitionMinimumRequestedNodesPerJob {
			partitionMetrics["PartitionAvgMinimumRequestedNodesPerJob"][partition] = []float64{}
		}
		_, mapContainsKeyPartitionAllocatedNodesPerJob := partitionMetrics["PartitionAvgAllocatedNodesPerJob"][partition]
		if !mapContainsKeyPartitionAllocatedNodesPerJob {
			partitionMetrics["PartitionAvgAllocatedNodesPerJob"][partition] = []float64{}
		}
		var nodes, _ = strconv.ParseFloat(fields[qsqNUMNODES], 64)
		if state == 0.0 {
			partitionMetrics["PartitionAvgMinimumRequestedNodesPerJob"][partition] = append(partitionMetrics["PartitionAvgMinimumRequestedNodesPerJob"][partition], nodes)
		} else {
			partitionMetrics["PartitionAvgAllocatedNodesPerJob"][partition] = append(partitionMetrics["PartitionAvgAllocatedNodesPerJob"][partition], nodes)
		}

		// Average maximum number of nodes allocated per job (state: running)
		_, mapContainsKeyPartitionMaximumAllocatedNodePerJob := partitionMetrics["PartitionAvgMaximumAllocatedNodePerJob"][partition]
		if !mapContainsKeyPartitionMaximumAllocatedNodePerJob {
			partitionMetrics["PartitionAvgMaximumAllocatedNodePerJob"][partition] = []float64{}
		}
		if state == 1.0 {
			nodes, _ = strconv.ParseFloat(fields[qsqMAXNODES], 64)
			partitionMetrics["PartitionAvgMaximumAllocatedNodePerJob"][partition] = append(partitionMetrics["PartitionAvgMaximumAllocatedNodePerJob"][partition], nodes)
		}

		// Average minimum memory requested per job
		_, mapContainsKeyPartitionMinimumRequestedMemoryPerJob := partitionMetrics["PartitionAvgMinimumRequestedMemoryPerJob"][partition]
		if !mapContainsKeyPartitionMinimumRequestedMemoryPerJob {
			partitionMetrics["PartitionAvgMinimumRequestedMemoryPerJob"][partition] = []float64{}
		}

		mem := parseMemField(fields[qsqMINMEM])
		partitionMetrics["PartitionAvgMinimumRequestedMemoryPerJob"][partition] = append(partitionMetrics["PartitionAvgMinimumRequestedMemoryPerJob"][partition], mem)

		// Average queued time per job
		_, mapContainsKeyPartitionQueueTimePerJob := partitionMetrics["PartitionAvgQueueTimePerJob"][partition]
		if !mapContainsKeyPartitionQueueTimePerJob {
			partitionMetrics["PartitionAvgQueueTimePerJob"][partition] = []float64{}
		}
		var time, _ = strconv.ParseFloat(fields[qsqPENDINGTIME], 64)
		partitionMetrics["PartitionAvgQueueTimePerJob"][partition] = append(partitionMetrics["PartitionAvgQueueTimePerJob"][partition], time)

		// Average time left to exhaust maximum time per job
		_, mapContainsKeyPartitionTimeLeftPerJob := partitionMetrics["PartitionAvgTimeLeftPerJob"][partition]
		if !mapContainsKeyPartitionTimeLeftPerJob {
			partitionMetrics["PartitionAvgTimeLeftPerJob"][partition] = []float64{}
		}
		// Parse time (day-hours:minute:second)
		partitionMetrics["PartitionAvgTimeLeftPerJob"][partition] = append(partitionMetrics["PartitionAvgTimeLeftPerJob"][partition], parseTime(fields[qsqLEFTTIME]))

		// Average execution time per job (state: running)
		_, mapContainsKeyPartitionExecutionTimePerJob := partitionMetrics["PartitionAvgExecutionTimePerJob"][partition]
		if !mapContainsKeyPartitionExecutionTimePerJob {
			partitionMetrics["PartitionAvgExecutionTimePerJob"][partition] = []float64{}
		}

		// Parse time (day-hours:minute:second)
		if state == 1.0 {
			partitionMetrics["PartitionAvgExecutionTimePerJob"][partition] = append(partitionMetrics["PartitionAvgExecutionTimePerJob"][partition], parseTime(fields[qsqUSEDTIME]))
		}
	}

	return metricsAvailable
}

func (sc *SlurmCollector) collectPartitionJobMetricsUsingSQueue(partitionJobMetrics map[string](map[string](float64))) bool {
	var metricsAvailable bool = false

	queueCommand := "squeue -h -O \"Partition,State,NumCPUs,MinCpus,MaxCPUs,NumNodes,MaxNodes,MinMemory,PendingTime,TimeLeft,TimeUsed,JobID,EndTime,Priority,StartTime,SubmitTime,TimeLimit\""
	session := ssh.ExecuteSSHCommand(queueCommand, sc.sshClient)
	if session != nil {
		defer session.CloseSession()
	} else {
		return metricsAvailable
	}

	// wait for stdout to fill (it is being filled async by ssh)
	time.Sleep(1000 * time.Millisecond)

	nextLine := nextLineIterator(session.OutBuffer, squeueLineParser2)
	for fields, err := nextLine(); err == nil; fields, err = nextLine() {
		// check the line is correctly parsed
		if err != nil {
			log.Warnln(err.Error())
			continue
		}

		metricsAvailable = true

		job_id := fields[qsqeJOBID]
		sc.pjLabels["job_id_hash"][job_id] = computeHash(job_id)
		sc.pjLabels["partition"][job_id] = fields[qsqePARTITION]
		sc.pjLabels["priority"][job_id] = fields[qsqePRIORITY]
		sc.pjLabels["submit_time"][job_id] = fields[qsqeSUBMITTIME]
		sc.pjLabels["time_limit"][job_id] = fields[qsqeTIMELIMIT]

		// Job State (Pending|Running)
		state := 0.0
		if fields[qsqSTATE] == "RUNNING" {
			state = 1.0
		}
		partitionJobMetrics["PartitionJobState"][job_id] = state

		// Number of CPUS requested/allocated for job
		var cpus, _ = strconv.ParseFloat(fields[qsqeNUMCPUS], 64)
		_, mapContainsKeyPartitionJobRequestedAllocatedCPUs := partitionJobMetrics["PartitionJobRequestedAllocatedCPUs"][job_id]
		if !mapContainsKeyPartitionJobRequestedAllocatedCPUs {
			partitionJobMetrics["PartitionJobRequestedAllocatedCPUs"][job_id] = cpus
		}

		// Minimum number of requested CPUs for job
		cpus, _ = strconv.ParseFloat(fields[qsqeMINCPUS], 64)
		_, mapContainsKeyPartitionJobMinimumRequestedCPUs := partitionJobMetrics["PartitionJobMinimumRequestedCPUs"][job_id]
		if !mapContainsKeyPartitionJobMinimumRequestedCPUs {
			partitionJobMetrics["PartitionJobMinimumRequestedCPUs"][job_id] = cpus
		}

		// Maximum number of allocated CPUs for job
		cpus, _ = strconv.ParseFloat(fields[qsqeMAXCPUS], 64)
		_, mapContainsKeyPartitionJobMaximumAllocatedCPUs := partitionJobMetrics["PartitionJobMaximumAllocatedCPUs"][job_id]
		if !mapContainsKeyPartitionJobMaximumAllocatedCPUs {
			partitionJobMetrics["PartitionJobMaximumAllocatedCPUs"][job_id] = cpus
		}

		// Number of nodes allocated/minimum number of requested for job
		var nodes, _ = strconv.ParseFloat(fields[qsqeNUMNODES], 64)
		_, mapContainsKeyPartitionJobAllocatedMinimumRequestedNodes := partitionJobMetrics["PartitionJobAllocatedMinimumRequestedNodes"][job_id]
		if !mapContainsKeyPartitionJobAllocatedMinimumRequestedNodes {
			partitionJobMetrics["PartitionJobAllocatedMinimumRequestedNodes"][job_id] = nodes
		}

		// Maximum number of nodes allocated for job
		nodes, _ = strconv.ParseFloat(fields[qsqeMAXNODES], 64)
		_, mapContainsKeyPartitionJobMaximumAllocatedNode := partitionJobMetrics["PartitionJobMaximumAllocatedNode"][job_id]
		if !mapContainsKeyPartitionJobMaximumAllocatedNode {
			partitionJobMetrics["PartitionJobMaximumAllocatedNode"][job_id] = nodes
		}

		// Minimum memory requested for job
		mem := parseMemField(fields[qsqeMINMEM])
		_, mapContainsKeyPartitionJobMinimumRequestedMemory := partitionJobMetrics["PartitionJobMinimumRequestedMemory"][job_id]
		if !mapContainsKeyPartitionJobMinimumRequestedMemory {
			partitionJobMetrics["PartitionJobMinimumRequestedMemory"][job_id] = mem
		}

		// Queued time for job
		var time, _ = strconv.ParseFloat(fields[qsqePENDINGTIME], 64)
		_, mapContainsKeyPartitionJobQueueTime := partitionJobMetrics["PartitionJobQueueTime"][job_id]
		if !mapContainsKeyPartitionJobQueueTime {
			partitionJobMetrics["PartitionJobQueueTime"][job_id] = time
		}

		// Time left to exhaust maximum time for job
		// Parse time (day-hours:minute:second)
		time = parseTime(fields[qsqeLEFTTIME])
		_, mapContainsKeyPartitionJobTimeLeft := partitionJobMetrics["PartitionJobTimeLeft"][job_id]
		if !mapContainsKeyPartitionJobTimeLeft {
			partitionJobMetrics["PartitionJobTimeLeft"][job_id] = time
		}

		// Execution time for job
		time = parseTime(fields[qsqeUSEDTIME])
		_, mapContainsKeyPartitionExecutionTimePerJob := partitionJobMetrics["PartitionJobExecutionTime"][job_id]
		if !mapContainsKeyPartitionExecutionTimePerJob {
			partitionJobMetrics["PartitionJobExecutionTime"][job_id] = time
		}

		// Start time for job
		time = computeSlurmDateTime(fields[qsqeSTARTTIME])
		_, mapContainsKeyPartitionJobExecutionStartTime := partitionJobMetrics["PartitionJobExecutionStartTime"][job_id]
		if !mapContainsKeyPartitionJobExecutionStartTime {
			partitionJobMetrics["PartitionJobExecutionStartTime"][job_id] = time
		}

		// End time for job
		time = computeSlurmDateTime(fields[qsqeENDTIME])
		_, mapContainsKeyPartitionJobExecutionEndTime := partitionJobMetrics["PartitionJobExecutionEndTime"][job_id]
		if !mapContainsKeyPartitionJobExecutionEndTime {
			partitionJobMetrics["PartitionJobExecutionEndTime"][job_id] = time
		}
	}

	return metricsAvailable
}

func computeHash(id string) string {
	h := sha256.New()
	h.Write([]byte(id))
	sha := base64.URLEncoding.EncodeToString(h.Sum(nil))
	return sha
}

func (sc *SlurmCollector) collectInfo() {
	log.Debugln("Collecting partition metrics...")
	var collected uint
	var metricsAvailable bool = false

	// Allocate metrics
	partitionMetrics := make(map[string](map[string]([]float64)))
	for key, _ := range sc.pMetrics {
		if strings.HasPrefix(key, "Partition") {
			partitionMetrics[key] = make(map[string][]float64)
		}
	}

	partitionJobMetrics := make(map[string](map[string](float64)))
	for key, _ := range sc.pMetrics {
		if strings.HasPrefix(key, "Partition") {
			partitionJobMetrics[key] = make(map[string]float64)
		}
	}

	// Stats from sinfo
	metricsAvailable = sc.collectPartitionMetricsUsingSInfo(partitionMetrics)

	// Stats from squeue
	metricsAvailable = sc.collectAveragedPartitionMetricsUsingSQueue(partitionMetrics) || metricsAvailable

	// Add stats per job (not averaged) in partition
	metricsAvailable = sc.collectPartitionJobMetricsUsingSQueue(partitionJobMetrics) || metricsAvailable

	// Computing average
	averageMetrics := []string{
		"PartitionAvailable", "PartitionCores", "PartitionCpus", "PartitionAvgCpusLoadLower",
		"PartitionAvgCpusLoadUpper", "PartitionAvgAllocMem", "PartitionAvgFreeMemLower", "PartitionAvgFreeMemUpper",
		"PartitionAvgMemory", "PartitionAvgJobSizeLower", "PartitionAvgJobSizeUpper",
		"PartitionAvgTimeLimit", "PartitionAvgRequestedCPUsPerJob", "PartitionAvgAllocatedCPUsPerJob",
		"PartitionAvgMinimumRequestedCPUsPerJob", "PartitionAvgMaximumAllocatedCPUsPerJob",
		"PartitionAvgMinimumRequestedNodesPerJob", "PartitionAvgAllocatedNodesPerJob",
		"PartitionAvgMaximumAllocatedNodePerJob", "PartitionAvgMinimumRequestedMemoryPerJob",
		"PartitionAvgQueueTimePerJob", "PartitionAvgTimeLeftPerJob", "PartitionAvgExecutionTimePerJob",
	}

	totalMetrics := []string{
		"PartitionNodes", "PartitionNodeAlloc", "PartitionNodeIdle",
		"PartitionNodeOther", "PartitionNodeTotal", "PartitionRunningJobs", "PartitionPendingJobs",
	}

	if metricsAvailable {
		for metric, metricMap := range partitionMetrics {
			for partition, value := range metricMap {
				if stringInSlice(metric, averageMetrics) {
					sc.pMetrics[metric][partition] = -1
					if len(value) > 0 {
						sc.pMetrics[metric][partition], _ = stats.Mean(value)
					}
				}
				if stringInSlice(metric, totalMetrics) {
					sc.pMetrics[metric][partition] = 0
					if len(value) > 0 {
						sc.pMetrics[metric][partition], _ = stats.Sum(value)
					}
				}
			}
		}

		// Metrics for jobs in partition
		for metric, metricMap := range partitionJobMetrics {
			for partition, value := range metricMap {
				sc.pjMetrics[metric][partition] = value
			}
		}

		collected = uint(len(sc.pMetrics["PartitionAvailable"]))
		log.Infof("Metrics for %d partitions collected", collected)
	} else {
		log.Warnf("No partition metrics could be collected from host %s. Commands sinfo and squeue could not be available for user", sc.sshConfig.Host)
	}
}

func parseMemField(sMem string) float64 {
	unit := sMem[len(sMem)-1:]
	factor := 1.0
	if unit == "G" {
		factor = 1024.0
	}
	var mem, _ = strconv.ParseFloat(sMem[:len(sMem)-1], 64)
	return factor * mem
}

func parseTime(sTime string) float64 {
	//Parse sTime string in format days-hours:minutes:seconds
	split := strings.Split(sTime, "-")
	days := 0.0
	remain := sTime
	if len(split) == 2 {
		days, _ = strconv.ParseFloat(split[0], 64)
		remain = split[1]
	}
	sremain := strings.Split(remain, ":")
	hours := 0.0
	if len(sremain) > 0 {
		hours, _ = strconv.ParseFloat(sremain[0], 64)
	}
	minutes := 0.0
	if len(sremain) > 1 {
		minutes, _ = strconv.ParseFloat(sremain[1], 64)
	}
	seconds := 0.0
	if len(sremain) > 2 {
		seconds, _ = strconv.ParseFloat(sremain[2], 64)
	}
	return days*24*60*60 + hours*60*60 + minutes*60 + seconds
}

func sinfoLineParser(line string) []string {
	fields := strings.Fields(line)

	if len(fields) < qsiFIELDS {
		log.Warnf("sinfo line parse failed (%s): %d fields expected, %d parsed", line, qsiFIELDS, len(fields))
		return nil
	}

	return fields
}

func squeueLineParser2(line string) []string {
	fields := strings.Fields(line)

	if len(fields) < qsqFIELDS {
		log.Warnf("squeue line parse failed (%s): %d fields expected, %d parsed", line, qFIELDS, len(fields))
		return nil
	}

	return fields
}

func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

// Remove non-alphabethical characters in partition name
func processPartition(partition string) string {
	reg, err := regexp.Compile("[^a-zA-Z0-9]+")
	var processedPartition = ""
	if err == nil {
		processedPartition = reg.ReplaceAllString(partition, "")
	}
	return processedPartition
}

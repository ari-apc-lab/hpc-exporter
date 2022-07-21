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
	"hpc_exporter/ssh"
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

func (sc *SlurmCollector) collectInfo() {
	log.Debugln("Collecting Info metrics...")
	var collected uint
	var metricsAvailable bool = false

	// Read queue information combining reports from squeue and sinfo commands
	// sinfo:  sinfo -h -O "Partition,StateLong,Available,Cores,CPUs,CPUsLoad,AllocMem,Nodes,FreeMem,Memory,NodeAIOT,Size,Time"
	// squeue:  squeue -h --partition=medium -O “MaxCPUs,MaxNodes,MinCpus,MinMemory,NumCPUs,NumNodes,Partition,PendingTime,State,TimeLeft,TimeUsed”

	// Compute average (for all jobs) metrics
	// Store metrics per partition

	// Stats from sinfo

	infoCommand := "sinfo -h -O \"Partition,StateLong,Available,Cores,CPUs,CPUsLoad,AllocMem,Nodes,FreeMem,Memory,NodeAIOT,Size,Time\" | uniq"
	session := ssh.ExecuteSSHCommand(infoCommand, sc.sshClient)
	if session != nil {
		defer session.CloseSession()
	} else {
		return
	}

	// wait for stdout to fill (it is being filled async by ssh)
	time.Sleep(1000 * time.Millisecond)

	// Allocate metrics
	partitionMetrics := make(map[string](map[string]([]float64)))
	for key, _ := range sc.pMetrics {
		if strings.HasPrefix(key, "Partition") {
			partitionMetrics[key] = make(map[string][]float64)
		}
	}

	nextLine := nextLineIterator(session.OutBuffer, sinfoLineParser)
	for fields, err := nextLine(); err == nil; fields, err = nextLine() {
		// check the line is correctly parsed
		if err != nil {
			log.Warnln(err.Error())
			continue
		}
		partition := fields[qsiPARTITION]
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
			_, mapContainsKeyPartitionCpusLoadLower := partitionMetrics["PartitionCpusLoadLower"][partition]
			if !mapContainsKeyPartitionCpusLoadLower {
				partitionMetrics["PartitionCpusLoadLower"][partition] = []float64{}
			}
			_, mapContainsKeyPartitionCpusLoadUpper := partitionMetrics["PartitionCpusLoadUpper"][partition]
			if !mapContainsKeyPartitionCpusLoadUpper {
				partitionMetrics["PartitionCpusLoadUpper"][partition] = []float64{}
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

			partitionMetrics["PartitionCpusLoadLower"][partition] = append(partitionMetrics["PartitionCpusLoadLower"][partition], cpusLoadLower)
			partitionMetrics["PartitionCpusLoadUpper"][partition] = append(partitionMetrics["PartitionCpusLoadUpper"][partition], cpusLoadUppper)

			// Alloc Mem
			_, mapContainsKeyPartitionAllocMem := partitionMetrics["PartitionAllocMem"][partition]
			if !mapContainsKeyPartitionAllocMem {
				partitionMetrics["PartitionAllocMem"][partition] = []float64{}
			}
			var allocMem, _ = strconv.ParseFloat(fields[qsiALLOCMEM], 64)
			partitionMetrics["PartitionAllocMem"][partition] = append(partitionMetrics["PartitionAllocMem"][partition], allocMem)

			// Nodes
			_, mapContainsKeyPartitionNodes := partitionMetrics["PartitionNodes"][partition]
			if !mapContainsKeyPartitionNodes {
				partitionMetrics["PartitionNodes"][partition] = []float64{}
			}
			var nodes, _ = strconv.ParseFloat(fields[qsiNODES], 64)
			partitionMetrics["PartitionNodes"][partition] = append(partitionMetrics["PartitionNodes"][partition], nodes)

			// Parse range
			_, mapContainsKeyPartitionFreeMemLower := partitionMetrics["PartitionFreeMemLower"][partition]
			if !mapContainsKeyPartitionFreeMemLower {
				partitionMetrics["PartitionFreeMemLower"][partition] = []float64{}
			}
			_, mapContainsKeyPartitionFreeMemUpper := partitionMetrics["PartitionFreeMemUpper"][partition]
			if !mapContainsKeyPartitionFreeMemUpper {
				partitionMetrics["PartitionFreeMemUpper"][partition] = []float64{}
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
			partitionMetrics["PartitionFreeMemLower"][partition] = append(partitionMetrics["PartitionFreeMemLower"][partition], freeMemLower)
			partitionMetrics["PartitionFreeMemUpper"][partition] = append(partitionMetrics["PartitionFreeMemUpper"][partition], freeMemUppper)

			// Memory
			_, mapContainsKeyPartitionMemory := partitionMetrics["PartitionMemory"][partition]
			if !mapContainsKeyPartitionMemory {
				partitionMetrics["PartitionMemory"][partition] = []float64{}
			}
			var memory, _ = strconv.ParseFloat(fields[qsiMEMORY], 64)
			partitionMetrics["PartitionMemory"][partition] = append(partitionMetrics["PartitionMemory"][partition], memory)

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
			_, mapContainsKeyPartitionSizeLower := partitionMetrics["PartitionJobSizeLower"][partition]
			if !mapContainsKeyPartitionSizeLower {
				partitionMetrics["PartitionJobSizeLower"][partition] = []float64{}
			}
			_, mapContainsKeyPartitionSizeUpper := partitionMetrics["PartitionJobSizeUpper"][partition]
			if !mapContainsKeyPartitionSizeUpper {
				partitionMetrics["PartitionJobSizeUpper"][partition] = []float64{}
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
			partitionMetrics["PartitionJobSizeLower"][partition] = append(partitionMetrics["PartitionJobSizeLower"][partition], sizeLower)
			partitionMetrics["PartitionJobSizeUpper"][partition] = append(partitionMetrics["PartitionJobSizeUpper"][partition], sizeUpper)

			// Time
			_, mapContainsKeyPartitionTimeLimit := partitionMetrics["PartitionTimeLimit"][partition]
			if !mapContainsKeyPartitionTimeLimit {
				partitionMetrics["PartitionTimeLimit"][partition] = []float64{}
			}
			// Parse time (day-hours:minute:second)
			partitionMetrics["PartitionTimeLimit"][partition] = append(partitionMetrics["PartitionTimeLimit"][partition], parseTime(fields[qsiTIME]))
		}
	}

	// Stats from squeue
	queueCommand := "squeue -h -O \"Partition,State,NumCPUs,MinCpus,MaxCPUs,NumNodes,MaxNodes,MinMemory,PendingTime,TimeLeft,TimeUsed\""
	session = ssh.ExecuteSSHCommand(queueCommand, sc.sshClient)
	if session != nil {
		defer session.CloseSession()
	} else {
		return
	}

	// wait for stdout to fill (it is being filled async by ssh)
	time.Sleep(1000 * time.Millisecond)

	nextLine = nextLineIterator(session.OutBuffer, squeueLineParser2)
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

		// Average number of CPUS requested (state: pending)/allocated (state: running) per job
		_, mapContainsKeyPartitionRequestedCPUsPerJob := partitionMetrics["PartitionRequestedCPUsPerJob"][partition]
		if !mapContainsKeyPartitionRequestedCPUsPerJob {
			partitionMetrics["PartitionRequestedCPUsPerJob"][partition] = []float64{}
		}
		_, mapContainsKeyPartitionAllocatedCPUsPerJob := partitionMetrics["PartitionAllocatedCPUsPerJob"][partition]
		if !mapContainsKeyPartitionAllocatedCPUsPerJob {
			partitionMetrics["PartitionAllocatedCPUsPerJob"][partition] = []float64{}
		}
		var cpus, _ = strconv.ParseFloat(fields[qsqNUMCPUS], 64)
		if state == 0.0 {
			partitionMetrics["PartitionRequestedCPUsPerJob"][partition] = append(partitionMetrics["PartitionRequestedCPUsPerJob"][partition], cpus)
		} else {
			partitionMetrics["PartitionAllocatedCPUsPerJob"][partition] = append(partitionMetrics["PartitionAllocatedCPUsPerJob"][partition], cpus)
		}

		// Average minimum number of requested CPUs per job
		_, mapContainsKeyPartitionMinimumRequestedCPUsPerJob := partitionMetrics["PartitionMinimumRequestedCPUsPerJob"][partition]
		if !mapContainsKeyPartitionMinimumRequestedCPUsPerJob {
			partitionMetrics["PartitionMinimumRequestedCPUsPerJob"][partition] = []float64{}
		}
		cpus, _ = strconv.ParseFloat(fields[qsqMINCPUS], 64)
		partitionMetrics["PartitionMinimumRequestedCPUsPerJob"][partition] = append(partitionMetrics["PartitionMinimumRequestedCPUsPerJob"][partition], cpus)

		// Average maximum number of allocated CPUs per job (state: running)
		_, mapContainsKeyPartitionMaximumAllocatedCPUsPerJob := partitionMetrics["PartitionMaximumAllocatedCPUsPerJob"][partition]
		if !mapContainsKeyPartitionMaximumAllocatedCPUsPerJob {
			partitionMetrics["PartitionMaximumAllocatedCPUsPerJob"][partition] = []float64{}
		}
		cpus, _ = strconv.ParseFloat(fields[qsqMAXCPUS], 64)
		partitionMetrics["PartitionMaximumAllocatedCPUsPerJob"][partition] = append(partitionMetrics["PartitionMaximumAllocatedCPUsPerJob"][partition], cpus)

		// Average number of nodes allocated (state: running) or minimum number of requested (state: pending) per job
		_, mapContainsKeyPartitionMinimumRequestedNodesPerJob := partitionMetrics["PartitionMinimumRequestedNodesPerJob"][partition]
		if !mapContainsKeyPartitionMinimumRequestedNodesPerJob {
			partitionMetrics["PartitionMinimumRequestedNodesPerJob"][partition] = []float64{}
		}
		_, mapContainsKeyPartitionAllocatedNodesPerJob := partitionMetrics["PartitionAllocatedNodesPerJob"][partition]
		if !mapContainsKeyPartitionAllocatedNodesPerJob {
			partitionMetrics["PartitionAllocatedNodesPerJob"][partition] = []float64{}
		}
		var nodes, _ = strconv.ParseFloat(fields[qsqNUMNODES], 64)
		if state == 0.0 {
			partitionMetrics["PartitionMinimumRequestedNodesPerJob"][partition] = append(partitionMetrics["PartitionMinimumRequestedNodesPerJob"][partition], nodes)
		} else {
			partitionMetrics["PartitionAllocatedNodesPerJob"][partition] = append(partitionMetrics["PartitionAllocatedNodesPerJob"][partition], nodes)
		}

		// Average maximum number of nodes allocated per job (state: running)
		_, mapContainsKeyPartitionMaximumAllocatedNodePerJob := partitionMetrics["PartitionMaximumAllocatedNodePerJob"][partition]
		if !mapContainsKeyPartitionMaximumAllocatedNodePerJob {
			partitionMetrics["PartitionMaximumAllocatedNodePerJob"][partition] = []float64{}
		}
		if state == 1.0 {
			nodes, _ = strconv.ParseFloat(fields[qsqMAXNODES], 64)
			partitionMetrics["PartitionMaximumAllocatedNodePerJob"][partition] = append(partitionMetrics["PartitionMaximumAllocatedNodePerJob"][partition], nodes)
		}

		// Average minimum memory requested per job
		_, mapContainsKeyPartitionMinimumRequestedMemoryPerJob := partitionMetrics["PartitionMinimumRequestedMemoryPerJob"][partition]
		if !mapContainsKeyPartitionMinimumRequestedMemoryPerJob {
			partitionMetrics["PartitionMinimumRequestedMemoryPerJob"][partition] = []float64{}
		}

		mem := parseMemField(fields[qsqMINMEM])
		partitionMetrics["PartitionMinimumRequestedMemoryPerJob"][partition] = append(partitionMetrics["PartitionMinimumRequestedMemoryPerJob"][partition], mem)

		// Average queued time per job
		_, mapContainsKeyPartitionQueueTimePerJob := partitionMetrics["PartitionQueueTimePerJob"][partition]
		if !mapContainsKeyPartitionQueueTimePerJob {
			partitionMetrics["PartitionQueueTimePerJob"][partition] = []float64{}
		}
		var time, _ = strconv.ParseFloat(fields[qsqPENDINGTIME], 64)
		partitionMetrics["PartitionQueueTimePerJob"][partition] = append(partitionMetrics["PartitionQueueTimePerJob"][partition], time)

		// Average time left to exhaust maximum time per job
		_, mapContainsKeyPartitionTimeLeftPerJob := partitionMetrics["PartitionTimeLeftPerJob"][partition]
		if !mapContainsKeyPartitionTimeLeftPerJob {
			partitionMetrics["PartitionTimeLeftPerJob"][partition] = []float64{}
		}
		// Parse time (day-hours:minute:second)
		partitionMetrics["PartitionTimeLeftPerJob"][partition] = append(partitionMetrics["PartitionTimeLeftPerJob"][partition], parseTime(fields[qsqLEFTTIME]))

		// Average execution time per job (state: running)
		_, mapContainsKeyPartitionExecutionTimePerJob := partitionMetrics["PartitionExecutionTimePerJob"][partition]
		if !mapContainsKeyPartitionExecutionTimePerJob {
			partitionMetrics["PartitionExecutionTimePerJob"][partition] = []float64{}
		}

		// Parse time (day-hours:minute:second)
		if state == 1.0 {
			partitionMetrics["PartitionExecutionTimePerJob"][partition] = append(partitionMetrics["PartitionExecutionTimePerJob"][partition], parseTime(fields[qsqUSEDTIME]))
		}
	}

	// Computing average
	averageMetrics := []string{
		"PartitionAvailable", "PartitionCores", "PartitionCpus", "PartitionCpusLoadLower",
		"PartitionCpusLoadUpper", "PartitionAllocMem", "PartitionFreeMemLower", "PartitionFreeMemUpper",
		"PartitionMemory", "PartitionJobSizeLower", "PartitionJobSizeUpper",
		"PartitionTimeLimit", "PartitionRequestedCPUsPerJob", "PartitionAllocatedCPUsPerJob",
		"PartitionMinimumRequestedCPUsPerJob", "PartitionMaximumAllocatedCPUsPerJob",
		"PartitionMinimumRequestedNodesPerJob", "PartitionAllocatedNodesPerJob",
		"PartitionMaximumAllocatedNodePerJob", "PartitionMinimumRequestedMemoryPerJob",
		"PartitionQueueTimePerJob", "PartitionTimeLeftPerJob", "PartitionExecutionTimePerJob",
	}

	totalMetrics := []string{
		"PartitionNodes", "PartitionNodeAlloc", "PartitionNodeIdle",
		"PartitionNodeOther", "PartitionNodeTotal",
	}

	if metricsAvailable {
		for metric, metricMap := range partitionMetrics {
			for partition, value := range metricMap {
				if len(value) > 0 {
					if stringInSlice(metric, averageMetrics) {
						sc.pMetrics[metric][partition], _ = stats.Mean(value)
					}
					if stringInSlice(metric, totalMetrics) {
						sc.pMetrics[metric][partition], _ = stats.Sum(value)
					}
				}
			}
		}

		collected = uint(len(sc.pMetrics["PartitionAvailable"]))
		log.Infof("%d partition info collected", collected)
	} else {
		log.Warnf("No metrics could be collected from host %s. Commands sinfo and squeue could not be available for user", sc.sshConfig.Host)
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

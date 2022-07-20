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

// const (
// 	iSTATESNUMBER = 4
// )

func (sc *SlurmCollector) collectInfo() {
	log.Debugln("Collecting Info metrics...")
	var collected uint

	//TODO
	// Read queue information combining reports from squeue and sinfo commands
	// sinfo:  sinfo -h -O "Partition,StateLong,Available,Cores,CPUs,CPUsLoad,AllocMem,Nodes,FreeMem,Memory,NodeAIOT,Size,Time"
	// squeue:  squeue -h --partition=medium -O “MaxCPUs,MaxNodes,MinCpus,MinMemory,NumCPUs,NumNodes,Partition,PendingTime,State,TimeLeft,TimeUsed”

	// Compute average (for all jobs) metrics
	// Store metrics per partition

	// Stats from sinfo

	queueCommand := "sinfo -h -O \"Partition,StateLong,Available,Cores,CPUs,CPUsLoad,AllocMem,Nodes,FreeMem,Memory,NodeAIOT,Size,Time\""
	session := ssh.ExecuteSSHCommand(queueCommand, sc.sshClient)
	if session != nil {
		defer session.CloseSession()
	} else {
		return
	}

	// wait for stdout to fill (it is being filled async by ssh)
	time.Sleep(1000 * time.Millisecond)

	// Allocate metrics
	metrics := make(map[string](map[string]([]float64)))
	for key, _ := range sc.pMetrics {
		if strings.HasPrefix(key, "Partition") {
			metrics[key] = make(map[string][]float64)
		}
	}

	nextLine := nextLineIterator(session.OutBuffer, squeueLineParser)
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
			//sc.trackedJobs[partition] = true
			sc.pLabels["partition"][partition] = partition

			// Partition Availability
			availability := 0.0
			if fields[qsiAVAILABLE] == "up" {
				availability = 1.0
			}
			_, mapContainsKeyPartitionAvailable := metrics["PartitionAvailable"][partition]
			if !mapContainsKeyPartitionAvailable {
				metrics["PartitionAvailable"][partition] = []float64{}
			}
			metrics["PartitionAvailable"][partition] = append(metrics["PartitionAvailable"][partition], availability)

			// Partition cores per node
			_, mapContainsKeyPartitionCores := metrics["PartitionCores"][partition]
			if !mapContainsKeyPartitionCores {
				metrics["PartitionCores"][partition] = []float64{}
			}
			var cores, _ = strconv.ParseFloat(fields[qsiCORES], 64)
			metrics["PartitionCores"][partition] = append(metrics["PartitionCores"][partition], cores)

			// Partition cpus per core
			_, mapContainsKeyPartitionCpus := metrics["PartitionCpus"][partition]
			if !mapContainsKeyPartitionCpus {
				metrics["PartitionCpus"][partition] = []float64{}
			}
			var cpus, _ = strconv.ParseFloat(fields[qsiCPUS], 64)
			metrics["PartitionCpus"][partition] = append(metrics["PartitionCpus"][partition], cpus)

			// CPUs Load
			// Parse range
			_, mapContainsKeyPartitionCpusLoadLower := metrics["PartitionCpusLoadLower"][partition]
			if !mapContainsKeyPartitionCpusLoadLower {
				metrics["PartitionCpusLoadLower"][partition] = []float64{}
			}
			_, mapContainsKeyPartitionCpusLoadUpper := metrics["PartitionCpusLoadUpper"][partition]
			if !mapContainsKeyPartitionCpusLoadUpper {
				metrics["PartitionCpusLoadUpper"][partition] = []float64{}
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

			metrics["PartitionCpusLoadLower"][partition] = append(metrics["PartitionCpusLoadLower"][partition], cpusLoadLower)
			metrics["PartitionCpusLoadUpper"][partition] = append(metrics["PartitionCpusLoadUpper"][partition], cpusLoadUppper)

			// Alloc Mem
			_, mapContainsKeyPartitionAllocMem := metrics["PartitionAllocMem"][partition]
			if !mapContainsKeyPartitionAllocMem {
				metrics["PartitionAllocMem"][partition] = []float64{}
			}
			var allocMem, _ = strconv.ParseFloat(fields[qsiALLOCMEM], 64)
			metrics["PartitionAllocMem"][partition] = append(metrics["PartitionAllocMem"][partition], allocMem)

			// Nodes
			_, mapContainsKeyPartitionNodes := metrics["PartitionNodes"][partition]
			if !mapContainsKeyPartitionNodes {
				metrics["PartitionNodes"][partition] = []float64{}
			}
			var nodes, _ = strconv.ParseFloat(fields[qsiNODES], 64)
			metrics["PartitionNodes"][partition] = append(metrics["PartitionNodes"][partition], nodes)

			// Parse range
			_, mapContainsKeyPartitionFreeMemLower := metrics["PartitionFreeMemLower"][partition]
			if !mapContainsKeyPartitionFreeMemLower {
				metrics["PartitionFreeMemLower"][partition] = []float64{}
			}
			_, mapContainsKeyPartitionFreeMemUpper := metrics["PartitionFreeMemUpper"][partition]
			if !mapContainsKeyPartitionFreeMemUpper {
				metrics["PartitionFreeMemUpper"][partition] = []float64{}
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
			metrics["PartitionFreeMemLower"][partition] = append(metrics["PartitionFreeMemLower"][partition], freeMemLower)
			metrics["PartitionFreeMemUpper"][partition] = append(metrics["PartitionFreeMemUpper"][partition], freeMemUppper)

			// Memory
			_, mapContainsKeyPartitionMemory := metrics["PartitionMemory"][partition]
			if !mapContainsKeyPartitionMemory {
				metrics["PartitionMemory"][partition] = []float64{}
			}
			var memory, _ = strconv.ParseFloat(fields[qsiMEMORY], 64)
			metrics["PartitionMemory"][partition] = append(metrics["PartitionMemory"][partition], memory)

			//Node AIOT
			_, mapContainsKeyPartitionNodeAlloc := metrics["PartitionNodeAlloc"][partition]
			if !mapContainsKeyPartitionNodeAlloc {
				metrics["PartitionNodeAlloc"][partition] = []float64{}
			}
			_, mapContainsKeyPartitionNodeIdle := metrics["PartitionNodeIdle"][partition]
			if !mapContainsKeyPartitionNodeIdle {
				metrics["PartitionNodeIdle"][partition] = []float64{}
			}
			_, mapContainsKeyPartitionNodeOther := metrics["PartitionNodeOther"][partition]
			if !mapContainsKeyPartitionNodeOther {
				metrics["PartitionNodeOther"][partition] = []float64{}
			}
			_, mapContainsKeyPartitionNodeTotal := metrics["PartitionNodeTotal"][partition]
			if !mapContainsKeyPartitionNodeTotal {
				metrics["PartitionNodeTotal"][partition] = []float64{}
			}
			nodeaiot := strings.Split(fields[qsiNODEAIOT], "/")
			nodealloc, _ := strconv.ParseFloat(nodeaiot[0], 64)
			nodeidle, _ := strconv.ParseFloat(nodeaiot[1], 64)
			nodeother, _ := strconv.ParseFloat(nodeaiot[2], 64)
			nodetotal, _ := strconv.ParseFloat(nodeaiot[3], 64)
			metrics["PartitionNodeAlloc"][partition] = append(metrics["PartitionNodeAlloc"][partition], nodealloc)
			metrics["PartitionNodeIdle"][partition] = append(metrics["PartitionNodeIdle"][partition], nodeidle)
			metrics["PartitionNodeOther"][partition] = append(metrics["PartitionNodeOther"][partition], nodeother)
			metrics["PartitionNodeTotal"][partition] = append(metrics["PartitionNodeTotal"][partition], nodetotal)

			// Size
			// Parse range
			_, mapContainsKeyPartitionSizeLower := metrics["PartitionJobSizeLower"][partition]
			if !mapContainsKeyPartitionSizeLower {
				metrics["PartitionJobSizeLower"][partition] = []float64{}
			}
			_, mapContainsKeyPartitionSizeUpper := metrics["PartitionJobSizeUpper"][partition]
			if !mapContainsKeyPartitionSizeUpper {
				metrics["PartitionJobSizeUpper"][partition] = []float64{}
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
			metrics["PartitionJobSizeLower"][partition] = append(metrics["PartitionJobSizeLower"][partition], sizeLower)
			metrics["PartitionJobSizeUpper"][partition] = append(metrics["PartitionJobSizeUpper"][partition], sizeUpper)

			// Time
			// Parse time (day-hours:minute:second)
			metrics["PartitionTimeLimit"][partition] = append(metrics["PartitionTimeLimit"][partition], parseTime(fields[qsiTIME]))
		}
	}

	// Stats from squeue TODO
	averageMetrics := []string{
		"PartitionAvailable", "PartitionCores", "PartitionCpus", "PartitionCpusLoadLower",
		"PartitionCpusLoadUpper", "PartitionAllocMem", "PartitionFreeMemLower", "PartitionFreeMemUpper",
		"PartitionMemory", "PartitionJobSizeLower", "PartitionJobSizeUpper",
		"PartitionTimeLimit",
	}

	totalMetrics := []string{
		"PartitionNodes", "PartitionNodeAlloc", "PartitionNodeIdle",
		"PartitionNodeOther", "PartitionNodeTotal",
	}
	for metric, metricMap := range metrics {
		for partition, value := range metricMap {
			if stringInSlice(metric, averageMetrics) {
				sc.pMetrics[metric][partition], _ = stats.Mean(value)
			}
			if stringInSlice(metric, totalMetrics) {
				sc.pMetrics[metric][partition], _ = stats.Sum(value)
			}
		}

	}
	collected = uint(len(sc.pMetrics["PartitionAvailable"]))
	log.Infof("%d partition info collected", collected)
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

func squeueLineParser(line string) []string {
	fields := strings.Fields(line)

	if len(fields) < qsiFIELDS {
		log.Warnf("sinfo line parse failed (%s): %d fields expected, %d parsed", line, qsiFIELDS, len(fields))
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

// func sinfoLineParser(line string) []string {
// 	fields := strings.Fields(line)

// 	if len(fields) != iFIELDS {
// 		log.Warnf("sinfo line parse failed (%s): %d fields expected, %d parsed", line, iFIELDS, len(fields))
// 		return nil
// 	}

// 	return fields
// }

// func parseNodes(ns string) (float64, float64, float64, error) {

// 	nodesByStatus := strings.Split(ns, "/")
// 	if len(nodesByStatus) != iSTATESNUMBER {
// 		return 0, 0, 0, errors.New("Could not parse nodes: " + ns)
// 	}
// 	alloc, _ := strconv.ParseFloat(nodesByStatus[0], 64)
// 	idle, _ := strconv.ParseFloat(nodesByStatus[1], 64)
// 	total, _ := strconv.ParseFloat(nodesByStatus[3], 64)
// 	return idle, alloc, total, nil
// }

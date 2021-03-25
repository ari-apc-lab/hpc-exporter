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
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

const (
	iPARTITION = iota
	iAVAIL
	iSTATES
	iFIELDS
)

const (
	infoCommand   = "sinfo -h -o \"%20R %.5a %.20F\" | uniq"
	iSTATESNUMBER = 4
)

var iSTATESNAMES = [4]string{"allocated", "idle", "other", "total"}

func (sc *SlurmCollector) collectInfo(ch chan<- prometheus.Metric) {
	log.Debugln("Collecting Info metrics...")
	var collected uint

	// execute the command
	log.Debugln(infoCommand)
	sshSession, err := sc.executeSSHCommand(infoCommand)
	if sshSession != nil {
		defer sshSession.Close()
	}
	if err != nil {
		log.Warnln(err.Error())
		return
	}

	// wait for stdout to fill (it is being filled async by ssh)
	time.Sleep(1000 * time.Millisecond)
	inactivePartitions := sc.trackedPartitions
	nextLine := nextLineIterator(sshSession.OutBuffer, sinfoLineParser)
	for fields, err := nextLine(); err == nil; fields, err = nextLine() {
		// check the line is correctly parsed
		if err != nil {
			log.Warnf(err.Error())
			continue
		}
		partition := fields[iPARTITION]

		if _, ok := sc.gaugePartsTotalMap[partition]; !ok {

			sc.trackedPartitions = append(sc.trackedPartitions, partition)

			var const_Labels = map[string]string{
				"partition": partition,
			}

			sc.gaugePartsAvailMap[partition] = prometheus.NewGauge(prometheus.GaugeOpts{
				Namespace:   "Slurm",
				Subsystem:   "Partition",
				Name:        "Availability",
				Help:        "Availability of the partition",
				ConstLabels: const_Labels,
			})

			sc.gaugePartsIdleMap[partition] = prometheus.NewGauge(prometheus.GaugeOpts{
				Namespace:   "Slurm",
				Subsystem:   "Partition",
				Name:        "Idle",
				Help:        "Number of idle nodes in the partition",
				ConstLabels: const_Labels,
			})

			sc.gaugePartsAllocMap[partition] = prometheus.NewGauge(prometheus.GaugeOpts{
				Namespace:   "Slurm",
				Subsystem:   "Partition",
				Name:        "Allocated",
				Help:        "Number of allocated nodes in the partition",
				ConstLabels: const_Labels,
			})

			sc.gaugePartsTotalMap[partition] = prometheus.NewGauge(prometheus.GaugeOpts{
				Namespace:   "Slurm",
				Subsystem:   "Partition",
				Name:        "Total",
				Help:        "Number of total nodes in the partition",
				ConstLabels: const_Labels,
			})

		} else {
			inactivePartitions.remove(partition)
		}

		idle, allocated, total, err := parseNodes(fields[iSTATES])

		if err != nil {
			log.Warnf(err.Error())
		}

		sc.gaugePartsAvailMap[partition].Set(float64(PartitionStateDict[fields[iAVAIL]]))
		sc.gaugePartsIdleMap[partition].Set(idle)
		sc.gaugePartsAllocMap[partition].Set(allocated)
		sc.gaugePartsTotalMap[partition].Set(total)

		ch <- sc.gaugePartsAvailMap[partition]
		ch <- sc.gaugePartsIdleMap[partition]
		ch <- sc.gaugePartsAllocMap[partition]
		ch <- sc.gaugePartsTotalMap[partition]

		// send num of nodes per state and partition

		collected++

	}
	log.Infof("%d partition info collected", collected)
}

func sinfoLineParser(line string) []string {
	fields := strings.Fields(line)

	if len(fields) != iFIELDS {
		log.Warnf("sinfo line parse failed (%s): %d fields expected, %d parsed", line, iFIELDS, len(fields))
		return nil
	}

	return fields
}

func parseNodes(ns string) (float64, float64, float64, error) {

	nodesByStatus := strings.Split(ns, "/")
	if len(nodesByStatus) != 4 {
		return 0, 0, 0, errors.New("Could not parse nodes: " + ns)
	}
	idle, _ := strconv.ParseFloat(nodesByStatus[1], 64)
	alloc, _ := strconv.ParseFloat(nodesByStatus[0], 64)
	total, _ := strconv.ParseFloat(nodesByStatus[3], 64)
	return idle, alloc, total, nil
}

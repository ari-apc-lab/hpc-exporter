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

// sacct Slurm auxiliary collector

package slurm

import (
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

const (
	qJOBID = iota
	qNAME
	qUSER
	qPARTITION
	qSTATE
	qNCPUS
	qPENDING
	qWALLTIME
	qFIELDS
)

const (
	currentCommand = "squeue -h -a -X -O \"%12JobID,%20Name,%15User,%10Partition,State,NumCPUs,PendingTime,TimeUsed\" -P | grep 'PENDING' "
)

func (sc *SlurmCollector) collectQueu(ch chan<- prometheus.Metric) {
	log.Debugln("Collecting Acct metrics...")
	var collected uint
	sc.runningJobs = nil
	sshSession, err := sc.executeSSHCommand(currentCommand)
	if sshSession != nil {
		defer sshSession.Close()
	}
	if err != nil {
		log.Errorf("sacct: %s", err.Error())
		return
	}

	// wait for stdout to fill (it is being filled async by ssh)
	time.Sleep(1000 * time.Millisecond)

	nextLine := nextLineIterator(sshSession.OutBuffer, squeueLineParser)
	for fields, err := nextLine(); err == nil; fields, err = nextLine() {
		// check the line is correctly parsed
		if err != nil {
			log.Warnln(err.Error())
			continue
		}
		jobid := fields[qJOBID]
		state := fields[qSTATE]

		if state == "RUNNING" || state == "COMPLETING" {
			sc.runningJobs = append(sc.runningJobs, jobid)
		} else {
			ch <- prometheus.MustNewConstMetric(
				sc.descPtrMap["JobState"],
				prometheus.GaugeValue,
				float64(LongStatusDict[state]),
				jobid, fields[qNAME], fields[qUSER], fields[qPARTITION],
			)

			walltime := computeSlurmTime(fields[qWALLTIME])
			ch <- prometheus.MustNewConstMetric(
				sc.descPtrMap["JobWalltime"],
				prometheus.GaugeValue,
				walltime,
				jobid, fields[qNAME], fields[qUSER], fields[qPARTITION],
			)

			ncpus, _ := strconv.ParseFloat(fields[qNCPUS], 64)
			ch <- prometheus.MustNewConstMetric(
				sc.descPtrMap["JobNCPUs"],
				prometheus.GaugeValue,
				ncpus,
				jobid, fields[qNAME], fields[qUSER], fields[qPARTITION],
			)

			ch <- prometheus.MustNewConstMetric(
				sc.descPtrMap["JobVMEM"],
				prometheus.GaugeValue,
				0,
				jobid, fields[qNAME], fields[qUSER], fields[qPARTITION],
			)

			wait, _ := strconv.ParseFloat(fields[accSUBMIT], 64)
			ch <- prometheus.MustNewConstMetric(
				sc.descPtrMap["JobQueued"],
				prometheus.GaugeValue,
				wait,
				jobid, fields[qNAME], fields[qUSER], fields[qPARTITION],
			)
		}

	}
	collected++
	log.Infof("%d queued jobs collected", collected)
}

func squeueLineParser(line string) []string {
	fields := strings.Fields(line)

	if len(fields) < accFIELDS {
		log.Warnf("squeue line parse failed (%s): %d fields expected, %d parsed", line, qFIELDS, len(fields))
		return nil
	}

	return fields
}

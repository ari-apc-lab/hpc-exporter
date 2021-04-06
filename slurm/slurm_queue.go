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
	"hpc_exporter/ssh"
	"strconv"
	"strings"
	"time"

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

func (sc *SlurmCollector) collectQueu() {
	log.Debugln("Collecting Queue metrics...")
	var collected uint
	sc.runningJobs = nil

	session, err := sc.openSession()
	defer closeSession(session)

	if err != nil {
		log.Errorf("Error opening session for squeue: %s ", err.Error())
		return
	}

	queueCommand := &ssh.SSHCommand{
		Path: "squeue -h -a -X -O \"%12JobID,%20Name,%15User,%10Partition,State,NumCPUs,PendingTime,TimeUsed\" -P | grep 'PENDING' ",
	}
	err = session.RunCommand(queueCommand)

	if err != nil {
		log.Errorf("squeue command failed: %s", err.Error())
		return
	}

	// wait for stdout to fill (it is being filled async by ssh)
	time.Sleep(1000 * time.Millisecond)

	nextLine := nextLineIterator(session.OutBuffer, squeueLineParser)
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
			sc.trackedJobs[jobid] = true
			sc.labels["JobName"][jobid] = fields[qNAME]
			sc.labels["JobUser"][jobid] = fields[qUSER]
			sc.labels["JobPartition"][jobid] = fields[qPARTITION]

			sc.jobMetrics["JobState"][jobid] = float64(LongStatusDict[state])
			sc.jobMetrics["JobWalltime"][jobid] = computeSlurmTime(fields[qWALLTIME])
			sc.jobMetrics["JobNCPUs"][jobid], _ = strconv.ParseFloat(fields[qNCPUS], 64)
			sc.jobMetrics["JobQueued"][jobid], _ = strconv.ParseFloat(fields[accSUBMIT], 64)
			sc.jobMetrics["JobVMEM"][jobid] = 0
			sc.jobMetrics["JobRSS"][jobid] = 0

		}

	}
	collected++
	log.Infof("%d queued jobs collected", collected)
	return
}

func squeueLineParser(line string) []string {
	fields := strings.Fields(line)

	if len(fields) < accFIELDS {
		log.Warnf("squeue line parse failed (%s): %d fields expected, %d parsed", line, qFIELDS, len(fields))
		return nil
	}

	return fields
}

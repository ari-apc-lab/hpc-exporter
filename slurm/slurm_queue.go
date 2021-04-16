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
	qUSERNAME
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

	queueCommand := "squeue -h -a -O \"JobID:12,Name:20,UserName:15,Partition:15,State:13,NumCPUs:7,SubmitTime:20,TimeUsed:.13\" -P "
	session := ssh.ExecuteSSHCommand(queueCommand, sc.sshClient)
	if session != nil {
		defer session.CloseSession()
	} else {
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
			sc.labels["JobUser"][jobid] = fields[qUSERNAME]
			sc.labels["JobPart"][jobid] = fields[qPARTITION]

			submit, _ := time.Parse(time.RFC3339, fields[qPENDING]+"Z")
			sc.jobMetrics["JobState"][jobid] = float64(LongStatusDict[state])
			sc.jobMetrics["JobWalltime"][jobid] = computeSlurmTime(fields[qWALLTIME])
			sc.jobMetrics["JobNCPUs"][jobid], _ = strconv.ParseFloat(fields[qNCPUS], 64)
			sc.jobMetrics["JobQueued"][jobid] = float64(time.Since(submit)) / 9
			sc.jobMetrics["JobVMEM"][jobid] = 0
			sc.jobMetrics["JobRSS"][jobid] = 0

		}
		collected++
	}

	log.Infof("%d queued jobs collected", collected)
}

func squeueLineParser(line string) []string {
	fields := strings.Fields(line)

	if len(fields) < qFIELDS {
		log.Warnf("squeue line parse failed (%s): %d fields expected, %d parsed", line, qFIELDS, len(fields))
		return nil
	}

	return fields
}

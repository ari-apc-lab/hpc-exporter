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
	accJOBID = iota
	accNAME
	accUSERNAME
	accPARTITION
	accSTATE
	accNCPUS
	accRESERVED
	accELAPSED
	accVMEM
	accRSS
	accFIELDS
)

func (sc *SlurmCollector) collectAcct() {
	log.Debugln("Collecting Acct metrics...")
	var collected uint

	startTime := getstarttime(sc.sacctHistory)
	acctCommand := "sacct -n -X -o \"JobIDRaw,JobName,User,Partition,State,NCPUS,Reserved,ElapsedRaw,MaxVMSize,MaxRSS\" -p -S " + startTime

	session := ssh.ExecuteSSHCommand(acctCommand, sc.sshClient)
	if session != nil {
		defer session.CloseSession()
	} else {
		return
	}

	// wait for stdout to fill (it is being filled async by ssh)
	time.Sleep(1000 * time.Millisecond)

	nextLine := nextLineIterator(session.OutBuffer, sacctLineParser)
	for fields, err := nextLine(); err == nil; fields, err = nextLine() {
		// check the line is correctly parsed
		if err != nil {
			log.Warnln(err.Error())
			continue
		}
		jobid := fields[accJOBID]
		state := strings.Fields(fields[accSTATE])[0]
		if (state == "RUNNING" || state == "COMPLETING") && notContains(sc.runningJobs, jobid) {
			continue
		}

		sc.trackedJobs[jobid] = true
		sc.jLabels["job_id"][jobid] = jobid
		sc.jLabels["job_name"][jobid] = fields[accNAME]
		sc.jLabels["job_user"][jobid] = fields[accUSERNAME]
		sc.jLabels["job_partition"][jobid] = fields[accPARTITION]

		sc.jMetrics["JobState"][jobid] = float64(LongStatusDict[state])
		sc.jMetrics["JobWalltime"][jobid], _ = strconv.ParseFloat(fields[accELAPSED], 64)
		sc.jMetrics["JobNCPUs"][jobid], _ = strconv.ParseFloat(fields[accNCPUS], 64)
		sc.jMetrics["JobQueued"][jobid] = computeSlurmTime(fields[accRESERVED])
		sc.jMetrics["JobVMEM"][jobid], _ = strconv.ParseFloat(fields[accNCPUS], 64)
		sc.jMetrics["JobRSS"][jobid] = parseMem(fields[accRSS])
		collected++
	}

	log.Infof("%d finished jobs collected", collected)
}

func sacctLineParser(line string) []string {
	fields := strings.Split(line, "|")

	if len(fields) < accFIELDS {
		log.Warnf("sacct line parse failed (%s): %d fields expected, %d parsed", line, accFIELDS, len(fields))
		return nil
	}

	return fields
}

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
	"fmt"
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
	accSUBMIT
	accELAPSED
	accVMEM
	accRSS
	accFIELDS
)

func (sc *SlurmCollector) collectAcct() {
	log.Debugln("Collecting Acct metrics...")
	var collected uint
	session, err := sc.openSession()
	defer closeSession(session)

	if err != nil {
		log.Errorf("Error opening session for sacct: %s ", err.Error())
		return
	}
	startTime := getstarttime(sc.sacctHistory)
	sacctCommand := &ssh.SSHCommand{
		Path: "sacct -n -X -o \"JobIDRaw,JobName%20,User%15,Partition%10,State%14,NCPUS%6,Submit%23,ElapsedRaw%7,MaxVMSize%10,MaxRSS%10\" -S " + startTime + " | grep -v 'PENDING' ",
	}

	err = session.RunCommand(sacctCommand)
	if err != nil {
		log.Errorf("sacct command failed: %s", err.Error())
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
		state := fields[accSTATE]

		if (state == "RUNNING" || state == "COMPLETING") && notContains(sc.runningJobs, jobid) {
			continue
		}

		sc.trackedJobs[jobid] = true
		sc.labels["JobName"][jobid] = fields[accNAME]
		sc.labels["JobUser"][jobid] = fields[accUSERNAME]
		sc.labels["JobPartition"][jobid] = fields[accPARTITION]

		submit, _ := time.Parse("RFC3339", fields[accSUBMIT]+"Z")

		sc.jobMetrics["JobState"][jobid] = float64(LongStatusDict[state])
		sc.jobMetrics["JobWalltime"][jobid], _ = strconv.ParseFloat(fields[accELAPSED], 64)
		sc.jobMetrics["JobNCPUs"][jobid], _ = strconv.ParseFloat(fields[accNCPUS], 64)
		sc.jobMetrics["JobQueued"][jobid] = float64(time.Now().Unix()) - float64(submit.Unix()) - sc.jobMetrics["JobWalltime"][jobid]
		sc.jobMetrics["JobVMEM"][jobid], _ = strconv.ParseFloat(fields[accNCPUS], 64)
		sc.jobMetrics["JobRSS"][jobid] = parseMem(fields[accRSS])
	}
	collected++
	log.Infof("%d finished jobs collected", collected)
	return
}

func sacctLineParser(line string) []string {
	fields := strings.Fields(line)

	if len(fields) < accFIELDS {
		log.Warnf("sacct line parse failed (%s): %d fields expected, %d parsed", line, accFIELDS, len(fields))
		return nil
	}

	return fields
}

func getstarttime(days int) string {

	start := time.Now().AddDate(0, 0, days)
	hour := start.Hour()
	minute := start.Minute()
	second := start.Second()
	day := start.Day()
	month := start.Month()
	year := start.Year()

	str := fmt.Sprintf("%4d-%2d-%2dT%2d:%2d:%2d", year, month, day, hour, minute, second)

	return str
}

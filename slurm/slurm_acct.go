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
	"hpc_exporter/helper"
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
	accNNODES
	accRESERVED
	accELAPSED
	accEXITCODE
	accCONSUMEDENERGY
	accCPUTIME
	accSUBMIT
	accEND
	accPRIORITY
	accQOS
	accTIMELIMIT
	accFIELDS
)

func (sc *SlurmCollector) collectAcct() {
	log.Infof("Collecting metrics for jobs %s in host %s", sc.targetJobIds, sc.sshConfig.Host)
	log.Debugln("Collecting metrics using acct command ...")
	var collected uint

	startTime := getstarttime(sc.sacctHistory)
	acctCommand := "sacct -n -X -o \"JobIDRaw,JobName,User,Partition,State,NCPUS,NNodes,Reserved,ElapsedRaw,ExitCode,ConsumedEnergyRaw,CPUTimeRaw,Submit,End,Priority,QoS,TimelimitRaw\" -p"

	if sc.targetJobIds != "" {
		acctCommand += " -j \"" + sc.targetJobIds + "\""
	} else {
		acctCommand += " -S " + startTime
	}

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
		if (sc.targetJobIds == "") && (state == "RUNNING" || state == "COMPLETING") && notContains(sc.runningJobs, jobid) {
			continue
		}

		//sc.trackedJobs[jobid] = true
		sc.jLabels["job_id"][jobid] = jobid
		sc.jLabels["job_name"][jobid] = fields[accNAME]
		sc.jLabels["job_user"][jobid] = fields[accUSERNAME]
		sc.jLabels["job_partition"][jobid] = fields[accPARTITION]
		sc.jLabels["job_priority"][jobid] = fields[accPRIORITY]
		sc.jLabels["job_qos"][jobid] = fields[accQOS]
		sc.jLabels["job_time_limit"][jobid] = fields[accTIMELIMIT]
		sc.jLabels["job_submit_time"][jobid] = computeSlurmAcctTimeForLabel(fields[accSUBMIT])
		

		sc.jMetrics["JobState"][jobid] = float64(LongStatusDict[state])
		sc.jMetrics["JobNCPUs"][jobid], _ = strconv.ParseFloat(fields[accNCPUS], 64)
		sc.jMetrics["JobNNodes"][jobid], _ = strconv.ParseFloat(fields[accNNODES], 64)
		sc.jMetrics["JobConsumedEnergy"][jobid], _ = strconv.ParseFloat(fields[accCONSUMEDENERGY], 64)
		sc.jMetrics["JobCPUTime"][jobid], _ = strconv.ParseFloat(fields[accCPUTIME], 64)
		sc.jMetrics["JobElapsetime"][jobid], _ = strconv.ParseFloat(fields[accELAPSED], 64)
		sc.jMetrics["JobExitCode"][jobid], sc.jMetrics["JobExitSignal"][jobid] = slurmExitCode(fields[accEXITCODE])
		sc.jMetrics["JobReserved"][jobid] = computeSlurmTime(fields[accRESERVED])
		end_time := fields[accEND]
		if end_time != "Unknown" {
			sc.jMetrics["JobEndTime"][jobid] = computeSlurmDateTime(end_time)
		}

		collected++
		// Remove jobid from list of jobs (sc.JobIds) if state is one of terminating states (e.g. COMPLETED, FAILED)
		// and end time is collected
		if helper.ListContainsElement(SLURM_Terminating_States, state) && strings.TrimSpace(end_time) != "Unknown" {
			log.Infof("Job with id %s ending with state %s at time %s. Removed for list of jobs to monitor",
				jobid, state, end_time)
			sc.TrackedJobs[jobid]--
		}
	}

	if sc.targetJobIds == "" {
		log.Infof("%d finished jobs collected", collected)
	} else {
		log.Infof("%d jobs collected", collected)
	}

}

func sacctLineParser(line string) []string {
	fields := strings.Split(line, "|")

	if len(fields) < accFIELDS {
		log.Warnf("sacct line parse failed (%s): %d fields expected, %d parsed", line, accFIELDS, len(fields))
		return nil
	}

	return fields
}

func slurmExitCode(s string) (float64, float64) {
	spl := strings.Split(s, ":")
	if exitCode, err := strconv.ParseFloat(spl[0], 64); err != nil {
		return -50, -50
	} else if exitSignal, err2 := strconv.ParseFloat(spl[1], 64); err2 != nil {
		return -50, -50
	} else {
		return exitCode, exitSignal
	}
}

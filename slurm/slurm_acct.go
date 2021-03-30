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
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

const (
	accJOBID = iota
	accNAME
	accUSERNAME
	accPARTITION
	accSTATE
	accVMEM
	accNCPUS
	accSUBMIT
	accELAPSED
	accFIELDS
)

func (sc *SlurmCollector) collectAcct(ch chan<- prometheus.Metric) {
	log.Debugln("Collecting Acct metrics...")
	var collected uint

	startTime := getstarttime(sc.sacctHistory)
	sacctCommand := "sacct -n -X -o \"JobIDRaw,JobName%20,User%15,Partition%10,State%14,AveVMSize%10,NCPUS%6,Submit%23,ElapsedRaw%7\" -S " + startTime + " | grep -v 'PENDING' "
	sshSession, err := sc.executeSSHCommand(sacctCommand)
	if sshSession != nil {
		defer sshSession.Close()
	}
	if err != nil {
		log.Errorf("sacct: %s", err.Error())
		return
	}

	// wait for stdout to fill (it is being filled async by ssh)
	time.Sleep(1000 * time.Millisecond)

	nextLine := nextLineIterator(sshSession.OutBuffer, sacctLineParser)
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
		ch <- prometheus.MustNewConstMetric(
			sc.descPtrMap["JobState"],
			prometheus.GaugeValue,
			float64(LongStatusDict[fields[accSTATE]]),
			fields[accJOBID], fields[accNAME], fields[accUSERNAME], fields[accPARTITION],
		)

		walltime, _ := strconv.ParseFloat(fields[accELAPSED], 64)
		ch <- prometheus.MustNewConstMetric(
			sc.descPtrMap["JobWalltime"],
			prometheus.GaugeValue,
			walltime,
			fields[accJOBID], fields[accNAME], fields[accUSERNAME], fields[accPARTITION],
		)

		ncpus, _ := strconv.ParseFloat(fields[accNCPUS], 64)
		ch <- prometheus.MustNewConstMetric(
			sc.descPtrMap["JobNCPUs"],
			prometheus.GaugeValue,
			ncpus,
			fields[accJOBID], fields[accNAME], fields[accUSERNAME], fields[accPARTITION],
		)

		vmem, _ := strconv.ParseFloat(fields[accVMEM], 64)
		ch <- prometheus.MustNewConstMetric(
			sc.descPtrMap["JobVMEM"],
			prometheus.GaugeValue,
			vmem,
			fields[accJOBID], fields[accNAME], fields[accUSERNAME], fields[accPARTITION],
		)

		submit, _ := time.Parse("RFC3339", fields[accSUBMIT]+"Z")
		queued := float64(time.Now().Unix()) - float64(submit.Unix()) - walltime
		ch <- prometheus.MustNewConstMetric(
			sc.descPtrMap["JobQueued"],
			prometheus.GaugeValue,
			queued,
			fields[accJOBID], fields[accNAME], fields[accUSERNAME], fields[accPARTITION],
		)

	}
	collected++
	log.Infof("%d finished jobs collected", collected)
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

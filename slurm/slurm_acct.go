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

const (
	currentCommand = "sacct -n -a -X -o \"JobIDRaw,JobName,User,Partition,State,AveVMSize,NCPUS,Submit,Elapsed\" -S00:00:00"
)

func (sc *SlurmCollector) collectAcct(ch chan<- prometheus.Metric) {
	log.Debugln("Collecting Acct metrics...")
	var collected uint

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
	inactiveJobs := sc.trackedJobs

	nextLine := nextLineIterator(sshSession.OutBuffer, sacctLineParser)
	for fields, err := nextLine(); err == nil; fields, err = nextLine() {
		// check the line is correctly parsed
		if err != nil {
			log.Warnln(err.Error())
			continue
		}

		// parse and send job state
		jobid := fields[accJOBID]

		if _, ok := sc.gaugeJobsStatusMap[jobid]; !ok {

			sc.trackedJobs = append(sc.trackedJobs, jobid)

			var const_Labels = map[string](string){
				"jobid":     jobid,
				"jobname":   fields[accNAME],
				"user":      fields[accUSERNAME],
				"partition": fields[accPARTITION],
			}

			sc.gaugeJobsStatusMap[jobid] = prometheus.NewGauge(prometheus.GaugeOpts{
				Namespace:   "Slurm",
				Subsystem:   "JobID",
				Name:        "Status",
				Help:        "Status of a Job",
				ConstLabels: const_Labels,
			})

			sc.gaugeJobsElapsedMap[jobid] = prometheus.NewGauge(prometheus.GaugeOpts{
				Namespace:   "Slurm",
				Subsystem:   "JobID",
				Name:        "Elapsed",
				Help:        "Elapsed time since the job started running in seconds",
				ConstLabels: const_Labels,
			})

			sc.gaugeJobsNCPUSMap[jobid] = prometheus.NewGauge(prometheus.GaugeOpts{
				Namespace:   "Slurm",
				Subsystem:   "JobID",
				Name:        "NCPUs",
				Help:        "Number of CPUs assigned to the job",
				ConstLabels: const_Labels,
			})

			sc.gaugeJobsVMEMOMap[jobid] = prometheus.NewGauge(prometheus.GaugeOpts{
				Namespace:   "Slurm",
				Subsystem:   "JobID",
				Name:        "VMemAvg",
				Help:        "Average virtual memory occupied by the job",
				ConstLabels: const_Labels,
			})

			sc.gaugeJobsSUBMITMap[jobid] = prometheus.NewGauge(prometheus.GaugeOpts{
				Namespace:   "Slurm",
				Subsystem:   "JobID",
				Name:        "SubmitTime",
				Help:        "Time in seconds since the job was submitted",
				ConstLabels: const_Labels,
			})

		} else {
			inactiveJobs.remove(jobid)
		}
		sc.gaugeJobsStatusMap[jobid].Set(float64(LongStatusDict[fields[accSTATE]]))
		sc.gaugeJobsElapsedMap[jobid].Set(computeSlurmTime(fields[accELAPSED]))
		sc.gaugeJobsVMEMOMap[jobid].Set(strconv.ParseFloat(fields[accVMEM], 64))
		sc.gaugeJobsNCPUSMap[jobid].Set(strconv.ParseFloat(fields[accNCPUS], 64))
		sc.gaugeJobsSUBMITMap[jobid].Set(computeSlurmTime(fields[accSUBMIT]))
		ch <- sc.gaugeJobsStatusMap[jobid]
		ch <- sc.gaugeJobsElapsedMap[jobid]
		ch <- sc.gaugeJobsNCPUSMap[jobid]
		ch <- sc.gaugeJobsVMEMOMap[jobid]
		ch <- sc.gaugeJobsSUBMITMap[jobid]

	}
	collected++
	log.Infof("%d finished jobs collected", collected)
	deleteJobs(inactiveJobs, sc)
}

func sacctLineParser(line string) []string {
	fields := strings.Fields(line)

	if len(fields) < aFIELDS {
		log.Warnf("sacct line parse failed (%s): %d fields expected, %d parsed", line, aFIELDS, len(fields))
		return nil
	}

	return fields
}

func computeSlurmTime(timeField string) float64 {

	split_walltime := strings.Split(timeField, ":")

	walltime_dd := 0.0
	walltime_hh := 0.0
	walltime_mm := 0.0
	walltime_ss := 0.0

	switch numfields := len(split_walltime); numfields {
	case 2:
		walltime_mm, _ = strconv.ParseFloat(split_walltime[0], 64)
		walltime_ss, _ = strconv.ParseFloat(split_walltime[1], 64)
	case 3:
		walltime_mm, _ = strconv.ParseFloat(split_walltime[1], 64)
		walltime_ss, _ = strconv.ParseFloat(split_walltime[2], 64)
		split_dh_walltime := strings.Split(split_walltime[0], "-")
		switch innernumfields := len(split_dh_walltime); innernumfields {
		case 1:
			walltime_hh, _ = strconv.ParseFloat(split_dh_walltime[0], 64)
		case 2:
			walltime_dd, _ = strconv.ParseFloat(split_dh_walltime[0], 64)
			walltime_hh, _ = strconv.ParseFloat(split_dh_walltime[1], 64)
		}
	}

	walltime := walltime_dd*86400.0 + walltime_hh*3600.0 + walltime_mm*60.0 + walltime_ss
	return walltime
}

func (iJ *trackedList) remove(job string) {
	for i, v := range *iJ {
		if v == job {
			append(*iJ[:i], iJ[i+1:])
		}
	}
}

func deleteJobs(inactiveJobs []string, sc *SlurmCollector) {
	for i, job := range inactiveJobs {
		sc.trackedJobs = append(sc.trackedJobs[:i], sc.trackedJobs[(i+1):])
		delete(sc.gaugeJobsStatusMap, job)
		delete(sc.gaugeJobsElapsedMap, job)
		delete(sc.gaugeJobsVMEMOMap, job)
		delete(sc.gaugeJobsNCPUSMap, job)
		delete(sc.gaugeJobsSUBMITMap, job)
	}
}

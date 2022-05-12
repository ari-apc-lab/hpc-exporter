package pbs

import (
	"hpc_exporter/helper"
	"hpc_exporter/ssh"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

func (pc *PBSCollector) collectJobs(ch chan<- prometheus.Metric) {

	log.Debugln("Collecting Job metrics...")

	jobCommand := "qstat -f1"

	sshSession := ssh.ExecuteSSHCommand(jobCommand, pc.sshClient)
	if sshSession != nil {
		defer sshSession.Close()
	} else {
		return
	}

	// wait for stdout to fill (it is being filled async by ssh)
	time.Sleep(100 * time.Millisecond)

	buffer := sshSession.OutBuffer

	mapJobs, err := parseBlocks(buffer)
	if err != nil {
		log.Warnln("There was an error pasing the Job metrics: %s", err.Error())
	}
	collected := 0
	// Remove all jobIds from collector if mapJobs is empty (no jobs were detected in target HPC).
	if len(mapJobs) == 0 {
		pc.JobIds = []string{}
	} else {
		// Remove jobIds not included in mapJobs from collector (some target jobs were not detected)
		var existing_jobs []string
		for _, id := range pc.JobIds {
			_, id_exists := mapJobs[id]
			if id_exists {
				existing_jobs = append(existing_jobs, id)
			}
		}
		pc.JobIds = existing_jobs
	}

	for jobid, mapMetrics := range mapJobs {
		if pc.targetJobIds == "" || strings.Contains(pc.targetJobIds, jobid) {
			pc.trackedJobs[jobid] = true
			var startTime, createdTime time.Time
			var state string
			pc.clearjMetrics(jobid)
			pc.jLabels["job_id"][jobid] = jobid
			for key, value := range mapMetrics {
				switch key {
				case "Job_Name":
					pc.jLabels["job_name"][jobid] = value

				case "euser":
					pc.jLabels["job_user"][jobid] = value

				case "queue":
					pc.jLabels["job_queue"][jobid] = value

				case "job_state":
					state = value
					pc.jMetrics["JobState"][jobid] = float64(StatusDict[value])

				case "Priority":
					pc.jMetrics["JobPriority"][jobid], _ = strconv.ParseFloat(value, 64)

				case "resources_used.walltime":
					pc.jMetrics["JobWalltimeUsed"][jobid], _ = parsePBSTime(value)

				case "Resource_List.walltime":
					pc.jMetrics["JobWalltimeMax"][jobid], _ = parsePBSTime(value)

				case "Walltime.Remaining":
					pc.jMetrics["JobWalltimeRem"][jobid], _ = strconv.ParseFloat(value, 64)

				case "resources_used.cput":
					pc.jMetrics["JobCPUTime"][jobid], _ = parsePBSTime(value)

				case "resources_used.vmem":
					if mem, err := parseMem(value); err == nil {
						pc.jMetrics["JobVMEM"][jobid] = mem
					}

				case "resources_used.mem":
					if mem, err := parseMem(value); err == nil {
						pc.jMetrics["JobRSS"][jobid] = mem
					}

				case "exit_status":
					pc.jMetrics["JobExitStatus"][jobid], _ = strconv.ParseFloat(value, 64)

				case "req_information.task_usage.0.task.0.threads ":
					pc.jMetrics["JobNCPUs"][jobid], _ = strconv.ParseFloat(value, 64)

				case "ctime":
					createdTime, _ = parsePBSDateTime(value)

				case "start_time":
					startTime, _ = parsePBSDateTime(value)
				}

			}
			pc.jMetrics["JobQueued"][jobid] = startTime.Sub(createdTime).Seconds()
			collected++
			// Remove jobid from list of jobs (sc.JobIds) if state is one of terminating states (e.g. COMPLETED, FAILED)
			if helper.ListContainsElement(PBS_Terminating_States, state) {
				pc.JobIds = helper.DeleteArrayEntry(pc.JobIds, jobid)
			}
		}
	}

	log.Infof("Collected jobs: %d", collected)
}

func (pc *PBSCollector) clearjMetrics(jobid string) {
	/*We give default values to all the metrics, they will be overwritten with the collected values if they exist, otherwise they keep these values.
	For example, if the job hasn't started running, the memory metrics will stay at 0. This way we also ensure that all the metrics exist as well*/
	pc.jMetrics["JobState"][jobid] = -1
	pc.jMetrics["JobPriority"][jobid] = -1
	pc.jMetrics["JobWalltimeUsed"][jobid] = 0
	pc.jMetrics["JobWalltimeMax"][jobid] = 0
	pc.jMetrics["JobWalltimeRem"][jobid] = -1
	pc.jMetrics["JobCPUTime"][jobid] = 0
	pc.jMetrics["JobVMEM"][jobid] = 0
	pc.jMetrics["JobRSS"][jobid] = 0
	pc.jMetrics["JobExitStatus"][jobid] = -50
	pc.jMetrics["JobNCPUs"][jobid] = 0
	pc.jMetrics["JobQueued"][jobid] = 0
}

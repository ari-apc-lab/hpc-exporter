package pbs

import (
	"hpc_exporter/ssh"
	"strconv"
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

	for jobid, mapMetrics := range mapJobs {
		pc.trackedJobs[jobid] = true
		var startTime, createdTime time.Time
		pc.clearMetrics(jobid)
		for key, value := range mapMetrics {
			switch key {
			case "Job_Name":
				pc.labels["JobName"][jobid] = value

			case "euser":
				pc.labels["JobUser"][jobid] = value

			case "queue":
				pc.labels["JobQueue"][jobid] = value

			case "job_state":
				pc.jobMetrics["JobState"][jobid] = float64(StatusDict[value])

			case "Priority":
				pc.jobMetrics["JobPriority"][jobid], _ = strconv.ParseFloat(value, 64)

			case "resources_used.walltime":
				pc.jobMetrics["JobWalltimeUsed"][jobid], _ = parsePBSTime(value)

			case "Resource_List.walltime":
				pc.jobMetrics["JobWalltimeMax"][jobid], _ = parsePBSTime(value)

			case " Walltime.Remaining":
				pc.jobMetrics["JobWalltimeRem"][jobid], _ = strconv.ParseFloat(value, 64)

			case "resources_used.cput":
				pc.jobMetrics["JobCPUTime"][jobid], _ = parsePBSTime(value)

			case "resources_used.vmem":
				if mem, err := parseMem(value); err == nil {
					pc.jobMetrics["JobVMEM"][jobid] = mem
				}

			case "resources_used.mem":
				if mem, err := parseMem(value); err == nil {
					pc.jobMetrics["JobRSS"][jobid] = mem
				}

			case "exit_status":
				pc.jobMetrics["JobExitStatus"][jobid], _ = strconv.ParseFloat(value, 64)

			case "req_information.task_usage.0.task.0.threads ":
				pc.jobMetrics["JobNCPUs"][jobid], _ = strconv.ParseFloat(value, 64)

			case "ctime":
				createdTime, _ = parsePBSDateTime(value)

			case "start_time":
				startTime, _ = parsePBSDateTime(value)
			}

		}
		pc.jobMetrics["JobQueued"][jobid] = startTime.Sub(createdTime).Seconds()
	}

	log.Infof("Collected jobs: %d", len(mapJobs))
}

func (pc *PBSCollector) clearMetrics(jobid string) {
	/*We give default values to all the metrics, they will be overwritten with the collected values if they exist, otherwise they keep these values.
	For example, if the job hasn't started running, the memory metrics will stay at 0. This way we also ensure that all the metrics exist as well*/
	pc.jobMetrics["JobState"][jobid] = -1
	pc.jobMetrics["JobPriority"][jobid] = -1
	pc.jobMetrics["JobWalltimeUsed"][jobid] = 0
	pc.jobMetrics["JobWalltimeMax"][jobid] = 0
	pc.jobMetrics["JobWalltimeRem"][jobid] = -1
	pc.jobMetrics["JobCPUTime"][jobid] = 0
	pc.jobMetrics["JobVMEM"][jobid] = 0
	pc.jobMetrics["JobRSS"][jobid] = 0
	pc.jobMetrics["JobExitStatus"][jobid] = -1
	pc.jobMetrics["JobNCPUs"][jobid] = 0
	pc.jobMetrics["JobQueued"][jobid] = 0
}

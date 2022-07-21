package pbs

import (
	"hpc_exporter/ssh"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

func (pc *PBSCollector) collectQueues(ch chan<- prometheus.Metric) {

	log.Debugln("Collecting Queue metrics...")

	queueCommand := "source /etc/profile; qstat -Q -f -w"

	sshSession := ssh.ExecuteSSHCommand(queueCommand, pc.sshClient)
	if sshSession != nil {
		defer sshSession.Close()
	} else {
		return
	}

	// wait for stdout to fill (it is being filled async by ssh)
	time.Sleep(100 * time.Millisecond)

	buffer := sshSession.OutBuffer

	mapQueues, err := parseBlocks(buffer)
	if err != nil {
		log.Warnln("There was an error pasing the Queue metrics: %s", err.Error())
	}

	for queue, mapMetrics := range mapQueues {
		pc.clearQueueMetrics(queue)
		pc.qLabels["queue_name"][queue] = queue
		for key, value := range mapMetrics {
			switch key {
			case "queue_type":
				pc.qLabels["queue_type"][queue] = value
			case "total_jobs":
				pc.qMetrics["QueueTotal"][queue], _ = strconv.ParseFloat(value, 64)
			case "state_count":
				chunks := strings.Split(value, " ")
				for _, chunk := range chunks {
					pair := strings.Split(chunk, ":")
					pc.qMetrics["Queue"+pair[0]][queue], _ = strconv.ParseFloat(pair[1], 64)
				}
			case "max_queuable":
				pc.qMetrics["QueueMax"][queue], _ = strconv.ParseFloat(value, 64)
			case "enabled":
				if value == "True" {
					pc.qMetrics["QueueEnabled"][queue] = 1
				} else {
					pc.qMetrics["QueueEnabled"][queue] = 0
				}
			case "started":
				if value == "True" {
					pc.qMetrics["QueueStarted"][queue] = 1
				} else {
					pc.qMetrics["QueueStarted"][queue] = 0
				}
			case "Priority":
				pc.qMetrics["QueuePriority"][queue], _ = strconv.ParseFloat(value, 64)
			case "resources_min.nodecounter":
				pc.qMetrics["QueueMinNodes"][queue], _ = strconv.ParseFloat(value, 64)
			case "resources_max.nodecounter":
				pc.qMetrics["QueueMaxNodes"][queue], _ = strconv.ParseFloat(value, 64)
			case "resources_max.walltime":
				pc.qMetrics["QueueMaxWalltime"][queue], _ = parsePBSTime(value)
			case "resources_available.nodecounter":
				pc.qMetrics["QueueAvailNodes"][queue], _ = strconv.ParseFloat(value, 64)
			case "resources_assigned.nodecounter":
				pc.qMetrics["QueueAssignNodes"][queue], _ = strconv.ParseFloat(value, 64)
			case "resources_assigned.ncpus":
				pc.qMetrics["QueueAssignCpus"][queue], _ = strconv.ParseFloat(value, 64)
			}
		}
	}

	log.Infof("Collected queues: %d", len(mapQueues))
}

func (pc *PBSCollector) clearQueueMetrics(queue string) {
	/*We give default values to all the metrics, they will be overwritten with the collected values if they exist, otherwise they keep these values.
	For example, if the job hasn't started running, the memory metrics will stay at 0. This way we also ensure that all the metrics exist as well*/
	pc.qMetrics["QueueTotal"][queue] = -1
	pc.qMetrics["QueueMax"][queue] = -1
	pc.qMetrics["QueueEnabled"][queue] = -1
	pc.qMetrics["QueueStarted"][queue] = -1
	pc.qMetrics["QueueQueued"][queue] = -1
	pc.qMetrics["QueueBegun"][queue] = -1
	pc.qMetrics["QueueRunning"][queue] = -1
	pc.qMetrics["QueueHeld"][queue] = -1
	pc.qMetrics["QueueWaiting"][queue] = -1
	pc.qMetrics["QueueTransit"][queue] = -1
	pc.qMetrics["QueueExiting"][queue] = -1
	pc.qMetrics["QueueComplete"][queue] = -1
	pc.qMetrics["QueuePriority"][queue] = -1
	pc.qMetrics["QueueMinNodes"][queue] = -1
	pc.qMetrics["QueueMaxNodes"][queue] = -1
	pc.qMetrics["QueueMaxWalltime"][queue] = -1
	pc.qMetrics["QueueAvailNodes"][queue] = -1
	pc.qMetrics["QueueAssignNodes"][queue] = -1
	pc.qMetrics["QueueAssignCpus"][queue] = -1
}

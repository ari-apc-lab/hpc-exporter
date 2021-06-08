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

	queueCommand := "qstat -Q -f1"

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
			case "queue_Type":
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
			}
		}
	}

	log.Infof("Collected queues: %d", len(mapQueues))
}

func (pc *PBSCollector) clearQueueMetrics(queue string) {
	/*We give default values to all the metrics, they will be overwritten with the collected values if they exist, otherwise they keep these values.
	For example, if the job hasn't started running, the memory metrics will stay at 0. This way we also ensure that all the metrics exist as well*/
	pc.qMetrics["QueueTotal"][queue] = 0
	pc.qMetrics["QueueMax"][queue] = -1
	pc.qMetrics["QueueEnabled"][queue] = 0
	pc.qMetrics["QueueStarted"][queue] = 0
	pc.qMetrics["QueueQueued"][queue] = 0
	pc.qMetrics["QueueRunning"][queue] = 0
	pc.qMetrics["QueueHeld"][queue] = 0
	pc.qMetrics["QueueWaiting"][queue] = 0
	pc.qMetrics["QueueTransit"][queue] = 0
	pc.qMetrics["QueueExiting"][queue] = 0
	pc.qMetrics["QueueComplete"][queue] = 0
}

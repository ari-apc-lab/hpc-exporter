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
		for key, value := range mapMetrics {
			switch key {
			case "queue_Type":
				pc.labels["QueueType"][queue] = value
			case "total_jobs":
				pc.qMetrics["QueueTotal"][queue], _ = strconv.ParseFloat(value, 64)
			case "state_count":
				chunks := strings.Split(value, " ")
				for _, chunk := range chunks {
					pair := strings.Split(chunk, ":")
					pc.qMetrics["Queue"+pair[0]][queue], _ = strconv.ParseFloat(pair[1], 64)
				}
			case "mtime":
				pc.qMetrics["QueueMTime"][queue], _ = strconv.ParseFloat(value, 64)
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

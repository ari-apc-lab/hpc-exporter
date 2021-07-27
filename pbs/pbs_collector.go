package pbs

import (
	"bytes"
	"errors"
	"hpc_exporter/conf"
	"hpc_exporter/ssh"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	log "github.com/sirupsen/logrus"
)

const (
	pCOMPLETED = iota
	pEXITING
	pRUNNING
	pQUEUED
	pWAITING
	pHELD
	pMOVING
	pSUSPENDED
)

/*
	from man qstat:
	-  the job state:
		C -  Job is completed after having run.
		E -  Job is exiting after having run.
		H -  Job is held.
		Q -  job is queued, eligible to run or routed.
		R -  job is running.
		T -  job is being moved to new location.
		W -  job is waiting for its execution time (-a option) to be reached.
		S -  (Unicos only) job is suspend.
*/

// StatusDict maps string status with its int values
var StatusDict = map[string]int{
	"C": pCOMPLETED,
	"E": pEXITING,
	"H": pHELD,
	"Q": pQUEUED,
	"R": pRUNNING,
	"T": pMOVING,
	"W": pWAITING,
	"S": pSUSPENDED,
}

var jobtags = []string{
	"job_id",
	"job_name",
	"job_user",
	"job_queue",
}

var queuetags = []string{
	"queue_name",
	"queue_type",
}

type PromMetricDesc struct {
	name               string
	desc               string
	variableLabelsTags []string
	constLabels        prometheus.Labels
	isJob              bool
}
type PBSCollector struct {
	descPtrMap     map[string](*prometheus.Desc)
	trackedJobs    map[string]bool
	sshConfig      *ssh.SSHConfig
	sshClient      *ssh.SSHClient
	scrapeInterval int
	lastScrape     time.Time
	jMetrics       map[string](map[string](float64))
	qMetrics       map[string](map[string](float64))
	jLabels        map[string](map[string](string))
	qLabels        map[string](map[string](string))
	mutex          *sync.Mutex
	targetJobIds   string
	JobIds         []string
	skipInfra      bool
	Email          string
}

func NewerPBSCollector(config *conf.CollectorConfig, email string) *PBSCollector {

	constLabels := make(prometheus.Labels)
	constLabels["deployment_label"] = config.Deployment_label
	constLabels["hpc"] = config.Hpc_label
	constLabels["monitoring_id"] = config.Monitoring_id

	var metrics = map[string]PromMetricDesc{
		"JobState":        {"pbs_job_state", "job current state", jobtags, constLabels, true},
		"JobPriority":     {"pbs_job_priority", "job current priority", jobtags, constLabels, true},
		"JobWalltimeUsed": {"pbs_job_walltime_used", "job walltime used, time the job has been running (sec)", jobtags, constLabels, true},
		"JobWalltimeMax":  {"pbs_job_walltime_max", "job maximum walltime allowed (sec)", jobtags, constLabels, true},
		"JobWalltimeRem":  {"pbs_job_walltime_remaining", "job walltime remaining (sec)", jobtags, constLabels, true},
		"JobCPUTime":      {"pbs_job_cpu_time", "job cpu time expended (sec)", jobtags, constLabels, true},
		"JobNCPUs":        {"pbs_job_cpu_n", "job number of threads requested by the job", jobtags, constLabels, true},
		"JobVMEM":         {"pbs_job_mem_virtual", "job virtual memory used", jobtags, constLabels, true},
		"JobQueued":       {"pbs_job_time_queued", "job time spent between creation and running start (or now)", jobtags, constLabels, true},
		"JobRSS":          {"pbs_job_mem_physical", "job physical memory used", jobtags, constLabels, true},
		"JobExitStatus":   {"pbs_job_exit_status", "job exit status. -1 if not completed", jobtags, constLabels, true},
		"QueueTotal":      {"pbs_queue_jobs_total", "queue total number of jobs assigned", queuetags, constLabels, false},
		"QueueMax":        {"pbs_queue_jobs_max", "queue max number of jobs", queuetags, constLabels, false},
		"QueueEnabled":    {"pbs_queue_enabled", "queue if enabled 1, disabled 0", queuetags, constLabels, false},
		"QueueStarted":    {"pbs_queue_started", "queue if started 1, stopped 0", queuetags, constLabels, false},
		"QueueQueued":     {"pbs_queue_jobs_queued", "number of jobs in a queued state in this queue", queuetags, constLabels, false},
		"QueueRunning":    {"pbs_queue_jobs_running", "number of jobs in a running state in this queue", queuetags, constLabels, false},
		"QueueHeld":       {"pbs_queue_jobs_held", "number of jobs in a held state in this queue", queuetags, constLabels, false},
		"QueueWaiting":    {"pbs_queue_jobs_waiting", "number of jobs in a waiting state in this queue", queuetags, constLabels, false},
		"QueueTransit":    {"pbs_queue_jobs_transit", "number of jobs in a transit state in this queue", queuetags, constLabels, false},
		"QueueExiting":    {"pbs_queue_jobs_exiting", "number of jobs in a exiting state in this queue", queuetags, constLabels, false},
		"QueueComplete":   {"pbs_queue_jobs_complete", "number of jobs in a complete state in this queue", queuetags, constLabels, false},
	}

	newerPBSCollector := &PBSCollector{
		descPtrMap:     make(map[string](*prometheus.Desc)),
		sshClient:      nil,
		trackedJobs:    make(map[string]bool),
		scrapeInterval: config.Scrape_interval,
		lastScrape:     time.Now().Add(time.Second * (time.Duration((-2 * config.Scrape_interval)))),
		jMetrics:       make(map[string](map[string](float64))),
		qMetrics:       make(map[string](map[string](float64))),
		jLabels:        make(map[string](map[string](string))),
		qLabels:        make(map[string](map[string](string))),
		mutex:          &sync.Mutex{},
		JobIds:         []string{},
		skipInfra:      config.Only_jobs,
		Email:          email,
	}

	newerPBSCollector.updateDynamicJobIds()

	switch authmethod := config.Auth_method; authmethod {
	case "keypair":
		newerPBSCollector.sshConfig = ssh.NewSSHConfigByPublicKeys(config.User, config.Host, 22, []byte(config.Private_key))
	case "password":
		newerPBSCollector.sshConfig = ssh.NewSSHConfigByPassword(config.User, config.Password, config.Host, 22)
	}

	for key, metric := range metrics {
		newerPBSCollector.descPtrMap[key] = prometheus.NewDesc(metric.name, metric.desc, metric.variableLabelsTags, metric.constLabels)
		if metric.isJob {
			newerPBSCollector.jMetrics[key] = make(map[string]float64)
		} else {
			newerPBSCollector.qMetrics[key] = make(map[string]float64)
		}
	}

	for _, label := range jobtags {
		newerPBSCollector.jLabels[label] = make(map[string]string)
	}

	for _, label := range queuetags {
		newerPBSCollector.qLabels[label] = make(map[string]string)
	}

	return newerPBSCollector
}

// Describe sends metrics descriptions of this collector through the ch channel.
// It implements collector interface
func (pc *PBSCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, element := range pc.descPtrMap {
		ch <- element
	}
}

// Collect read the values of the metrics and
// passes them to the ch channel.
// It implements collector interface
func (pc *PBSCollector) Collect(ch chan<- prometheus.Metric) {
	pc.mutex.Lock()
	defer pc.mutex.Unlock()
	log.Debugf("Time since last scrape: %f seconds", time.Since(pc.lastScrape).Seconds())

	if time.Since(pc.lastScrape).Seconds() > float64(pc.scrapeInterval) {
		pc.updateDynamicJobIds()
		var err error
		pc.sshClient, err = pc.sshConfig.NewClient()
		if err != nil {
			log.Errorf("Creating SSH client: %s", err.Error())
			return
		}
		defer pc.sshClient.Close()
		log.Infof("Collecting metrics from PBS...")
		pc.trackedJobs = make(map[string]bool)
		if pc.targetJobIds != "" {
			pc.collectJobs(ch)
		}
		if !pc.skipInfra {
			pc.collectQueues(ch)
		}
		pc.lastScrape = time.Now()
	}
	pc.updateMetrics(ch)
}

func parsePBSTime(field string) (float64, error) {
	var days, hours, minutes, seconds uint64
	var err error

	toParse := field
	haveDays := false

	// get days
	slice := strings.Split(toParse, "-")
	if len(slice) == 1 {
		toParse = slice[0]
	} else if len(slice) == 2 {
		days, err = strconv.ParseUint(slice[0], 10, 64)
		if err != nil {
			return 0, err
		}
		toParse = slice[1]
		haveDays = true
	} else {
		err = errors.New("PBS time could not be parsed: " + field)
		return 0, err
	}

	// get hours, minutes and seconds
	slice = strings.Split(toParse, ":")
	if len(slice) == 3 {
		hours, err = strconv.ParseUint(slice[0], 10, 64)
		if err == nil {
			minutes, err = strconv.ParseUint(slice[1], 10, 64)
			if err == nil {
				seconds, err = strconv.ParseUint(slice[1], 10, 64)
			}
		}
		if err != nil {
			return 0, err
		}
	} else if len(slice) == 2 {
		if haveDays {
			hours, err = strconv.ParseUint(slice[0], 10, 64)
			if err == nil {
				minutes, err = strconv.ParseUint(slice[1], 10, 64)
			}
		} else {
			minutes, err = strconv.ParseUint(slice[0], 10, 64)
			if err == nil {
				seconds, err = strconv.ParseUint(slice[1], 10, 64)
			}
		}
		if err != nil {
			return 0, err
		}
	} else if len(slice) == 1 {
		if haveDays {
			hours, err = strconv.ParseUint(slice[0], 10, 64)
		} else {
			minutes, err = strconv.ParseUint(slice[0], 10, 64)
		}
		if err != nil {
			return 0, err
		}
	} else {
		err = errors.New("PBS time could not be parsed: " + field)
		return 0, err
	}

	return float64(days*24*60*60 + hours*60*60 + minutes*60 + seconds), nil
}

// nextLineIterator returns a function that iterates
// over an io.Reader object returning each line  parsed
// in fields following the parser method passed as argument

func parseBlocks(buf io.Reader) (map[string](map[string](string)), error) {
	buffer := buf.(*bytes.Buffer)
	result := make(map[string](map[string](string)))
	var err error
	var line string
	var split_line []string
	var label string
	for {
		line, err = buffer.ReadString('\n')
		if err == io.EOF {
			return result, nil
		} else if err != nil {
			return result, err
		}
		split_line = strings.Split(line, ":")
		label = strings.TrimSpace(split_line[1])
		result[label] = make(map[string](string))
		for {
			line, err = buffer.ReadString('\n')
			if err == io.EOF {
				return result, nil
			} else if err != nil {
				return result, err
			}
			split_line = strings.Split(line, "=")
			if len(split_line) < 2 {
				break
			}
			result[label][strings.TrimSpace(split_line[0])] = strings.TrimSpace(split_line[1])
		}
	}
}

func (pc *PBSCollector) updateMetrics(ch chan<- prometheus.Metric) {

	log.Infof("Refreshing exposed metrics")

	for metric, elem := range pc.jMetrics {

		for jobid, value := range elem {
			labels := make([]string, len(pc.jLabels))
			for i, key := range jobtags {
				labels[i] = pc.jLabels[key][jobid]
			}
			ch <- prometheus.MustNewConstMetric(
				pc.descPtrMap[metric],
				prometheus.GaugeValue,
				value,
				labels...,
			)
		}
	}
	if !pc.skipInfra {
		for metric, elem := range pc.qMetrics {

			for queue, value := range elem {
				labels := make([]string, len(pc.qLabels))
				for i, key := range queuetags {
					labels[i] = pc.qLabels[key][queue]
				}
				ch <- prometheus.MustNewConstMetric(
					pc.descPtrMap[metric],
					prometheus.GaugeValue,
					value,
					labels...,
				)
			}
		}
	}

}

func parseMem(s string) (float64, error) {
	l := len(s)
	if l == 0 {
		return -1, nil
	} else if f, e := strconv.ParseFloat(s, 64); e == nil {
		return f, nil
	} else if l <= 2 {
		return 0, errors.New("could not parse memory")
	}
	num, err := strconv.ParseFloat(s[:l-2], 64)
	if err != nil {

		return 0, errors.New("could not parse memory: " + err.Error())
	}
	switch c := s[l-2:]; c {
	case "kb":
		num = num * 1e3
	case "mb":
		num = num * 1e6
	case "gb":
		num = num * 1e9
	default:
		return 0, errors.New("could not parse memory")
	}
	return num, nil
}

func parsePBSDateTime(s string) (time.Time, error) {
	format := "Mon Jan 2 15:04:05 2006"
	return time.Parse(format, s)
}

func (pc *PBSCollector) updateDynamicJobIds() {

	targetJobIds := strings.Join(pc.JobIds, ",")
	pc.targetJobIds = strings.Trim(targetJobIds, ",")
}

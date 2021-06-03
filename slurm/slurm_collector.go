package slurm

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"hpc_exporter/ssh"

	"github.com/prometheus/client_golang/prometheus"

	log "github.com/sirupsen/logrus"
)

const (
	sCOMPLETED = iota
	sCOMPLETING
	sSTAGE_OUT
	sRUNNING
	sCONFIGURING
	sPENDING
	sREQUEUED
	sREQUEUE_FED
	sRESIZING
	sRESV_DEL_HOLD
	sREQUEUE_HOLD
	sSIGNALING
	sSUSPENDED
	sSTOPPED
	sPREEMPTED
	sREVOKED
	sSPECIAL_EXIT
	sDEADLINE
	sTIMEOUT
	sOUT_OF_MEMORY
	sBOOT_FAIL
	sNODE_FAIL
	sFAILED
	sCANCELLED
)

const (
	sUP = iota
	sDOWN
	sDRAIN
	sINACT
)

var PartitionStateDict = map[string]int{
	"up":    sUP,
	"down":  sDOWN,
	"drain": sDRAIN,
	"inact": sINACT,
}

// StatusDict maps string status with its int values
var ShortStatusDict = map[string]int{
	"BF":  sBOOT_FAIL,
	"CA":  sCANCELLED,
	"CD":  sCOMPLETED,
	"CF":  sCONFIGURING,
	"CG":  sCOMPLETING,
	"DL":  sDEADLINE,
	"F":   sFAILED,
	"NF":  sNODE_FAIL,
	"OOM": sOUT_OF_MEMORY,
	"PD":  sPENDING,
	"PR":  sPREEMPTED,
	"R":   sRUNNING,
	"RD":  sRESV_DEL_HOLD,
	"RF":  sREQUEUE_FED,
	"RH":  sREQUEUE_HOLD,
	"RQ":  sREQUEUED,
	"RS":  sRESIZING,
	"RV":  sREVOKED,
	"SI":  sSIGNALING,
	"SE":  sSPECIAL_EXIT,
	"SO":  sSTAGE_OUT,
	"ST":  sSTOPPED,
	"S":   sSUSPENDED,
	"TO":  sTIMEOUT,
}

var LongStatusDict = map[string]int{
	"BOOT_FAIL":     sBOOT_FAIL,
	"CANCELLED":     sCANCELLED,
	"COMPLETED":     sCOMPLETED,
	"CONFIGURING":   sCONFIGURING,
	"COMPLETING":    sCOMPLETING,
	"DEADLINE":      sDEADLINE,
	"FAILED":        sFAILED,
	"NODE_FAIL":     sNODE_FAIL,
	"OUT_OF_MEMORY": sOUT_OF_MEMORY,
	"PENDING":       sPENDING,
	"PREEMPTED":     sPREEMPTED,
	"RUNNING":       sRUNNING,
	"RESV_DEL_HOLD": sRESV_DEL_HOLD,
	"REQUEUE_FED":   sREQUEUE_FED,
	"REQUEUE_HOLD":  sREQUEUE_HOLD,
	"REQUEUED":      sREQUEUED,
	"RESIZING":      sRESIZING,
	"REVOKED":       sREVOKED,
	"SIGNALING":     sSIGNALING,
	"SPECIAL_EXIT":  sSPECIAL_EXIT,
	"STAGE_OUT":     sSTAGE_OUT,
	"STOPPED":       sSTOPPED,
	"SUSPENDED":     sSUSPENDED,
	"TIMEOUT":       sTIMEOUT,
}

var jobtags = []string{
	"job_id",
	"job_name",
	"job_user",
	"job_partition",
}

var partitiontags = []string{
	"partition",
}

type PromMetricDesc struct {
	name               string
	desc               string
	variableLabelsTags []string
	constLabels        prometheus.Labels
	isJob              bool
}

var metrics = map[string]PromMetricDesc{
	"JobState":      {"slurm_job_state", "job current state", jobtags, nil, true},
	"JobWalltime":   {"slurm_job_walltime_used", "job current walltime", jobtags, nil, true},
	"JobNCPUs":      {"slurm_job_cpu_n", "job ncpus assigned", jobtags, nil, true},
	"JobVMEM":       {"slurm_job_memory_virtual_max", "job maximum virtual memory consumed", jobtags, nil, true},
	"JobQueued":     {"slurm_job_queued", "job time in the queue", jobtags, nil, true},
	"JobRSS":        {"slurm_job_memory_physical_max", "job maximum Resident Set Size", jobtags, nil, true},
	"JobExitCode":   {"slurm_job_exit_code", "job exit code", jobtags, nil, true},
	"JobExitSignal": {"slurm_job_exit_signal", "job exit signal that caused the exit code", jobtags, nil, true},
	"PartAvai":      {"slurm_partition_availability", "partition availability", partitiontags, nil, false},
	"PartIdle":      {"slurm_partition_cores_idle", "partition number of idle cores", partitiontags, nil, false},
	"PartAllo":      {"slurm_partition_cores_allocated", "partition number of allocated cores", partitiontags, nil, false},
	"PartTota":      {"slurm_partition_cores_total", "partition number of total cores", partitiontags, nil, false},
}

type CollectFunc func(ch chan<- prometheus.Metric)

type trackedList []string

type SlurmCollector struct {
	descPtrMap     map[string](*prometheus.Desc)
	sacctHistory   int
	sshConfig      *ssh.SSHConfig
	sshClient      *ssh.SSHClient
	runningJobs    trackedList
	trackedJobs    map[string]bool
	scrapeInterval int
	lastScrape     time.Time
	jMetrics       map[string](map[string](float64))
	pMetrics       map[string](map[string](float64))
	jLabels        map[string](map[string](string))
	pLabels        map[string](map[string](string))
	mutex          *sync.Mutex
	targetJobIds   string
	staticJobIds   []string
	dynamicJobIds  []string
	targetJobsFile string
}

func NewerSlurmCollector(host, sshUser, sshAuthMethod, sshPass string, sshPrivKey []byte, sshKnownHosts, timeZone string, sacct_History, scrapeInterval int, targetJobIds, targetJobsFile string) *SlurmCollector {
	newerSlurmCollector := &SlurmCollector{
		descPtrMap:     make(map[string](*prometheus.Desc)),
		sacctHistory:   sacct_History,
		sshClient:      nil,
		runningJobs:    make(trackedList, 0),
		trackedJobs:    make(map[string]bool),
		scrapeInterval: scrapeInterval,
		lastScrape:     time.Now().Add(time.Second * (time.Duration((-2 * scrapeInterval)))),
		jMetrics:       make(map[string](map[string](float64))),
		pMetrics:       make(map[string](map[string](float64))),
		jLabels:        make(map[string](map[string](string))),
		pLabels:        make(map[string](map[string](string))),
		mutex:          &sync.Mutex{},
		staticJobIds:   strings.Split(targetJobIds, ","), //Statically tracked JobIds (through the arguments)
		dynamicJobIds:  make([]string, 0),                //Dynamically tracked JobIds (through a file)
		targetJobsFile: targetJobsFile,                   //File where the dynamically tracked JobIds are stored. If equal to "" there is no dynamic tracking
	}

	newerSlurmCollector.updateDynamicJobIds()

	switch authmethod := sshAuthMethod; authmethod {
	case "keypair":
		newerSlurmCollector.sshConfig = ssh.NewSSHConfigByPublicKeys(sshUser, host, 22, sshPrivKey, sshKnownHosts)
	case "password":
		newerSlurmCollector.sshConfig = ssh.NewSSHConfigByPassword(sshUser, sshPass, host, 22)
	default:
		log.Fatalf("The authentication method provided (%s) is not supported.", authmethod)
	}

	for key, metric := range metrics {
		newerSlurmCollector.descPtrMap[key] = prometheus.NewDesc(metric.name, metric.desc, metric.variableLabelsTags, metric.constLabels)
		if metric.isJob {
			newerSlurmCollector.jMetrics[key] = make(map[string]float64)
		} else {
			newerSlurmCollector.pMetrics[key] = make(map[string]float64)
		}
	}

	for _, label := range jobtags {
		newerSlurmCollector.jLabels[label] = make(map[string]string)
	}

	for _, label := range partitiontags {
		newerSlurmCollector.pLabels[label] = make(map[string]string)
	}

	log.Infof("Target jobs, if specified: %s", newerSlurmCollector.targetJobIds)

	return newerSlurmCollector
}

// Describe sends metrics descriptions of this collector through the ch channel.
// It implements collector interface
func (sc *SlurmCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, element := range sc.descPtrMap {
		ch <- element
	}
}

// Collect read the values of the metrics and
// passes them to the ch channel.
// It implements collector interface
func (sc *SlurmCollector) Collect(ch chan<- prometheus.Metric) {
	sc.mutex.Lock()
	defer sc.mutex.Unlock()

	log.Debugf("Time since last scrape: %f seconds", time.Since(sc.lastScrape).Seconds())
	if time.Since(sc.lastScrape).Seconds() > float64(sc.scrapeInterval) {
		sc.updateDynamicJobIds()
		var err error
		sc.sshClient, err = sc.sshConfig.NewClient()
		if err != nil {
			log.Errorf("Creating SSH client: %s", err.Error())
			return
		}
		defer sc.sshClient.Close()
		log.Infof("Collecting metrics from Slurm...")
		sc.trackedJobs = make(map[string]bool)
		if sc.targetJobIds == "" {
			sc.collectQueue()
		}
		sc.collectAcct()
		sc.collectInfo()
		sc.lastScrape = time.Now()
		sc.delJobs()

	}

	sc.updateMetrics(ch)
}

func getstarttime(days int) string {

	start := time.Now().AddDate(0, 0, -days)
	hour := start.Hour()
	minute := start.Minute()
	second := start.Second()
	day := start.Day()
	month := start.Month()
	year := start.Year()

	str := fmt.Sprintf("%4d-%02d-%02dT%02d:%02d:%02d", year, month, day, hour, minute, second)

	return str
}

// nextLineIterator returns a function that iterates
// over an io.Reader object returning each line  parsed
// in fields following the parser method passed as argument
func nextLineIterator(buf io.Reader, parser func(string) []string) func() ([]string, error) {
	var buffer = buf.(*bytes.Buffer)
	var parse = parser
	return func() ([]string, error) {
		// get next line in buffer
		line, err := buffer.ReadString('\n')
		if err != nil {
			return nil, err
		}
		// fmt.Print(line)

		// parse the line and return
		parsed := parse(line)
		if parsed == nil {
			return nil, errors.New("not able to parse line")
		}
		return parsed, nil
	}
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
	default:
		return 0.0
	}

	walltime := walltime_dd*86400.0 + walltime_hh*3600.0 + walltime_mm*60.0 + walltime_ss
	return walltime
}

func notContains(slice trackedList, s string) bool {
	for _, e := range slice {
		if s == e {
			return false
		}
	}
	return true
}

func parseMem(s string) float64 {
	if len(s) == 0 {
		return -1
	}
	if f, e := strconv.ParseFloat(s, 64); e == nil {
		return f
	}
	num, _ := strconv.ParseFloat(s[:len(s)-1], 64)
	switch c := s[len(s)]; c {
	case 'K', 'k':
		return num * 1e3
	case 'M', 'm':
		return num * 1e6
	case 'G', 'g':
		return num * 1e9
	}
	return num

}

func (sc *SlurmCollector) updateMetrics(ch chan<- prometheus.Metric) {

	log.Infof("Refreshing exposed metrics")

	for metric, elem := range sc.jMetrics {

		for jobid, value := range elem {
			labels := make([]string, len(sc.jLabels))
			for i, key := range jobtags {
				labels[i] = sc.jLabels[key][jobid]
			}
			ch <- prometheus.MustNewConstMetric(
				sc.descPtrMap[metric],
				prometheus.GaugeValue,
				value,
				labels...,
			)
		}
	}

	for metric, elem := range sc.pMetrics {

		for partition, value := range elem {
			labels := make([]string, len(sc.pLabels))
			for i, key := range partitiontags {
				labels[i] = sc.pLabels[key][partition]
			}
			ch <- prometheus.MustNewConstMetric(
				sc.descPtrMap[metric],
				prometheus.GaugeValue,
				value,
				labels...,
			)
		}
	}

}

func (sc *SlurmCollector) delJobs() {
	log.Debugf("Cleaning old jobs")
	i := 0
	for job, tracked := range sc.trackedJobs {
		if !tracked {
			for _, elems := range sc.jMetrics {
				delete(elems, job)
				i++
			}
		}
	}
	log.Debugf("%d old jobs deleted", i)
}

func (sc *SlurmCollector) updateDynamicJobIds() {
	if sc.targetJobsFile != "" {
		if fileBytes, err := os.ReadFile(sc.targetJobsFile); err == nil {
			fileStr := string(fileBytes)
			fileStr = strings.Trim(fileStr, "\n")
			sc.dynamicJobIds = strings.Split(fileStr, "\n")
		} else {
			log.Warnf("Could not open the dynamic target Job IDs file: %s", sc.targetJobsFile)
		}
	}
	//Join the static and dynamic jobIds in a single string separated by commas
	targetJobIds := strings.Join([]string{strings.Join(sc.staticJobIds, ","), strings.Join(sc.dynamicJobIds, ",")}, ",") //We join the static and dynamic jobIds in a single string separated by commas

	sc.targetJobIds = strings.Trim(targetJobIds, ",")
}

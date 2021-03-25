package slurm

import (
	"bytes"
	"errors"
	"io"

	//	"strconv"
	"hpc_exporter/ssh"
	"time"

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
	pUP = iota
	pDOWN
	pDRAIN
	pINACT
)

var PartitionStateDict = map[string]int{
	"up":    pUP,
	"down":  pDOWN,
	"drain": pDRAIN,
	"inact": pINACT,
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

type CollectFunc func(ch chan<- prometheus.Metric)

type jobDetailsMap map[string](string)

type trackedList []string

type SlurmCollector struct {
	gaugeJobsStatusMap  map[string](prometheus.Gauge)
	gaugeJobsElapsedMap map[string](prometheus.Gauge)
	gaugeJobsNCPUSMap   map[string](prometheus.Gauge)
	gaugeJobsVMEMOMap   map[string](prometheus.Gauge)
	gaugeJobsSUBMITMap  map[string](prometheus.Gauge)
	gaugePartsAvailMap  map[string](prometheus.Gauge)
	gaugePartsIdleMap   map[string](prometheus.Gauge)
	gaugePartsAllocMap  map[string](prometheus.Gauge)
	gaugePartsTotalMap  map[string](prometheus.Gauge)

	sshConfig         *ssh.SSHConfig
	sshClient         *ssh.SSHClient
	timeZone          *time.Location
	trackedJobs       trackedList
	trackedPartitions trackedList
	lasttime          time.Time

	jobsMap map[string](jobDetailsMap)
}

func NewerSlurmCollector(host, sshUser, sshAuthMethod, sshPass, sshPrivKey, sshKnownHosts, timeZone string, targetJobIds string) *SlurmCollector {
	newerSlurmCollector := &SlurmCollector{

		gaugeJobsStatusMap:  make(map[string](prometheus.Gauge)),
		gaugeJobsElapsedMap: make(map[string](prometheus.Gauge)),
		gaugeJobsNCPUSMap:   make(map[string](prometheus.Gauge)),
		gaugeJobsVMEMOMap:   make(map[string](prometheus.Gauge)),
		gaugeJobsSUBMITMap:  make(map[string](prometheus.Gauge)),
		gaugePartsAvailMap:  make(map[string](prometheus.Gauge)),
		gaugePartsIdleMap:   make(map[string](prometheus.Gauge)),
		gaugePartsAllocMap:  make(map[string](prometheus.Gauge)),
		gaugePartsTotalMap:  make(map[string](prometheus.Gauge)),
		sshClient:           nil,
		trackedJobs:         make(trackedList, 0),
		trackedPartitions:   make(trackedList, 0),
	}

	switch authmethod := sshAuthMethod; authmethod {
	case "keypair":
		newerSlurmCollector.sshConfig = ssh.NewSSHConfigByPublicKeys(sshUser, host, 22, sshPrivKey, sshKnownHosts)
	case "password":
		newerSlurmCollector.sshConfig = ssh.NewSSHConfigByPassword(sshUser, sshPass, host, 22)
	default:
		log.Fatalf("The authentication method provided (%s) is not supported.", authmethod)
	}

	var err error
	newerSlurmCollector.timeZone, err = time.LoadLocation(timeZone)
	if err != nil {
		newerSlurmCollector.timeZone, _ = time.LoadLocation("Local")
		log.Warningln("Did not recognize time zone, set 'Local' timezone instead")
	}
	newerSlurmCollector.lasttime = time.Now()
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
	var err error
	sc.sshClient, err = sc.sshConfig.NewClient()
	if err != nil {
		log.Errorf("Creating SSH client: %s", err.Error())
		return
	}

	sc.collectAcct(ch)
	sc.collectInfo(ch)

	err = sc.sshClient.Close()
	if err != nil {
		log.Errorf("Closing SSH client: %s", err.Error())
	}
}

func (sc *SlurmCollector) executeSSHCommand(cmd string) (*ssh.SSHSession, error) {
	command := &ssh.SSHCommand{
		Path: cmd,
		// Env:    []string{"LC_DIR=/usr"},
	}

	var outb, errb bytes.Buffer
	session, err := sc.sshClient.OpenSession(nil, &outb, &errb)
	if err == nil {
		err = session.RunCommand(command)
		return session, err
	} else {
		log.Errorf("Opening SSH session: %s", err.Error())
		return nil, err
	}
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

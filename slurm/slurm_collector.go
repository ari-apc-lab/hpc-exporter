package slurm

import (
	"bytes"
	"errors"
	"io"
//	"strconv"
	"strings"
	"time"
	"hpc_exporter/ssh"
	
	"github.com/prometheus/client_golang/prometheus"

	log "github.com/sirupsen/logrus"
)

const (
	sBOOT_FAIL 		= iota
	sCANCELLED		= iota
	sCOMPLETED		= iota
	sCONFIGURING	= iota
	sCOMPLETING		= iota
	sDEADLINE		= iota
	sFAILED			= iota
	sNODE_FAIL		= iota
	sOUT_OF_MEMORY	= iota
	sPENDING			= iota
	sPREEMPTED		= iota
	sRUNNING			= iota
	sRESV_DEL_HOLD	= iota
	sREQUEUE_FED	= iota
	sREQUEUE_HOLD	= iota
	sREQUEUED		= iota
	sRESIZING		= iota
	sREVOKED			= iota
	sSIGNALING		= iota
	sSPECIAL_EXIT	= iota
	sSTAGE_OUT		= iota
	sSTOPPED			= iota
	sSUSPENDED		= iota
	sTIMEOUT			= iota
)

// StatusDict maps string status with its int values
var ShortStatusDict = map[string]int{
	"BF"	:	sBOOT_FAIL,
	"CA"	:	sCANCELLED,
	"CD"	:	sCOMPLETED,
	"CF"	:	sCONFIGURING,
	"CG"	:	sCOMPLETING,
	"DL"	:	sDEADLINE,
	"F"	:	sFAILED,
	"NF"	:	sNODE_FAIL,
	"OOM"	:	sOUT_OF_MEMORY,
	"PD"	:	sPENDING,
	"PR"	:	sPREEMPTED,
	"R"	:	sRUNNING,
	"RD"	:	sRESV_DEL_HOLD,
	"RF"	:	sREQUEUE_FED,
	"RH"	:	sREQUEUE_HOLD,
	"RQ"	:	sREQUEUED,
	"RS"	:	sRESIZING,
	"RV"	:	sREVOKED,
	"SI"	:	sSIGNALING,
	"SE"	:	sSPECIAL_EXIT,
	"SO"	:	sSTAGE_OUT,
	"ST"	:	sSTOPPED,
	"S"	:	sSUSPENDED,
	"TO"	:	sTIMEOUT,
}

var LongStatusDict = map[string]int{
	"BOOT_FAIL"		: sBOOT_FAIL,
	"CANCELLED"		: sCANCELLED,
	"COMPLETED"		: sCOMPLETED,
	"CONFIGURING"	: sCONFIGURING,
	"COMPLETING"	: sCOMPLETING,
	"DEADLINE"		: sDEADLINE,
	"FAILED"			: sFAILED,
	"NODE_FAIL"		: sNODE_FAIL,
	"OUT_OF_MEMORY": sOUT_OF_MEMORY,
	"PENDING"		: sPENDING,
	"PREEMPTED"		: sPREEMPTED,
	"RUNNING"		: sRUNNING,
	"RESV_DEL_HOLD": sRESV_DEL_HOLD,
	"REQUEUE_FED"	: sREQUEUE_FED,
	"REQUEUE_HOLD"	: sREQUEUE_HOLD,
	"REQUEUED"		: sREQUEUED,
	"RESIZING"		: sRESIZING,
	"REVOKED"		: sREVOKED,
	"SIGNALING"		: sSIGNALING,
	"SPECIAL_EXIT"	: sSPECIAL_EXIT,
	"STAGE_OUT"		: sSTAGE_OUT,
	"STOPPED"		: sSTOPPED,
	"SUSPENDED"		: sSUSPENDED,
	"TIMEOUT"		: sTIMEOUT,
}

type CollectFunc 		func(ch chan<- prometheus.Metric)

type jobDetailsMap	map[string](string)

type SlurmCollector struct {
	
	descPtrMap			map[string](*prometheus.Desc)
		
	sshConfig         *ssh.SSHConfig
	sshClient         *ssh.SSHClient
	timeZone          *time.Location
	targetJobIdsList	[]string
	alreadyRegistered []string
//	lasttime          time.Time
	
	jobsMap				map[string](jobDetailsMap)
}

func NewerSlurmCollector(host, sshUser, sshAuthMethod, sshPass, sshPrivKey, sshKnownHosts, timeZone string, targetJobIds string) *SlurmCollector {
	newerSlurmCollector := &SlurmCollector {	
		descPtrMap:				make(map[string](*prometheus.Desc)),
		sshClient:				nil,
		alreadyRegistered:	make([]string, 0),
	}
	
	switch authmethod := sshAuthMethod; authmethod {
		case "keypair":
			newerSlurmCollector.sshConfig = ssh.NewSSHConfigByPublicKeys(sshUser, host, 22, sshPrivKey, sshKnownHosts)
		case "password":
			newerSlurmCollector.sshConfig = ssh.NewSSHConfigByPassword(sshUser, sshPass, host, 22)
		default:
			log.Fatalf("The authentication method provided (%s) is not supported.", authmethod)
	}
	
	newerSlurmCollector.descPtrMap["userJobState"] = prometheus.NewDesc(
		"slurm_jobstate",
		"user job current state",
		[]string{
			"job_id", "username", "job_name", "job_state", "exit_status_full",
		},
		nil,
	)
	
	newerSlurmCollector.descPtrMap["userJobExitStatus1"] = prometheus.NewDesc(
		"slurm_jobexitstatus1",
		"user job current state",
		[]string{
			"job_id", "username", "job_name", "job_state", "exit_status_full",
		},
		nil,
	)
		
	newerSlurmCollector.descPtrMap["userJobExitStatus2"] = prometheus.NewDesc(
		"slurm_jobexitstatus2",
		"user job current state",
		[]string{
			"job_id", "username", "job_name", "job_state", "exit_status_full",
		},
		nil,
	)
	
	newerSlurmCollector.descPtrMap["userJobWallTime"] = prometheus.NewDesc(
		"slurm_jobwalltime",
		"user job current state",
		[]string{
			"job_id", "username", "job_name", "job_state", "wall_time",
		},
		nil,
	)

	if (targetJobIds != "") {
		targetJobIds = strings.TrimFunc(targetJobIds, func(r rune) bool { return r == ',' })
		newerSlurmCollector.targetJobIdsList = strings.Split(targetJobIds,",")
		log.Infof("Target jobs: %s %d",newerSlurmCollector.targetJobIdsList,len(newerSlurmCollector.targetJobIdsList))
	} else {
		log.Fatalf("Target jobs list is mandatory for Slurm collector")
		return nil
	}

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
	
	sc.collectJobInfo(ch)

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

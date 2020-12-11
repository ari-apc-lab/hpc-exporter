package pbs

import (
	"io"
	"strings"
	"time"
	"unicode"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

const (
	aJOBID     = iota
	aUSERNAME  = iota
	aQUEUE     = iota
	aJOBNAME   = iota
	aSESSID    = iota
	aNDS       = iota
	aTSK       = iota
	aREQDMEM   = iota
	aREQDTIME  = iota
	aS         = iota
	aELAPTIME  = iota
	aFIELDS    = iota
)

func (sc *PBSCollector) collectJobQstat(jobid string) jobDetailsMap {

	jobDetails := make(jobDetailsMap)

	log.Debugf("Collecting job %s detailed qstat metrics...",jobid)
	
	currentCommand := "qstat -f " + jobid + " -1"
	log.Debugln(currentCommand)
	
	sshSession, err := sc.executeSSHCommand(currentCommand)
	if sshSession != nil {
		defer sshSession.Close()
	}
	if err != nil {
		log.Errorf("qstat: %s ", err.Error())
		return nil
	}
	
	// wait for stdout to fill (it is being filled async by ssh)
	time.Sleep(500 * time.Millisecond)
	//sc.setLastTime()

	var buffer = sshSession.OutBuffer
	
	for{
		line, _ := buffer.ReadString('\n')	// new line
		if line == ""  {
			break
		}

		line = strings.TrimLeftFunc(line, func(r rune) bool { return unicode.IsSpace(r) })				
		split_line := strings.Split(line," = ")
		if len(split_line) == 2 {
			field_name := strings.ToLower(split_line[0])
			field_value := split_line[1]			
			field_value = strings.TrimRightFunc(field_value, func(r rune) bool { return unicode.IsSpace(r) })			
			jobDetails[field_name] = field_value
		}
	}
	
	// sc.jobsMap[jobid] = jobDetails
	return jobDetails
}

func (sc *PBSCollector) collectQstat(ch chan<- prometheus.Metric) {

	log.Debugln("Collecting qstat metrics...")
	var collected uint

	currentCommand := "qstat -a"
	log.Debugln(currentCommand)

	sshSession, err := sc.executeSSHCommand(currentCommand)
	if sshSession != nil {
		defer sshSession.Close()
	}
	if err != nil {
		log.Errorf("qstat: %s ", err.Error())
		return
	}

	// wait for stdout to fill (it is being filled async by ssh)
	time.Sleep(100 * time.Millisecond)
	//sc.setLastTime()

	var buffer = sshSession.OutBuffer
	
	line, error := buffer.ReadString('\n')	// new line
	line, error = buffer.ReadString('\n')	// hazelhen-batch.hww.hlrs.de:
	line, error = buffer.ReadString('\n')	// new line
	line, error = buffer.ReadString('\n')	// header line: "Job ID..."
	line, error = buffer.ReadString('\n')	// dashes: "------..."
	if error == nil {
		log.Debugf("qstat: Last header line read: %s", line)
	} else {
		if error == io.EOF {
			log.Info("qstat: No user jobs in the infrastructure")
			return
		} else {
			log.Fatalf("qstat: Something went wrong when parsing the output: %s", error)
		}
	}	

	nextLine := nextLineIterator(sshSession.OutBuffer, qstatLineParser)
	for fields, err := nextLine(); err == nil; fields, err = nextLine() {
		// check the line is correctly parsed
		if err != nil {
			log.Warnln(err.Error())
			continue
		}	
	
		if (len(sc.targetJobIdsList) > 0) && !(sc.jobIsTarget(fields[aJOBID])) {
			continue
		}

		// get job details		
		jobdetails := sc.collectJobQstat(fields[aJOBID])
		// status, statusOk := StatusDict[fields[aSTATE]]
		status, statusOk := StatusDict[fields[aS]]
		if statusOk {
			// if jobIsNotInQueue(status) {
			ch <- prometheus.MustNewConstMetric(
				sc.descPtrMap["userJobs"],
				prometheus.GaugeValue,
				float64(status),
				fields[aJOBID], fields[aUSERNAME], fields[aJOBNAME], fields[aS],				
				/*jobdetails["ctime"], jobdetails["qtime"], jobdetails["mtime"], jobdetails["etime"],*/
				jobdetails["start_time"], jobdetails["comp_time"], jobdetails["total_runtime"],
				jobdetails["resources_used.cput"],
				jobdetails["resources_used.mem"],
				jobdetails["resources_used.vmem"],
				jobdetails["resources_used.walltime"],
			)
			sc.alreadyRegistered = append(sc.alreadyRegistered, fields[aJOBID])
			//log.Debugln("Job " + fields[aJOBID] + " finished with state " + fields[aSTATE])
			collected++
			// }			
			
		} else {
			// log.Warnf("Couldn't parse job status: '%s', fields '%s'", fields[aSTATE], strings.Join(fields, "|"))
			log.Warnf("Couldn't parse job status: '%s', fields '%s'", fields[aS], strings.Join(fields, "|"))
		}
	}

	log.Infof("Collected jobs: %d", collected)
}

func (sc *PBSCollector) jobIsTarget(fullJobId string) bool {
	// Check whether a job found in the qstat response is a monitoring target
	isTarget := false	
	for _, s := range sc.targetJobIdsList {
		isTarget = isTarget || (s == fullJobId)
	}
	return isTarget
}

func jobIsNotInQueue(state int) bool {
	// return state != sPENDING && state != sRUNNING && state != sCOMPLETING
	return state != sEXITING && state != sQUEUED && state != sRUNNING
}

func qstatLineParser(line string) []string {
	fields := strings.Fields(line)

	if len(fields) < aFIELDS {
		log.Warnf("qstat line parse failed (%s): %d fields expected, %d parsed", line, aFIELDS, len(fields))
		return nil
	}

	return fields
}

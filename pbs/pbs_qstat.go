package pbs

import (
	"io"
	"strings"
	"time"
	"unicode"
	"strconv"

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
		state, stateOk := StatusDict[fields[aS]]
		if stateOk {
		
			// Job state		
			ch <- prometheus.MustNewConstMetric(
				sc.descPtrMap["userJobState"],
				prometheus.GaugeValue,
				float64(state),
				fields[aJOBID], fields[aUSERNAME], fields[aJOBNAME], fields[aS], jobdetails["exit_status"],
			)
			
			// Job exit status			
			exit_status, exit_status_err := strconv.ParseInt(jobdetails["exit_status"], 10, 0)
			if exit_status_err != nil {
				exit_status = -1
			}						
			ch <- prometheus.MustNewConstMetric(
				sc.descPtrMap["userJobExitStatus"],
				prometheus.GaugeValue,
				float64(exit_status),
				fields[aJOBID], fields[aUSERNAME], fields[aJOBNAME], fields[aS], jobdetails["exit_status"],
			)
			
			// Job total runtime
			total_runtime, total_runtime_err := strconv.ParseFloat(jobdetails["total_runtime"], 64)
			if total_runtime_err != nil {
				total_runtime = -1
			}						
			ch <- prometheus.MustNewConstMetric(
				sc.descPtrMap["userJobTotalRuntime"],
				prometheus.GaugeValue,
				float64(total_runtime),
				fields[aJOBID], fields[aUSERNAME], fields[aJOBNAME], fields[aS], jobdetails["exit_status"],
				jobdetails["start_time"],jobdetails["comp_time"],
			)
				
			// Job resources consumed wall time
			walltime := -1.0
			if jobdetails["resources_used.walltime"] != "" {
				split_walltime := strings.Split(jobdetails["resources_used.walltime"],":")
				walltime_hh, _ := strconv.ParseFloat(split_walltime[0], 64)
				walltime_mm, _ := strconv.ParseFloat(split_walltime[1], 64)
				walltime_ss, _ := strconv.ParseFloat(split_walltime[2], 64)
				walltime = walltime_hh * 3600.0 + walltime_mm * 60.0 + walltime_ss
			}			
			ch <- prometheus.MustNewConstMetric(
				sc.descPtrMap["userJobResourcesWallTime"],
				prometheus.GaugeValue,
				float64(walltime),
				fields[aJOBID], fields[aUSERNAME], fields[aJOBNAME], fields[aS], jobdetails["exit_status"],
				jobdetails["start_time"],jobdetails["comp_time"],		
			)
			
			// Job resources consumed cpu time
			cputime := -1.0
			if jobdetails["resources_used.cput"] != "" {
				split_cputime := strings.Split(jobdetails["resources_used.cput"],":")
				cputime_hh, _ := strconv.ParseFloat(split_cputime[0], 64)
				cputime_mm, _ := strconv.ParseFloat(split_cputime[1], 64)
				cputime_ss, _ := strconv.ParseFloat(split_cputime[2], 64)
				cputime = cputime_hh * 3600.0 + cputime_mm * 60.0 + cputime_ss
			}			
			ch <- prometheus.MustNewConstMetric(
				sc.descPtrMap["userJobResourcesCpuTime"],
				prometheus.GaugeValue,
				float64(cputime),
				fields[aJOBID], fields[aUSERNAME], fields[aJOBNAME], fields[aS], jobdetails["exit_status"],
				jobdetails["start_time"],jobdetails["comp_time"],		
			)
			
			// Job resources consumed pyhsical memory		
			physmem := -1.0
			physmem_units := ""
			if jobdetails["resources_used.mem"] != "" {
				split_physmem := strings.Split(jobdetails["resources_used.mem"],"k")
				physmem, _ = strconv.ParseFloat(split_physmem[0], 64)
				physmem_units = "kb"
			}
			ch <- prometheus.MustNewConstMetric(
				sc.descPtrMap["userJobResourcesPhysMem"],
				prometheus.GaugeValue,
				float64(physmem),
				fields[aJOBID], fields[aUSERNAME], fields[aJOBNAME], fields[aS], jobdetails["exit_status"],
				physmem_units,		
			)	
			
			// Job resources consumed virtual memory
			virtmem := -1.0
			virtmem_units := ""
			if jobdetails["resources_used.vmem"] != "" {
				split_virtmem := strings.Split(jobdetails["resources_used.vmem"],"k")
				virtmem, _ = strconv.ParseFloat(split_virtmem[0], 64)
				virtmem_units = "kb"
			}
			ch <- prometheus.MustNewConstMetric(
				sc.descPtrMap["userJobResourcesVirtMem"],
				prometheus.GaugeValue,
				float64(virtmem),
				fields[aJOBID], fields[aUSERNAME], fields[aJOBNAME], fields[aS], jobdetails["exit_status"],
				virtmem_units,		
			)		
			
			sc.alreadyRegistered = append(sc.alreadyRegistered, fields[aJOBID])
			collected++
		
		} else {
			log.Warnf("Couldn't parse job state: '%s', fields '%s'", fields[aS], strings.Join(fields, "|"))
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

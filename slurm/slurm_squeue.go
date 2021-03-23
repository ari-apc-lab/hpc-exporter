package slurm

import (
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

const (
	aJOBID = iota
	aPARTITION
	aNAME
	aUSER
	aSTATE
	aTIME
	aTIME_LIMI
	aNODES
	aNODELIST_REASON
	aFIELDS
)

func (sc *SlurmCollector) collectSqueue(targetjobid string, ch chan<- prometheus.Metric) {

	formatString := "\"%.18i %.18P %.18j %.18u %.18T %.18M %.18l %.18D %.18R\""
	currentCommand := "squeue -j " + targetjobid + " -o " + formatString + " -P --noheader"
	log.Debugln(currentCommand)

	sshSession, err := sc.executeSSHCommand(currentCommand)
	if sshSession != nil {
		defer sshSession.Close()
	}
	if err != nil {
		log.Errorf("squeue: %s ", err.Error())
		return
	}

	time.Sleep(100 * time.Millisecond)

	nextLine := nextLineIterator(sshSession.OutBuffer, squeueLineParser)
	for fields, err := nextLine(); err == nil; fields, err = nextLine() {
		// check the line is correctly parsed
		if err != nil {
			log.Warnln(err.Error())
			continue
		}

		state, stateOk := LongStatusDict[fields[aSTATE]]
		if stateOk {

			// Job state
			ch <- prometheus.MustNewConstMetric(
				sc.descPtrMap["userJobState"],
				prometheus.GaugeValue,
				float64(state),
				fields[aJOBID], fields[aUSER], fields[aNAME], fields[aSTATE], "-1:-1",
			)

			exit_status_1 := -1.0
			exit_status_2 := -1.0

			// Job exit status LHS
			ch <- prometheus.MustNewConstMetric(
				sc.descPtrMap["userJobExitStatus1"],
				prometheus.GaugeValue,
				float64(exit_status_1),
				fields[aJOBID], fields[aUSER], fields[aNAME], fields[aSTATE], "-1:-1",
			)

			// Job exit status RHS
			ch <- prometheus.MustNewConstMetric(
				sc.descPtrMap["userJobExitStatus2"],
				prometheus.GaugeValue,
				float64(exit_status_2),
				fields[aJOBID], fields[aUSER], fields[aNAME], fields[aSTATE], "-1:-1",
			)

			// Job wall time
			walltime := computeSlurmTime(fields[aTIME])
			ch <- prometheus.MustNewConstMetric(
				sc.descPtrMap["userJobWallTime"],
				prometheus.GaugeValue,
				float64(walltime),
				fields[aJOBID], fields[aUSER], fields[aNAME], fields[aSTATE], "-1:-1", fields[aTIME],
			)

		} else {
			log.Warnf("Couldn't parse job state: '%s', fields '%s'", fields[aSTATE], strings.Join(fields, "|"))
		}
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
	}

	walltime := walltime_dd*86400.0 + walltime_hh*3600.0 + walltime_mm*60.0 + walltime_ss
	return walltime

}

func squeueLineParser(line string) []string {
	fields := strings.Fields(line)

	if len(fields) < aFIELDS {
		log.Warnf("squeue line parse failed (%s): %d fields expected, %d parsed", line, aFIELDS, len(fields))
		return nil
	}

	return fields
}

func sacctJobStateLineParser(line string) []string {
	fields := strings.Fields(line)
	if len(fields) < 1 {
		log.Warnf("sacct job state parse failed (%s): %d fields expected, %d parsed", line, 1, len(fields))
		return nil
	}
	return fields
}

func (sc *SlurmCollector) collectSacct(targetjobid string, ch chan<- prometheus.Metric) {

	sshUser := sc.sshConfig.Config.User
	formatString := "JobId,JobName,Elapsed,State,ExitCode"

	currentCommand := "sacct -j " + targetjobid + " -o " + formatString + " -p --noheader"
	log.Debugln(currentCommand)

	sshSession, err := sc.executeSSHCommand(currentCommand)
	if sshSession != nil {
		defer sshSession.Close()
	}
	if err != nil {
		log.Errorf("sacct: %s ", err.Error())
		return
	}

	time.Sleep(1000 * time.Millisecond)

	nextLine := nextLineIterator(sshSession.OutBuffer, sacctFullLineParser)
	for fields, err := nextLine(); err == nil; fields, err = nextLine() {
		// check the line is correctly parsed
		if err != nil {
			log.Warnln(err.Error())
			continue
		}

		state, stateOk := LongStatusDict[fields[accSTATE]]
		if stateOk {

			// Job state
			ch <- prometheus.MustNewConstMetric(
				sc.descPtrMap["userJobState"],
				prometheus.GaugeValue,
				float64(state),
				fields[accJOBID], sshUser, fields[accNAME], fields[accSTATE], fields[accEXITCODE],
			)

			split_exitcode := strings.Split(fields[accEXITCODE], ":")
			exit_status_1, _ := strconv.ParseFloat(split_exitcode[0], 64)
			exit_status_2, _ := strconv.ParseFloat(split_exitcode[1], 64)

			// Job exit status LHS
			ch <- prometheus.MustNewConstMetric(
				sc.descPtrMap["userJobExitStatus1"],
				prometheus.GaugeValue,
				float64(exit_status_1),
				fields[accJOBID], sshUser, fields[accNAME], fields[accSTATE], fields[accEXITCODE],
			)

			// Job exit status RHS
			ch <- prometheus.MustNewConstMetric(
				sc.descPtrMap["userJobExitStatus2"],
				prometheus.GaugeValue,
				float64(exit_status_2),
				fields[accJOBID], sshUser, fields[accNAME], fields[accSTATE], fields[accEXITCODE],
			)

			// Job wall time
			walltime := computeSlurmTime(fields[accELAPSED])
			ch <- prometheus.MustNewConstMetric(
				sc.descPtrMap["userJobWallTime"],
				prometheus.GaugeValue,
				float64(walltime),
				fields[accJOBID], sshUser, fields[accNAME], fields[accSTATE], fields[accEXITCODE], fields[accELAPSED],
			)

		} else {
			log.Warnf("Couldn't parse job info: fields '%s'", strings.Join(fields, "|"))
		}
	}
}

func sacctFullLineParser(line string) []string {
	fields := strings.Split(line, "|")
	if len(fields) < 5 {
		log.Warnf("sacct full line parse failed (%s): %d fields expected, %d parsed", line, 5, len(fields))
		return nil
	}
	return fields
}

func (sc *SlurmCollector) collectJobInfo(ch chan<- prometheus.Metric) {

	for _, targetJobId := range sc.targetJobIdsList {

		currentCommand := "sacct -j " + targetJobId + " -o State --noheader"

		sshSession, err := sc.executeSSHCommand(currentCommand)
		if sshSession != nil {
			defer sshSession.Close()
		}
		if err != nil {
			log.Errorf("sacct: %s ", err.Error())
			return
		}

		nextLine := nextLineIterator(sshSession.OutBuffer, sacctJobStateLineParser)
		for fields, err := nextLine(); err == nil; fields, err = nextLine() {
			if err != nil {
				log.Warnln(err.Error())
				continue
			}

			longstate := fields[0]
			log.Debugf("%s: %s", targetJobId, longstate)
			if longstate == "PENDING" {
				sc.collectSqueue(targetJobId, ch)
			} else {
				sc.collectSacct(targetJobId, ch)
			}
		}
	}

}

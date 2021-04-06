// Copyright (c) 2017 MSO4SC - javier.carnero@atos.net
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// sinfo Slurm auxiliary collector

package slurm

import (
	"errors"
	"hpc_exporter/ssh"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	iPARTITION = iota
	iAVAIL
	iSTATES
	iFIELDS
)

const (
	iSTATESNUMBER = 4
)

func (sc *SlurmCollector) collectInfo() {
	log.Debugln("Collecting Info metrics...")
	var collected uint
	session, err := sc.openSession()
	defer closeSession(session)

	if err != nil {
		log.Errorf("Error opening session for sinfo: %s ", err.Error())
		return
	}
	// execute the command
	infoCommand := &ssh.SSHCommand{
		Path: "sinfo -h -o \"%20R %.5a %.20F\" | uniq",
	}
	log.Debugln(infoCommand)
	err = session.RunCommand(infoCommand)

	if err != nil {
		err_chunk, err2 := session.ErrBuffer.ReadString('\n')
		err_str := err_chunk
		for ; err2 == nil; err_str += err_chunk {
			err_chunk, err2 = session.ErrBuffer.ReadString('\n')
		}
		log.Errorf("sinfo command failed: %s", err.Error())
		log.Debugf("Error was: %s", err_str)
		return
	}

	// wait for stdout to fill (it is being filled async by ssh)
	time.Sleep(1000 * time.Millisecond)

	nextLine := nextLineIterator(session.OutBuffer, sinfoLineParser)
	for fields, err := nextLine(); err == nil; fields, err = nextLine() {
		// check the line is correctly parsed
		if err != nil {
			log.Warnf(err.Error())
			continue
		}
		partition := fields[iPARTITION]
		availability, errb := PartitionStateDict[fields[iAVAIL]]

		if !errb {
			log.Warnf("Error parsing '%s' partition availability: %s", partition, fields[iAVAIL])
			continue
		}

		idle, allocated, total, err := parseNodes(fields[iSTATES])

		if err != nil {
			log.Warnf(err.Error())
			return
		}

		sc.jobMetrics["PartAvai"][partition] = float64(availability)
		sc.jobMetrics["PartIdle"][partition] = float64(idle)
		sc.jobMetrics["PartAllo"][partition] = float64(allocated)
		sc.jobMetrics["PartTota"][partition] = float64(total)

		collected++

	}
	log.Infof("%d partition info collected", collected)
	return
}

func sinfoLineParser(line string) []string {
	fields := strings.Fields(line)

	if len(fields) != iFIELDS {
		log.Warnf("sinfo line parse failed (%s): %d fields expected, %d parsed", line, iFIELDS, len(fields))
		return nil
	}

	return fields
}

func parseNodes(ns string) (float64, float64, float64, error) {

	nodesByStatus := strings.Split(ns, "/")
	if len(nodesByStatus) != 4 {
		return 0, 0, 0, errors.New("Could not parse nodes: " + ns)
	}
	alloc, _ := strconv.ParseFloat(nodesByStatus[0], 64)
	idle, _ := strconv.ParseFloat(nodesByStatus[1], 64)
	total, _ := strconv.ParseFloat(nodesByStatus[3], 64)
	return idle, alloc, total, nil
}

package api

import (
	"encoding/json"
	"fmt"
	"hpc_exporter/conf"
	"io/ioutil"
	"net/http"

	log "github.com/sirupsen/logrus"
)

func (s *HpcExporterStore) AddJobHandler(w http.ResponseWriter, r *http.Request) {

	bodyBytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	defer r.Body.Close()

	ct := r.Header.Get("content-type")
	if ct != "application/json" {
		w.WriteHeader(http.StatusUnsupportedMediaType)
		w.Write([]byte(fmt.Sprintf("need content-type 'application/json', but got '%s'", ct)))
		return
	}

	// Define default values
	config := conf.DefaultConfig()

	err = json.Unmarshal(bodyBytes, &config)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	} else if config.Deployment_id == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Request does not contain the deployment_id"))
		return
	}

	if config.Job_id == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Request does not contain the Job_id"))
		return
	}

	if config.Host == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Request does not contain the HPC Host"))
		return
	}

	s.Lock()
	defer s.Unlock()
	log.Infof("Adding job %s in host %s for monitoring_id %s", config.Job_id, config.Host, config.Deployment_id)
	key := config.Deployment_id + "@" + config.Host

	if collector, ok := s.storePBS[key]; ok {
		collector.JobIds = append(collector.JobIds, config.Job_id)
		collector.TrackedJobs[config.Job_id] = 5 //Set to number of monitor scrapes to wait to collect metrics after job termination
	} else if collector, ok := s.storeSlurm[key]; ok {
		collector.JobIds = append(collector.JobIds, config.Job_id)
		collector.TrackedJobs[config.Job_id] = 5
	} else {
		msg := fmt.Sprintf("Request to add job %s failed. No collector found for host %s and monitoring_id %s ",
			config.Job_id, config.Host, config.Deployment_id)
		log.Info(msg)
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(msg))
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Job ID added"))

}

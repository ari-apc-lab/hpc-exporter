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
	} else if config.Deployment_id == "no_label" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Need the deployment_id"))
		return
	}

	if config.Job_id == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Need the Job_id"))
		return
	}

	if config.Host == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Need the HPC Host"))
		return
	}

	s.Lock()
	defer s.Unlock()
	log.Infof("Adding job %s in host %s for monitoring_id %s", config.Job_id, config.Host, config.Deployment_id)
	key := config.Deployment_id + config.Host

	if collector, ok := s.storePBS[key]; ok {
		collector.JobIds = append(collector.JobIds, config.Job_id)
	} else if collector, ok := s.storeSlurm[key]; ok {
		collector.JobIds = append(collector.JobIds, config.Job_id)
	} else {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("Combination of Host and Monitoring_id not found"))
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Job ID added"))

}

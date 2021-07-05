package api

import (
	"encoding/json"
	"fmt"
	"hpc_exporter/conf"
	"io/ioutil"
	"net/http"
)

func (s *HpcExporterStore) AddJobHandler(w http.ResponseWriter, r *http.Request) {

	bodyBytes, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

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
	} else if config.Monitoring_id == "no_label" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Need the Monitoring_id"))
		return
	}

	if config.Job_id == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Need the Job_id"))
		return
	}

	s.Lock()
	defer s.Unlock()

	if collector, ok := s.storePBS[config.Monitoring_id]; ok {
		collector.JobIds = append(collector.JobIds, config.Job_id)
	} else if collector, ok := s.storeSlurm[config.Monitoring_id]; ok {
		collector.JobIds = append(collector.JobIds, config.Job_id)
	} else {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("Monitoring_id not found"))
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Job ID added"))

}

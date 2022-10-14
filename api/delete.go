package api

import (
	"encoding/json"
	"fmt"
	"hpc_exporter/conf"
	"io/ioutil"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

func (s *HpcExporterStore) DeleteHandler(w http.ResponseWriter, r *http.Request) {

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
	}

	if config.Deployment_id == "no_label" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Need the deployment_id"))
		return
	}

	if config.Host == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Need the Host"))
		return
	}

	key := config.Deployment_id + config.Host

	if collector, ok := s.storePBS[key]; ok {
		if (!config.Force && len(collector.JobIds) > 0){
			w.WriteHeader(http.StatusConflict)
			w.Write([]byte("collector is still pending of monitoring jobs, wait for then to complete before deleting this collector"))
			return
		}
		prometheus.Unregister(collector)
		delete(s.storePBS, key)
	} else if collector, ok := s.storeSlurm[key]; ok {
		if (!config.Force && len(collector.JobIds) > 0){
			w.WriteHeader(http.StatusConflict)
			w.Write([]byte("collector is still pending of monitoring jobs, wait for then to complete before deleting this collector"))
			return
		}
		prometheus.Unregister(collector)
		delete(s.storeSlurm, key)
	} else {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("monitoring_id not found"))
		return
	}

	log.Infof("Deleted collector for deployment_id %s in host %s", config.Deployment_id, config.Host)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Collector deleted"))

}

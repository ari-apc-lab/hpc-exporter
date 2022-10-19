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

	if config.Deployment_id == "" && config.User == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Request does contain neither the deployment_id nor the HPC ssh user"))
		return
	}

	if config.Host == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Request does not contain the Host"))
		return
	}

	// Setting collector key based on host (for infrastructure collectors) or deployment_id (for workflow collectors)
	var key string
	if config.Deployment_id != "" {
		key = config.Deployment_id + "@" + config.Host
	} else {
		key = config.User + "@" + config.Host
	}

	if collector, ok := s.storePBS[key]; ok {
		if !config.Force && len(collector.JobIds) > 0 {
			msg := fmt.Sprintf("collector %s is still pending of monitoring jobs, wait for then to complete before deleting this collector", key)
			log.Info(msg)
			w.WriteHeader(http.StatusConflict)
			w.Write([]byte(msg))
			return
		}
		prometheus.Unregister(collector)
		delete(s.storePBS, key)
	} else if collector, ok := s.storeSlurm[key]; ok {
		if !config.Force && len(collector.JobIds) > 0 {
			msg := fmt.Sprintf("collector %s is still pending of monitoring jobs, wait for then to complete before deleting this collector", key)
			log.Info(msg)
			w.WriteHeader(http.StatusConflict)
			w.Write([]byte(msg))
			return
		}
		prometheus.Unregister(collector)
		delete(s.storeSlurm, key)
	} else {
		msg := fmt.Sprintf("collector %s not found", key)
		log.Error(msg)
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(msg))
		return
	}

	msg := fmt.Sprintf("collector %s deleted", key)
	log.Infof(msg)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(msg))

}

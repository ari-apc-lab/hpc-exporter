package api

import (
	"encoding/json"
	"fmt"
	"hpc_exporter/conf"
	"io/ioutil"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
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
	} else if config.Monitoring_id == "no_label" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Need the monitoring_id"))
		return
	}

	if collector, ok := s.storePBS[config.Monitoring_id]; ok {
		prometheus.Unregister(collector)
	} else if collector, ok := s.storeSlurm[config.Monitoring_id]; ok {
		prometheus.Unregister(collector)
	} else {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("monitoring_id not found"))
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Collector deleted"))

}

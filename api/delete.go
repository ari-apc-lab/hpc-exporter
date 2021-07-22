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

	userData := NewUserData()
	err := userData.GetUser(r, *s.security)
	if err != nil {
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(err.Error()))
		return
	}

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

	if config.Monitoring_id == "no_label" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Need the monitoring_id"))
		return
	}

	if config.Host == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Need the Host"))
		return
	}

	key := config.Monitoring_id + config.Host

	if collector, ok := s.storePBS[key]; ok {
		if collector.Email == userData.email {
			prometheus.Unregister(collector)
		} else {
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte("User not authorized to delete the provided monitoring_id"))
		}
	} else if collector, ok := s.storeSlurm[key]; ok {
		if collector.Email == userData.email {
			prometheus.Unregister(collector)
		} else {
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte("User not authorized to delete the provided monitoring_id"))
		}
	} else {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("monitoring_id not found"))
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Collector deleted"))

}

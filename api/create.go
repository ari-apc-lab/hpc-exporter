package api

import (
	"encoding/json"
	"fmt"
	"hpc_exporter/conf"
	"hpc_exporter/pbs"
	"hpc_exporter/slurm"
	"io/ioutil"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
)

func (s *HpcExporterStore) CreateHandler(w http.ResponseWriter, r *http.Request) {

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

	if config.Host == "localhost" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Localhost connection not available."))
	} else if config.Host == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("HPC Host missing."))
	} else {
		if config.User == "" {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("SSH user missing."))
		}
		switch authmethod := config.Auth_method; authmethod {
		case "keypair":
			if config.Private_key == "" {
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte("SSH private key missing."))
			}
		case "password":
			if config.Password == "" {
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte("SSH password missing."))
			}
		default:
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(fmt.Sprintf("The authentication method provided (%s) is not supported.", authmethod)))
		}
	}

	s.Lock()
	defer s.Unlock()

	switch sched := config.Scheduler; sched {
	case "pbs":
		s.storePBS[config.Monitoring_id] = pbs.NewerPBSCollector(config)
		prometheus.MustRegister(s.storePBS[config.Monitoring_id])
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Collector created"))
	case "slurm":
		s.storeSlurm[config.Monitoring_id] = slurm.NewerSlurmCollector(config)
		prometheus.MustRegister(s.storeSlurm[config.Monitoring_id])
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Collector created"))
	default:
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(fmt.Sprintf("The scheduler type provided (%s) is not supported.", sched)))
	}
}

package api

import (
	"encoding/json"
	"fmt"
	"hpc_exporter/conf"
	"hpc_exporter/pbs"
	"hpc_exporter/slurm"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

func (s *HpcExporterStore) CreateHandler(w http.ResponseWriter, r *http.Request) {

	ct := r.Header.Get("content-type")
	if ct != "application/json" {
		w.WriteHeader(http.StatusUnsupportedMediaType)
		w.Write([]byte(fmt.Sprintf("need content-type 'application/json', but got '%s'", ct)))
		return
	}

	config := conf.DefaultConfig()
	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(&config)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}

	if config.Host == "localhost" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Localhost connection not available."))
		return
	} else if config.Host == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("HPC Host missing. (\"host\")"))
		return
	} else if config.User == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("SSH user missing. (\"user\")"))
	} else {
		if config.Password != "" && config.Auth_method != "keypair" {
			config.Auth_method = "password"
		} else if config.Private_key != "" {
			config.Auth_method = "keypair"
		} else {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("No password or private key provided"))
		}
	}

	key := config.Monitoring_id + config.Host

	s.Lock()
	defer s.Unlock()

	// TODO Add support for multiple collectors of the same type and the same monitoring_id (different hpc_label)
	log.Infof("Creating collector for monitoring_id %s in host %s", config.Monitoring_id, config.Host)

	switch sched := config.Scheduler; sched {
	case "pbs":
		if _, exists := s.storePBS[key]; exists {
			log.Errorf("There is already a PBS collector for the provided monitoring_id %s and host %s", config.Monitoring_id, config.Host)
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("There is already a PBS collector for the provided monitoring_id and host"))
			return
		}
		s.storePBS[key] = pbs.NewerPBSCollector(config)
		prometheus.MustRegister(s.storePBS[key])
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Collector created"))
	case "slurm":
		if _, exists := s.storeSlurm[key]; exists {
			log.Errorf("There is already a Slurm collector for the provided monitoring_id %s and host %s", config.Monitoring_id, config.Host)
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("There is already a Slurm collector for the provided monitoring_id and host"))
			return
		}
		s.storeSlurm[key] = slurm.NewerSlurmCollector(config)
		prometheus.MustRegister(s.storeSlurm[key])
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Collector created"))
	default:
		log.Errorf("The scheduler type provided (%s) is not supported.", sched)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(fmt.Sprintf("The scheduler type provided (%s) is not supported.", sched)))
	}
}

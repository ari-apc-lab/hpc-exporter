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

	// Setting collector key based on host (for infrastructure collectors) or deployment_id (for workflow collectors)
	var key string
	if config.Deployment_id != ""{
		key = config.Deployment_id + "@" + config.Host 
	} else {
		key = config.User + "@" + config.Host 
	}

	s.Lock()
	defer s.Unlock()

	log.Infof("Creating collector for %s", key)

	switch sched := config.Scheduler; sched {
	case "pbs":
		if _, exists := s.storePBS[key]; exists {
			msg := fmt.Sprintf("There is already a PBS collector for %s", key)
			log.Errorf(msg)
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(msg))
			return
		}
		msg := fmt.Sprintf("collector %s created", key)
		log.Infof(msg)
		s.storePBS[key] = pbs.NewerPBSCollector(config)
		prometheus.MustRegister(s.storePBS[key])
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(msg))
	case "slurm":
		if _, exists := s.storeSlurm[key]; exists {
			msg := fmt.Sprintf("There is already a Slurm collector for %s", key)
			log.Errorf(msg)
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(msg))
			return
		}
		s.storeSlurm[key] = slurm.NewerSlurmCollector(config)
		prometheus.MustRegister(s.storeSlurm[key])
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Collector created"))
	default:
		msg := fmt.Sprintf("The scheduler type provided (%s) is not supported.", sched)
		log.Errorf(msg)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(msg))
	}
}

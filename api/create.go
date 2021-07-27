package api

import (
	"encoding/json"
	"fmt"
	"hpc_exporter/conf"
	"hpc_exporter/pbs"
	"hpc_exporter/slurm"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
)

func (s *HpcExporterStore) CreateHandler(w http.ResponseWriter, r *http.Request) {
	userData := NewUserData()
	err := userData.GetUser(r, *s.security)
	if err != nil {
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(err.Error()))
		return
	}

	ct := r.Header.Get("content-type")
	if ct != "application/json" {
		w.WriteHeader(http.StatusUnsupportedMediaType)
		w.Write([]byte(fmt.Sprintf("need content-type 'application/json', but got '%s'", ct)))
		return
	}

	config := conf.DefaultConfig()
	decoder := json.NewDecoder(r.Body)
	err = decoder.Decode(&config)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}

	err = userData.GetSSHCredentials(config.Auth_method, config.Hpc_label, r, *s.security)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
	}

	config.User = userData.ssh_user

	if config.Host == "localhost" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Localhost connection not available."))
		return
	} else if config.Host == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("HPC Host missing."))
		return
	} else {
		switch authmethod := config.Auth_method; authmethod {
		case "keypair":
			config.Private_key = userData.ssh_private_key
		case "password":
			config.Password = userData.ssh_password
		default:
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(fmt.Sprintf("The authentication method provided (%s) is not supported.", authmethod)))
			return
		}
	}

	key := config.Monitoring_id + config.Host

	s.Lock()
	defer s.Unlock()

	// TODO Add support for multiple collectors of the same type and the same monitoring_id (different hpc_label)

	switch sched := config.Scheduler; sched {
	case "pbs":
		if _, exists := s.storePBS[key]; exists {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("There is already a PBS collector for the provided monitoring_id and host"))
			return
		}
		s.storePBS[key] = pbs.NewerPBSCollector(config, userData.email)
		prometheus.MustRegister(s.storePBS[key])
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Collector created"))
	case "slurm":
		if _, exists := s.storePBS[key]; exists {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("There is already a Slurm collector for the provided monitoring_id and host"))
			return
		}
		s.storeSlurm[key] = slurm.NewerSlurmCollector(config, userData.email)
		prometheus.MustRegister(s.storeSlurm[key])
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Collector created"))
	default:
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(fmt.Sprintf("The scheduler type provided (%s) is not supported.", sched)))
	}
}

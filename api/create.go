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

	userData := NewUserData()
	err := userData.GetEmail(r, *s.security)
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

	err = userData.GetSSHCredentials(config.Auth_method, r, *s.security)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
	}

	config.User = userData.user

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
			config.Private_key = userData.private_key
		case "password":
			config.Password = userData.password
		default:
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(fmt.Sprintf("The authentication method provided (%s) is not supported.", authmethod)))
			return
		}
	}

	s.Lock()
	defer s.Unlock()

	// TODO Add support for multiple collectors of the same type and the same monitoring_id (different hpc_label)

	switch sched := config.Scheduler; sched {
	case "pbs":
		if _, exists := s.storePBS[config.Monitoring_id]; exists {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("There is already a PBS collector for the provided monitoring_id"))
			return
		}
		s.storePBS[config.Monitoring_id] = pbs.NewerPBSCollector(config, userData.email)
		prometheus.MustRegister(s.storePBS[config.Monitoring_id])
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Collector created"))
	case "slurm":
		if _, exists := s.storePBS[config.Monitoring_id]; exists {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("There is already a Slurm collector for the provided monitoring_id"))
			return
		}
		s.storeSlurm[config.Monitoring_id] = slurm.NewerSlurmCollector(config, userData.email)
		prometheus.MustRegister(s.storeSlurm[config.Monitoring_id])
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Collector created"))
	default:
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(fmt.Sprintf("The scheduler type provided (%s) is not supported.", sched)))
	}
}

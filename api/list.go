package api

import (
	"encoding/json"
	"hpc_exporter/pbs"
	"hpc_exporter/slurm"
	"net/http"
)

func (s *HpcExporterStore) ListHandler(w http.ResponseWriter, r *http.Request) {

	// Return registered collectors
	collectors := NewCollectorList(s.storeSlurm, s.storePBS)

	jsonBytes, err := json.Marshal(collectors)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("List of collectors could not be retrieved"))
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(jsonBytes)
	json.NewEncoder(w).Encode(s.storePBS)
}

type CollectorList struct {
	Slurm_collectors map[string]*slurm.SlurmCollector
	Pbs_collectors   map[string]*pbs.PBSCollector
}

func NewCollectorList(slurm map[string]*slurm.SlurmCollector, pbs map[string]*pbs.PBSCollector) *CollectorList {
	return &CollectorList{
		Slurm_collectors: slurm,
		Pbs_collectors:   pbs,
	}
}

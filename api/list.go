package api

import (
	"encoding/json"
	"hpc_exporter/pbs"
	"hpc_exporter/slurm"
	"net/http"
)

func (s *HpcExporterStore) ListHandler(w http.ResponseWriter, r *http.Request) {

	// Return registered collector for user
	collectors := NewCollectorList(s.storeSlurm, s.storePBS)
	// collectors.filterByUserEmail(userData.email)

	jsonBytes, err := json.Marshal(collectors)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("List of collectors could not be retrieved"))
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(jsonBytes)
	//json.NewEncoder(w).Encode(s.storePBS)
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

// func (s *CollectorList) filterByUserEmail(email string) {
// 	//PBS collectors
// 	pbs_keys_to_removed := []string{}
// 	for key, pbs := range s.Pbs_collectors {
// 		if pbs.Email != email {
// 			pbs_keys_to_removed = append(pbs_keys_to_removed, key)
// 		}
// 	}
// 	for _, key := range pbs_keys_to_removed {
// 		delete(s.Pbs_collectors, key)
// 	}

// 	//Slurm collectors
// 	slurm_keys_to_removed := []string{}
// 	for key, pbs := range s.Slurm_collectors {
// 		if pbs.Email != email {
// 			slurm_keys_to_removed = append(slurm_keys_to_removed, key)
// 		}
// 	}
// 	for _, key := range slurm_keys_to_removed {
// 		delete(s.Pbs_collectors, key)
// 	}
// }

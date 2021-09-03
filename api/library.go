package api

import (
	"hpc_exporter/pbs"
	"hpc_exporter/slurm"
	"sync"
)

type HpcExporterStore struct {
	sync.Mutex
	storeSlurm map[string]*slurm.SlurmCollector
	storePBS   map[string]*pbs.PBSCollector
}

func NewCollectorStore() *HpcExporterStore {
	return &HpcExporterStore{
		storeSlurm: map[string]*slurm.SlurmCollector{},
		storePBS:   map[string]*pbs.PBSCollector{},
	}
}

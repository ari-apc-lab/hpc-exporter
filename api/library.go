package api

import (
	"hpc_exporter/conf"
	"hpc_exporter/pbs"
	"hpc_exporter/slurm"
	"sync"
)

type HpcExporterStore struct {
	sync.Mutex
	storeSlurm map[string]*slurm.SlurmCollector
	storePBS   map[string]*pbs.PBSCollector
	security   *conf.Security
}

func NewCollectorStore(sec *conf.Security) *HpcExporterStore {
	return &HpcExporterStore{
		storeSlurm: map[string]*slurm.SlurmCollector{},
		storePBS:   map[string]*pbs.PBSCollector{},
		security:   sec,
	}
}

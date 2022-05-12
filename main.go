package main

import (
	"flag"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	log "github.com/sirupsen/logrus"

	"hpc_exporter/api"

	"github.com/gorilla/mux"
)

var (
	addr = flag.String(
		"listen-address",
		":9110",
		"The address to listen on for HTTP requests.",
	)

	logLevel = flag.String(
		"log-level",
		"error",
		"Log level of the Application.",
	)
)

func main() {
	flag.Parse()
	// Parse and set log lovel
	level, err := log.ParseLevel(*logLevel)
	if err == nil {
		log.SetLevel(level)
	} else {
		log.SetLevel(log.WarnLevel)
		log.Warnf("Log level %s not recognized, setting 'warn' as default.")
	}

	collectorStore := api.NewCollectorStore()
	// Expose the registered metrics via HTTP.
	log.Infof("Starting Server: %s", *addr)

	router := mux.NewRouter()
	router.Handle("/metrics", promhttp.Handler())
	router.HandleFunc("/collector", collectorStore.ListHandler).Methods("GET")
	router.HandleFunc("/collector", collectorStore.CreateHandler).Methods("POST")
	router.HandleFunc("/collector", collectorStore.DeleteHandler).Methods("DELETE")
	router.HandleFunc("/job", collectorStore.AddJobHandler).Methods("POST")
	router.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
			<head><title>Multi-Tenant HPC Exporter</title></head>
			<body>
			<h1>HPC Exporter</h1>
			<p><a href="/metrics">Metrics</a></p>
			</body>
			</html>`))
	}).Methods("GET")
	log.Debug(http.ListenAndServe(*addr, router))
}

package main

import (
	"flag"
	"net/http"
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	log "github.com/sirupsen/logrus"

	"hpc_exporter/pbs"
	"hpc_exporter/torque"
)

var (
	addr = flag.String(
		"listen-address",
		":9100",
		"The address to listen on for HTTP requests.",
	)
	scheduler = flag.String(
		"scheduler",
		"pbs",
		"Type of scheduler: pbs",
	)
	host = flag.String(
		"host",
		"localhost",
		"HPC infrastructure domain name or IP.",
	)
	sshUser = flag.String(
		"ssh-user",
		"",
		"SSH user for remote frontend connection (no localhost).",
	)
	sshPass = flag.String(
		"ssh-password",
		"",
		"SSH password for remote frontend connection (no localhost).",
	)
	countryTZ = flag.String(
		"countrytz",
		"Europe/Madrid",
		"Country Time zone of the host, (e.g. \"Europe/Madrid\").",
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

	// Flags check
	if *host == "localhost" {
		flag.Usage()
		log.Fatalln("Localhost connection not implemented yet.")
	} else {
		if *sshUser == "" {
			flag.Usage()
			log.Fatalln("A user must be provided to connect to a frontend remotely.")
		}
		if *sshPass == "" {
			flag.Usage()
			log.Warnln("A password should be provided to connect to a frontend remotely.")
		}
	}

//	prometheus.MustRegister(NewerTorqueCollector(*host, *sshUser, *sshPass, *countryTZ))
	switch sched := *scheduler; sched {
        case "torque":
		log.Debugf("Registering collector for scheduler %s", sched)
		prometheus.MustRegister(torque.NewerTorqueCollector(*host, *sshUser, *sshPass, *countryTZ))
        case "pbs":
		log.Debugf("Registering collector for scheduler %s", sched)
		prometheus.MustRegister(pbs.NewerPBSCollector(*host, *sshUser, *sshPass, *countryTZ))
        default:
                os.Exit(-1)
        }

	// Expose the registered metrics via HTTP.
	log.Infof("Starting Server: %s", *addr)
	http.Handle("/metrics", promhttp.Handler())
	log.Fatal(http.ListenAndServe(*addr, nil))
}

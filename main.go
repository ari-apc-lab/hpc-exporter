package main

import (
	"flag"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	log "github.com/sirupsen/logrus"

	"hpc_exporter/pbs"
	"hpc_exporter/slurm"
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
		"Type of scheduler",
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
	sshAuthMethod = flag.String(
		"ssh-auth-method",
		"keypair",
		"SSH authentication method for remote frontend connection (no localhost).",
	)
	sshPass = flag.String(
		"ssh-password",
		"",
		"SSH password for remote frontend connection (no localhost).",
	)
	sshPrivKey = flag.String(
		"ssh-private-key",
		"",
		"Path to the SSH private key recognised by the remote frontend",
	)
	sshKnownHosts = flag.String(
		"ssh-known-hosts",
		"",
		"Path to the SSH local known_hosts file including the remote frontend",
	)
	/*
		countryTZ = flag.String(
			"countrytz",
			"Europe/Madrid",
			"Country Time zone of the host, (e.g. \"Europe/Madrid\").",
		)
	*/
	sacctHistory = flag.Int(
		"sacctHistory",
		5,
		"Jobs reported will be the ones submitted this many days back. Default is 5",
	)
	scrapeInterval = flag.Int(
		"scrape-interval",
		300,
		"How often (in seconds) SSH commands will be executed on the HPC frontend to update metrics",
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
		switch authmethod := *sshAuthMethod; authmethod {
		case "keypair":
			//				if (*sshPrivKey == "" || *sshKnownHosts == "") {
			if *sshPrivKey == "" {
				flag.Usage()
				//					log.Fatalln("Paths to a private key and a known hosts file should be provided to connect to a frontend remotely using this authentication method.")
				log.Fatalln("Path to a private key should be provided to connect to a frontend remotely using this authentication method.")
			}
			if *sshKnownHosts == "" {
				log.Infoln("Known hosts file is not mandatory but recommended.")
			}
		case "password":
			if *sshPass == "" {
				flag.Usage()
				log.Fatalln("A password should be provided to connect to a frontend remotely using this authentication method.")
			}
		default:
			flag.Usage()
			log.Fatalf("The authentication method provided (%s) is not supported.", authmethod)
		}
	}

	switch sched := *scheduler; sched {
	case "pbs":
		log.Debugf("Registering collector for scheduler %s", sched)
		prometheus.MustRegister(pbs.NewerPBSCollector(*host, *sshUser, *sshAuthMethod, *sshPass, *sshPrivKey, *sshKnownHosts, ""))
	case "slurm":
		log.Debugf("Registering collector for scheduler %s", sched)
		prometheus.MustRegister(slurm.NewerSlurmCollector(*host, *sshUser, *sshAuthMethod, *sshPass, *sshPrivKey, *sshKnownHosts, "", *sacctHistory, *scrapeInterval))
	default:
		log.Fatalf("The scheduler type provided (%s) is not supported.", sched)
	}

	// Expose the registered metrics via HTTP.
	log.Infof("Starting Server: %s", *addr)
	http.Handle("/metrics", promhttp.Handler())
	log.Fatal(http.ListenAndServe(*addr, nil))
}

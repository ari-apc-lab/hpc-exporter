package main

import (
	"flag"
	"net/http"
	"os"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	log "github.com/sirupsen/logrus"

	"hpc_exporter/api"
	"hpc_exporter/conf"
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

	introspection_endpoint = flag.String(
		"introspection-endpoint",
		"",
		"Introspection endpoint to check JWT (Keycloak).",
	)

	introspection_client = flag.String(
		"introspection-client",
		"",
		"Introspection client.",
	)

	introspection_secret = flag.String(
		"introspection-secret",
		"",
		"Introspection client secret.",
	)

	vault_login_endpoint = flag.String(
		"vault-login-endpoint",
		"",
		"Vault login endpoint.",
	)

	vault_storage_endpoint = flag.String(
		"vault-storage-endpoint",
		"",
		"Vault secret endpoint.",
	)

	vault_role = flag.String(
		"vault-role",
		"",
		"Vault role.",
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

	security_config := conf.NewSecurityConf()

	if *introspection_endpoint == "" {
		security_config.Introspection_endpoint = os.Getenv("OIDC_INTROSPECTION_ENDPOINT")
		if security_config.Introspection_endpoint == "" {
			log.Fatal("No introspection endpoint given. Provide argument --introspection-endpoint or set environment variable OIDC_INTROSPECTION_ENDPOINT")
		}
	} else {
		security_config.Introspection_endpoint = *introspection_endpoint
	}

	if *introspection_client == "" {
		security_config.Introspection_client = os.Getenv("OIDC_INTROSPECTION_CLIENT")
		if security_config.Introspection_client == "" {
			log.Fatal("No introspection client given. Provide argument --introspection-client or set environment variable OIDC_INTROSPECTION_CLIENT")
		}
	} else {
		security_config.Introspection_client = *introspection_client
	}

	if *introspection_secret == "" {
		security_config.Introspection_secret = os.Getenv("OIDC_INTROSPECTION_SECRET")
		if security_config.Introspection_secret == "" {
			log.Fatal("No introspection secret given. Provide argument --introspection-secret or set environment variable OIDC_INTROSPECTION_SECRET")
		}
	} else {
		security_config.Introspection_secret = *introspection_secret
	}

	if *vault_login_endpoint == "" {
		security_config.Vault_login_endpoint = os.Getenv("VAULT_LOGIN_ENDPOINT")
		if security_config.Vault_login_endpoint == "" {
			log.Fatal("No introspection secret given. Provide argument --vault-login-endpoint or set environment variable VAULT_LOGIN_ENDPOINT")
		}
	} else {
		security_config.Vault_login_endpoint = *vault_login_endpoint
	}

	if *vault_role == "" {
		security_config.Vault_role = os.Getenv("VAULT_ROLE")
		if security_config.Vault_role == "" {
			log.Fatal("No introspection secret given. Provide argument --vault-role or set environment variable VAULT_ROLE")
		}
	} else {
		security_config.Vault_role = *vault_role
	}

	if *vault_storage_endpoint == "" {
		security_config.Vault_storage_endpoint = os.Getenv("VAULT_STORAGE_ENDPOINT")
		if security_config.Vault_storage_endpoint == "" {
			log.Fatal("No introspection secret given. Provide argument --vault-storage-endpoint or set environment variable VAULT_STORAGE_ENDPOINT")
		}
	} else {
		security_config.Vault_storage_endpoint = *introspection_secret
	}

	collectorStore := api.NewCollectorStore(security_config)
	// Expose the registered metrics via HTTP.
	log.Infof("Starting Server: %s", *addr)
	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/create", collectorStore.CreateHandler)
	http.HandleFunc("/delete", collectorStore.DeleteHandler)
	http.HandleFunc("/addjob", collectorStore.AddJobHandler)
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
			<head><title>Multi-Tenant HPC Exporter</title></head>
			<body>
			<h1>Node Exporter</h1>
			<p><a href="/metrics">Metrics</a></p>
			</body>
			</html>`))
	})
	log.Fatal(http.ListenAndServe(*addr, nil))
}

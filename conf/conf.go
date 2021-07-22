package conf

type CollectorConfig struct {
	Host             string
	Scheduler        string
	User             string
	Auth_method      string
	Password         string
	Private_key      string
	Private_key_pw   string
	Sacct_history    int
	Scrape_interval  int
	Deployment_label string
	Monitoring_id    string
	Hpc_label        string
	Only_jobs        bool
	Job_id           string
}

func DefaultConfig() *CollectorConfig {
	return &CollectorConfig{
		Host:             "",
		Scheduler:        "",
		User:             "",
		Auth_method:      "",
		Password:         "",
		Private_key:      "",
		Sacct_history:    5,
		Scrape_interval:  15,
		Deployment_label: "no_label",
		Monitoring_id:    "no_label",
		Hpc_label:        "no_label",
		Only_jobs:        false,
		Job_id:           "",
	}
}

type Security struct {
	Introspection_endpoint string
	Introspection_secret   string
	Introspection_client   string
	Vault_address          string
}

func NewSecurityConf() *Security {
	return &Security{
		Introspection_endpoint: "",
		Introspection_secret:   "",
		Introspection_client:   "",
		Vault_address:          "",
	}
}

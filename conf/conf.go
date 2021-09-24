package conf

type CollectorConfig struct {
	Host             string `json:"host"`
	Scheduler        string `json:"scheduler"`
	User             string `json:"ssh_user"`
	Auth_method      string `json:"auth_method"`
	Password         string `json:"ssh_password"`
	Private_key      string `json:"ssh_pkey"`
	Sacct_history    int    `json:"sacct_history"`
	Scrape_interval  int    `json:"scrape_interval"`
	Deployment_label string `json:"deployment_label"`
	Monitoring_id    string `json:"monitoring_id"`
	Hpc_label        string `json:"hpc_label"`
	Only_jobs        bool   `json:"only_jobs"`
	Job_id           string `json:"job_id"`
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

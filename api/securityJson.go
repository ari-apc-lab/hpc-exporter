package api

type KeycloakResponse struct {
	Email  string `json:"email"`
	Active bool   `json:"active"`
}

func newKeycloakResponse() *KeycloakResponse {
	return &KeycloakResponse{
		Email:  "",
		Active: false,
	}
}

type VaultAuth struct {
	Client_token string `json:"client_token"`
}
type VaultLoginResponse struct {
	Auth *VaultAuth `json:"auth"`
}

func newVaultLoginResponse() *VaultLoginResponse {
	return &VaultLoginResponse{
		Auth: &VaultAuth{
			Client_token: "",
		},
	}
}

// TODO Write correct json tags
type VaultSecret struct {
	Password    string `json:"password"`
	User        string `json:"user"`
	Private_key string `json:"private_key"`
}

type VaultSecretResponse struct {
	Data *VaultSecret `json:"data"`
}

func newVaultSecretResponse() *VaultSecretResponse {
	return &VaultSecretResponse{
		Data: &VaultSecret{
			Password:    "",
			User:        "",
			Private_key: "",
		},
	}
}

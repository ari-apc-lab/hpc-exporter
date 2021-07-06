package api

import (
	"encoding/json"
	"errors"
	"hpc_exporter/conf"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

type UserData struct {
	email       string
	jwt         string
	private_key string
	password    string
	user        string
}

func NewUserData() *UserData {
	return &UserData{
		email:       "",
		jwt:         "",
		private_key: "",
		password:    "",
		user:        "",
	}
}

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

// TODO Fix vault response according to real example
type VaultResponse struct {
	Password    string
	User        string
	Private_key string
}

func newVaultResponse() *VaultResponse {
	return &VaultResponse{
		Password:    "",
		User:        "",
		Private_key: "",
	}
}

func (d *UserData) getJWT(r *http.Request) {
	reqToken := r.Header.Get("Authorization")
	splitToken := strings.Split(reqToken, "Bearer")
	d.jwt = strings.TrimSpace(splitToken[1])
}

func (d *UserData) GetEmail(r *http.Request, security_conf conf.Security) error {
	if d.jwt == "" {
		d.getJWT(r)
	}
	client := &http.Client{}

	data := url.Values{}
	data.Set("token", d.jwt)

	req, err := http.NewRequest("POST", security_conf.Introspection_endpoint, strings.NewReader(data.Encode()))
	if err != nil {
		return errors.New("could not create authentication request")
	}

	req.SetBasicAuth(security_conf.Introspection_client, security_conf.Introspection_secret)
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Add("Content-Length", strconv.Itoa(len(data.Encode())))

	resp, err := client.Do(req)
	if err != nil {
		return errors.New("there was an error trying to reach the Keycloak server")
	}

	ok := resp.StatusCode == 200

	keycloak_response := newKeycloakResponse()
	if err := json.NewDecoder(resp.Body).Decode(keycloak_response); err != nil {

	} else if !ok || !keycloak_response.Active {
		return errors.New("Unauthorized")
	}
	d.email = keycloak_response.Email
	return nil

}

// TODO Fix secret retrieval using real example

func (d *UserData) GetSSHCredentials(method string, r *http.Request, security_conf conf.Security) error {
	if d.jwt == "" {
		d.getJWT(r)
	}

	vault_client_token, err := d.getVaultToken(security_conf)
	if err != nil {
		return err
	}

	client := &http.Client{}
	req, err := http.NewRequest("GET", security_conf.Vault_storage_endpoint, nil)
	if err != nil {
		return err
	}
	req.Header.Set("x-vault-token", vault_client_token)
	resp_vault, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp_vault.Body.Close()
	vault_response := newVaultResponse()
	if err := json.NewDecoder(resp_vault.Body).Decode(vault_response); err != nil {
		return errors.New("error when retrieving the Vault secrets")
	}

	d.user = vault_response.User
	if d.user == "" {
		return errors.New("no user stored in Vault")
	}
	switch method {
	case "password":
		d.password = vault_response.Password
		if d.password == "" {
			return errors.New("no password stored in Vault")
		}
	case "keypair":
		d.private_key = vault_response.Private_key
		if d.private_key == "" {
			return errors.New("no private key stored in Vault")
		}
	}

	return nil
}

// TODO Fix token retrieval using example

func (d *UserData) getVaultToken(security_conf conf.Security) (string, error) {
	client := &http.Client{}
	data := url.Values{}

	data.Set("jwt", d.jwt)
	data.Set("role", security_conf.Vault_role)

	req, err := http.NewRequest("POST", security_conf.Vault_login_endpoint, strings.NewReader(data.Encode()))
	if err != nil {
		return "", errors.New("could not create Vault login request")
	}

	req.Header.Add("Content-Length", strconv.Itoa(len(data.Encode())))

	resp_login, err := client.Do(req)
	if err != nil {
		return "", errors.New("there was an error trying to log into Vault")
	}
	defer resp_login.Body.Close()
	client_token_raw, err := ioutil.ReadAll(resp_login.Body)
	if err != nil {
		return "", errors.New("could not log into Vault successfully")
	}
	return string(client_token_raw), nil
}

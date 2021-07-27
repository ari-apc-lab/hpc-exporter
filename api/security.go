package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"hpc_exporter/conf"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

type UserData struct {
	username        string
	email           string
	jwt             string
	ssh_private_key string
	ssh_password    string
	ssh_user        string
}

func NewUserData() *UserData {
	return &UserData{
		username:        "",
		email:           "",
		jwt:             "",
		ssh_private_key: "",
		ssh_password:    "",
		ssh_user:        "",
	}
}

func (d *UserData) getJWT(r *http.Request) error {
	reqToken := r.Header.Get("Authorization")
	splitToken := strings.Split(reqToken, "Bearer")
	if len(splitToken) != 2 {
		return errors.New("incorrect authorization header format")
	}
	d.jwt = strings.TrimSpace(splitToken[1])
	return nil
}

func (d *UserData) GetUser(r *http.Request, security_conf conf.Security) error {
	if d.jwt == "" {
		if err := d.getJWT(r); err != nil {
			return err
		}
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
	d.username = keycloak_response.Username
	return nil

}

func (d *UserData) GetSSHCredentials(method, hpc string, r *http.Request, security_conf conf.Security) error {
	if d.jwt == "" {
		d.getJWT(r)
	}

	vault_client_token, err := d.getVaultToken(security_conf)
	if err != nil {
		return err
	}

	client := &http.Client{}

	secret_endpoint := "http://" + security_conf.Vault_address + "/v1/hpc/" + d.username + "/" + hpc
	req, err := http.NewRequest("GET", secret_endpoint, nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Vault-Token", vault_client_token)
	resp_vault, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp_vault.Body.Close()
	vault_response := newVaultSecretResponse()
	if err := json.NewDecoder(resp_vault.Body).Decode(vault_response); err != nil {
		return errors.New("error when retrieving the Vault secrets")
	}
	vault_secret := vault_response.Data
	d.ssh_user = vault_secret.User
	if d.ssh_user == "" {
		return errors.New("no user stored in Vault")
	}
	switch method {
	case "password":
		d.ssh_password = vault_secret.Password
		if d.ssh_password == "" {
			return errors.New("no password stored in Vault")
		}
	case "keypair":
		d.ssh_private_key = vault_secret.Private_key
		if d.ssh_private_key == "" {
			return errors.New("no private key stored in Vault")
		}
	}

	return nil
}

func (d *UserData) getVaultToken(security_conf conf.Security) (string, error) {
	client := &http.Client{}

	s := vaultLogin(d.jwt, d.username)
	b := new(bytes.Buffer)
	json.NewEncoder(b).Encode(s)

	vault_login_endpoint := "http://" + security_conf.Vault_address + "/v1/auth/jwt/login"
	req, err := http.NewRequest("POST", vault_login_endpoint, b)
	if err != nil {
		return "", errors.New("could not create Vault login request")
	}

	resp_login, err := client.Do(req)
	if err != nil {
		return "", errors.New("there was an error trying to log into Vault")
	}
	defer resp_login.Body.Close()

	vault_response := newVaultLoginResponse()
	if err := json.NewDecoder(resp_login.Body).Decode(vault_response); err != nil {
		return "", errors.New("error when retrieving the Vault token")
	}

	return vault_response.Auth.Client_token, nil
}

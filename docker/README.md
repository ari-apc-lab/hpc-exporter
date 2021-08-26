# HPC exporter Docker image

A docker image can be found [here](https://hub.docker.com/r/sodaliteh2020/hpc-exporter/tags). This image runs a single instance of hpc-exporter inside a container.

## Usage

Recommended usage is to use environment variables to launch the image. Internal port is 9110 by default, it must  be mapped to whatever port is needed by the user. Environment variables:

- `<OIDC_INTROSPECTION_ENDPOINT>`: Endpoint of the service to verify JWTs. Default is env variable  `OIDC_INTROSPECTION_ENDPOINT`
- `<OIDC_INTROSPECTION_CLIENT>`: Client of the service to verify JWTs. Default is env variable  `OIDC_INTROSPECTION_CLIENT`
- `<OIDC_INTROSPECTION_SECRET>`: Client secret of the service to verify JWTs. Default is env variable  `OIDC_INTROSPECTION_SECRET`
- `<VAULT_ADDRESS>`: Address of the Vault instance that holds the ssh credentials. Default is env variable `VAULT_ADDRESS`



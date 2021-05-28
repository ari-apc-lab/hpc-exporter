# HPC exporter Docker image

A docker image can be found [here](https://hub.docker.com/r/sodaliteh2020/hpc-exporter/tags). This image runs a single instance of hpc-exporter inside a container.

## Usage

You can can check the following files for an example of usage with the xopera orchestrator.
 
* `Dockerfile` with image definition
* xOpera input file:
  * `input.yaml`: Example of how the parameters must be passed to run the exporter in a container    
* xOpera service files:
  * `service.yaml.local`: Image must be built and pushed to a local registry 
  * `service.yaml.privateregistry`: Image is pulled from the private Sodalite registry

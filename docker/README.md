# HPC exporter Docker image

Docker image to run a single instance of hpc-exporter inside a container.

## Contents

* `Dockerfile` with image definition
* xOpera input file:
  * `input.yaml`: Example of how the parameters must be passed to run the exporter in a container    
* xOpera service files:
  * `service.yaml.local`: Image must be built and pushed to a local registry 
  * `service.yaml.privateregistry`: Image is pulled from the private Sodalite registry

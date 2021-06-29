FROM golang:1.16-alpine

# LABEL maintainer="jesus.ramos.external@atos.net"

RUN mkdir /app
WORKDIR /app

# Install git
RUN apk add --update --no-cache git

# Get HPC exporter
RUN git clone -b ignore-host-key https://github.com/SODALITE-EU/hpc-exporter.git

# Build the code
RUN cd hpc-exporter && go build

EXPOSE ${EXPORTER_PORT}

CMD [ "sh", "-c", "./hpc-exporter/hpc_exporter -host=${HPC_FRONTEND} -listen-address=:${EXPORTER_PORT} -scheduler=${SCHEDULER} -ssh-user=${SSH_USER} -ssh-priv-key=${SSH_PRIV_KEY} -log-level=${LOG_LEVEL} -job-file=/tmp/${JOBLIST_FILE} -hpc-label=${HPC_LABEL} -deployment-label=${DEPLOYMENT_LABEL}" -deployment-id=${DEPLOYMENT_ID}]

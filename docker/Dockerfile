FROM golang:1.16-alpine

# LABEL maintainer="jesus.ramos.external@atos.net"

RUN mkdir /app
WORKDIR /app

# Install git
RUN apk add --update --no-cache git

# Get HPC exporter
RUN git clone -b multi-tenant https://github.com/SODALITE-EU/hpc-exporter.git
# Build the code
RUN cd hpc-exporter && go build

EXPOSE ${EXPORTER_PORT}

CMD [ "sh", "-c", "./hpc-exporter/hpc_exporter "]

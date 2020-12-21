FROM golang:1.13.8-alpine

# LABEL maintainer="fabeirojorge.secondwindow@gmail.com"

RUN mkdir /app
WORKDIR /app

# Copy private key
COPY ./id_rsa /app/id_rsa

# Install git
RUN apk add --update --no-cache git

# Get HPC exporter
RUN git clone -b ignore-host-key https://github.com/SODALITE-EU/hpc-exporter.git

RUN cd hpc-exporter && go build

EXPOSE 9100

CMD ["./hpc-exporter/hpc_exporter", "-host=sodalite-fe.hlrs.de", "-ssh-user=jorge", "-ssh-private-key=id_rsa", "-log-level=info"]

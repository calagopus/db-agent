FROM alpine:latest

RUN apk add --no-cache ca-certificates && \
	update-ca-certificates

# Add calagopus-db-agent
ARG TARGETPLATFORM
COPY .docker/${TARGETPLATFORM#linux/}/calagopus-db-agent /usr/bin/calagopus-db-agent

ENV OCI_CONTAINER=official

ENTRYPOINT ["/usr/bin/calagopus-db-agent"]

#!/bin/bash

TIMEOUT=3
GATEWAY_HOST="flakyct1.dev.continuuity.net"
GATEWAY_PORT=10000
URL="http://${GATEWAY_HOST}:${GATEWAY_PORT}/v2/streams/event-stream"

curl -X POST -s --connect-timeout ${TIMEOUT} -d "$*" "${URL}"

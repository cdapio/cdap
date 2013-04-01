#!/bin/bash

#
# Sends random disk metrics
#

if [ $# -ne 1 ]; then
	echo "Required argument: <num_metrics>"
	exit
fi

num_metrics=$1

# Set pointers to URL endpoint
GATEWAY_HOSTNAME=${GATEWAY_HOSTNAME:=localhost}
GATEWAY_REST_BASE_URL=${GATEWAY_REST_BASE_URL:=http://${GATEWAY_HOSTNAME}:10000/stream/}
STREAM_NAME=${STREAM_NAME:=diskStatsStream}
GATEWAY_STREAM_URL=${GATEWAY_STREAM_URL:=$GATEWAY_REST_BASE_URL$STREAM_NAME}

# Initialize variables
timestamp=`date +%s` # Initial action id (increments from here)


# Generate random cpu spikes
for (( i=0; i<$num_metrics; i++ )); do
    disk=$RANDOM
    disk=$((disk*100000)) 
	metric=$timestamp", "$disk", "$HOSTNAME
    echo "Inserting action: $metric to $GATEWAY_STREAM_URL"
	curl  "$GATEWAY_STREAM_URL" --data "$metric"
	sleep 1
done

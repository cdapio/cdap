#!/bin/bash

#
# Script writes clusters in specified file to the clusters stream
#
# Configuration of hostname, port, and anything else is done with environment variables
#

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
READER_SCRIPT=${SCRIPT_DIR}/file_reader.sh

# Must know location of CONTINUUITY_HOME to be able to use scripts and it should contain stream-client

CONTINUUITY_HOME=${CONTINUUITY_HOME:=UNSET}
if [[ $CONTINUUITY_HOME == "UNSET" ]]; then
	echo "Must have CONTINUUITY_HOME environment variable set (ie. export CONTINUUITY_HOME=/usr/local/continuuity"
	exit
fi

SCRIPT_NAME=${SCRIPT_NAME:=stream-client}
SCRIPT=${CONTINUUITY_HOME}/bin/${SCRIPT_NAME}
if [ ! -f $SCRIPT ]; then
	echo "Did not find $SCRIPT_NAME at location $SCRIPT"
	exit
fi

# Set defaults (reading existing environment variables if specified)

GATEWAY_HOSTNAME=${GATEWAY_HOSTNAME:=localhost}
CLUSTER_STREAM_NAME=${CLUSTER_STREAM_NAME:=clusters}

MAX_CLUSTERS=50
CLEAR_CLUSTERS_CSV=${CLEAR_CLUSTERS_CSV:="reset_clusters,${MAX_CLUSTERS},\"Cluster reset from cmdline script\""} # "

# Require one argument (filename of csv clusters file)

if [ $# -ne 1 ]; then
	echo "Required argument: <clusters_filename.csv>"
	exit
fi

filename=$1

if [ ! -f $filename ]; then
	echo "File not found: $filename"
	exit
fi

# Perform clear of clusters

$SCRIPT send --stream $CLUSTER_STREAM_NAME --body "$CLEAR_CLUSTERS_CSV" --host $GATEWAY_HOSTNAME

# Read file, for each line, write to gateway

entries=0
lines=`$READER_SCRIPT $filename` # sub-optimal but should be okay if fewer than 1000s of entries

while read -r line; do
	if [[ $line == cluster* ]]; then echo -n;
	else
		echo "Sending cluster config line: $line"
		$SCRIPT send --stream $CLUSTER_STREAM_NAME --body "$line" --host $GATEWAY_HOSTNAME
		((entries++))
	fi
done <<< "$lines"

echo "Completed.  Wrote $entries CSV lines of cluster configuration"

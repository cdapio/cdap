#!/bin/bash

#
# Script writes JSON formatted social actions in specified file to the social-actions stream

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
ACTIONS_STREAM_NAME=${ACTIONS_STREAM_NAME:=social-actions}

# Require one argument (filename of json actions file)

if [ $# -ne 1 ]; then
	echo "Required argument: <social_actions.json>"
	exit
fi

filename=$1

if [ ! -f $filename ]; then
	echo "File not found: $filename"
	exit
fi

# Read file, for each line, write to gateway

entries=0
lines=`$READER_SCRIPT $filename` # sub-optimal but should be okay if fewer than 1000s of entries

while read -r line; do
	if [[ $line == cluster* ]]; then echo -n;
	else
		echo "Sending social action: $line"
		$SCRIPT send --stream $ACTIONS_STREAM_NAME --body "$line" --host $GATEWAY_HOSTNAME
		((entries++))
	fi
done <<< "$lines"

echo "Completed.  Wrote $entries JSON entries of social actions"

#!/usr/bin/env bash

# Set environment variables here.

# Main cmd is the non-java command to run.
MAIN_CMD=node

export NODE_ENV=production

# Arguments for MAIN_CMD
MAIN_CMD_ARGS="$CDAP_HOME/ui/server.js"

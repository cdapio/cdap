#!/usr/bin/env bash

# Set environment variables here.

# Main cmd is the non-java command to run.  
MAIN_CMD=node

export NODE_ENV=production

WEB_APP_PATH="$CDAP_HOME/web-app/server/main.js"

# if ENABLE_BETA_UI is set then start beta ui
if $ENABLE_BETA_UI; then
  WEB_APP_PATH="$CDAP_HOME/web-app/beta/server.js"
fi

# Arguments for MAIN_CMD
MAIN_CMD_ARGS="$WEB_APP_PATH"


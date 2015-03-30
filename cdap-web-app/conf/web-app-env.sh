#!/usr/bin/env bash

# Set environment variables here.

# Main cmd is the non-java command to run.  
MAIN_CMD=node

export NODE_ENV=production

WEB_APP_PATH="$CDAP_HOME/web-app/server/main.js"

# if ENABLE_ALPHA_UI is set then start new ui
if [ "$ENABLE_ALPHA_UI" == "true" ]; then
  WEB_APP_PATH="$CDAP_HOME/web-app/alpha/server.js"
fi

# Arguments for MAIN_CMD
MAIN_CMD_ARGS="$WEB_APP_PATH"


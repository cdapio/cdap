#!/usr/bin/env bash
#
# Copyright © 2015-2016 Cask Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#


# Main cmd is the non-java command to run.
MAIN_CMD=node

# Check for embedded node binary, and ensure it's the correct binary ABI for this system
if test -x ${CDAP_HOME}/ui/bin/node ; then
  ${CDAP_HOME}/ui/bin/node --version >/dev/null 2>&1
  if [ $? -eq 0 ] ; then
    MAIN_CMD=${CDAP_HOME}/ui/bin/node
  elif [[ $(which node 2>/dev/null) ]]; then
    MAIN_CMD=node
  else
    echo "Unable to locate Node.js binary (node), is it installed and in the PATH?"
    exit 1
  fi
fi

declare -r MAIN_CMD=${MAIN_CMD}
export NODE_ENV=production

# Arguments for MAIN_CMD
declare -r MAIN_CMD_ARGS="${CDAP_HOME}/ui/server.js"

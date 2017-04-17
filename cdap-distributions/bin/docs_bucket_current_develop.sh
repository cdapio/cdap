#!/usr/bin/env bash
#
# Copyright © 2017 Cask Data, Inc.
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

LOCAL_DIR=${LOCAL_DIR:-cdap}
S3_BUCKET=${S3_BUCKET:-docs.cask.co}

# Get Docs versions
echo "Getting Docs version from local directory: ${LOCAL_DIR}"
__local=${VERSION:-$(ls -1F ${LOCAL_DIR}/ | sort -rn | grep '/$' | tail -n 1 | sed -e 's:/$::')}
echo "Getting Docs versions from remote: ${S3_BUCKET}/${LOCAL_DIR}"
__current=$(curl -sL http://s3.amazonaws.com/${S3_BUCKET}/${LOCAL_DIR}/version)
__develop=$(curl -sL http://s3.amazonaws.com/${S3_BUCKET}/${LOCAL_DIR}/development)
echo
echo "Local CDAP Docs version: ${__local}"
echo "Remote CDAP Docs current version: ${__current}"
echo "Remote CDAP Docs development version: ${__develop}"
echo

__s3cmd=${bamboo_capability_system_builder_command_s3cmd:-s3cmd}

function __s3copy() {
  local __dir=${1:-current} __opts="--progress --recursive"
  echo "Remote copying ${__local} to ${__dir}"
  ${__s3cmd} cp --acl-public ${__opts} s3://${S3_BUCKET}/${LOCAL_DIR}/${__local}/ s3://${S3_BUCKET}/${LOCAL_DIR}/${__dir}/
  __ret=$?
  if [[ ${__ret} -ne 0 ]]; then
    echo "ERROR: Failed to s3cmd cp ${__local} to ${__dir}"
    exit 1
  fi
  # If we get here, assuming we're good
  echo "Docs ${__local} are copied to ${__dir}"
}

# Are we develop?
if [[ ${__local} == ${__develop} ]]; then
  __s3copy develop
# Are we current?
elif [[ ${__local} == ${__current} ]]; then
  __s3copy
else
  echo "${__local} matches neither current nor develop... skipping"
fi
exit 0

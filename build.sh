#!/usr/bin/env bash

# Copyright © 2015 Cask Data, Inc.
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
  
# Builds README.rst file for GitHub pages

SCRIPT=`basename ${0}`
SCRIPT_PATH=`pwd`
RETURN=$(printf '\n')
CDAP="cdap"
CDAP_PATH="${SCRIPT_PATH}/${CDAP}"
README_FILE="README.rst"
README_FILE_PATH="${SCRIPT_PATH}/${README_FILE}"

READ_ME_BASE=`cat <<EOF
=================
CDAP GitHub Pages
=================

Cask Data Application Platform (CDAP) Documentation
EOF`

READ_ME_EARLIER=`cat <<EOF
Earlier versions:
EOF`

function get_current_version() {
  for release in $(ls -d */ | sort --reverse)
    do
      if [[ "${release}" =~ ^[0-9].* ]]; then
        CURRENT="${release%%/}"
        return
      fi
    done
}

function write_version() {
  local rel="${1%%/}"
  echo "- \`Version ${rel} <http://docs.cdap.io/cdap/${rel}>\`__" >> ${README_FILE_PATH}
}


echo "${READ_ME_BASE}" > ${README_FILE_PATH}

cd ${CDAP_PATH}

get_current_version
echo "${RETURN}" >> ${README_FILE_PATH}
echo "\`Latest version: ${CURRENT} <http://docs.cdap.io/cdap/current>\`__" >> ${README_FILE_PATH}
echo "${RETURN}" >> ${README_FILE_PATH}
write_version ${CURRENT}
echo "${RETURN}" >> ${README_FILE_PATH}

echo "${READ_ME_EARLIER}" >> ${README_FILE_PATH}
echo "${RETURN}" >> ${README_FILE_PATH}

for release in $(ls -d */ | sort --reverse)
do
  rel="${release%%/}"
  if [[ "$rel" != "${CURRENT}" && "$rel" != "current" ]]; then
    write_version ${rel}
  fi
done

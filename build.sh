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

SCRIPT_PATH=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

CDAP_PATH="${SCRIPT_PATH}/cdap"
README_FILE_PATH="${SCRIPT_PATH}/README.rst"

READ_ME_BASE=`cat <<EOF
=================
CDAP GitHub Pages
=================

Cask Data Application Platform (CDAP) Documentation
EOF`

function get_current_version() {
  for release in $(ls -d ${CDAP_PATH}/*/ | sort --reverse )
    do
      rel="$(basename $release)"
      if [[ "${rel}" =~ ^[0-9].* ]]; then
        CURRENT="${rel%%/}"
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

echo >> ${README_FILE_PATH}
echo "\`Latest version: ${CURRENT} <http://docs.cdap.io/cdap/current>\`__" >> ${README_FILE_PATH}
echo >> ${README_FILE_PATH}
write_version ${CURRENT}

echo >> ${README_FILE_PATH}
echo "Earlier versions:" >> ${README_FILE_PATH}
echo >> ${README_FILE_PATH}

for release in $(ls -d ${CDAP_PATH}/*/ | sort --reverse )
do
  rel="$(basename $release)"
  rel="${rel%%/}"
  if [[ "$rel" != "${CURRENT}" && "$rel" != "current" ]]; then
    write_version ${rel}
  fi
done

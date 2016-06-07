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
# NOTE: This has not been updated to match current style of page with spaces and titles
# Until this is fixed, please edit the RST file manually!

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
        return 0
      fi
    done
  return 1
}

function write_version() {
  local rel="${1%%/}"
  echo "- \`Version ${rel} <http://docs.cdap.io/cdap/${rel}>\`__" >&1
}


cd ${CDAP_PATH}

get_current_version
if [ $? -ne 0 ] ; then echo "Failed to get current version" && exit 1 ; fi

echo "${READ_ME_BASE}" > ${README_FILE_PATH}

echo >> ${README_FILE_PATH}
echo "\`Latest version: ${CURRENT} <http://docs.cdap.io/cdap/current>\`__" >> ${README_FILE_PATH}
echo >> ${README_FILE_PATH}
write_version ${CURRENT} 1>> ${README_FILE_PATH}

echo >> ${README_FILE_PATH}
echo "Earlier versions:" >> ${README_FILE_PATH}
echo >> ${README_FILE_PATH}

for release in $(ls -d ${CDAP_PATH}/*/ | sort --reverse )
do
  rel="$(basename $release)"
  rel="${rel%%/}"
  if [[ "$rel" != "${CURRENT}" && "$rel" != "current" ]]; then
    write_version ${rel} 1>> ${README_FILE_PATH}
  fi
done

exit 0

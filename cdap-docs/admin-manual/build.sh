#!/usr/bin/env bash

# Copyright © 2014-2017 Cask Data, Inc.
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

# Build script for docs

source ../vars
source ../_common/common-build.sh

DEFAULT_XML="../../cdap-common/src/main/resources/cdap-default.xml"
DEFAULT_XML_MD5_HASH="97ddb1f9b71fa064d57b094d8b306865"

DEFAULT_TOOL="../tools/cdap-default/doc-cdap-default.py"
DEFAULT_DEPRECATED_XML="../tools/cdap-default/cdap-default-deprecated.xml"
DEFAULT_RST="cdap-default-table.rst"
DEFAULT_DEPRECATED_RST="cdap-default-deprecated-table.rst"

CHECK_INCLUDES=${TRUE}

function rewrite_references_sed() {
  local source_rst=${1}
  local target_rst=${2}
  local source_pattern=${3}
  local target_pattern=${4}
  sed -e "s|${source_pattern}|${target_pattern}|g" ${source_rst} > ${target_rst}
  echo "Copied file ${source_rst} changing '${source_pattern}' to '${target_pattern}'"
}

function rewrite_references_in_place_sed() {
  local source_rst=${1}
  local source_pattern=${2}
  local target_pattern=${3}
  local target_pattern_escaped=$(printf '%s\n' "${target_pattern}" | sed 's,[\/&],\\&,g;s/$/\\/')
  target_pattern_escaped=${target_pattern_escaped%?}
  sed -e "s|${source_pattern}|${target_pattern_escaped}|g" -i_bu ${source_rst}
  echo "Rewrote file ${source_rst} changing '${source_pattern}' to '${target_pattern}'"
}

function download_includes() {
  local target_includes_dir=${1}

  echo_red_bold "Check guarded files for changes."
  test_an_include "${DEFAULT_XML_MD5_HASH}" "${DEFAULT_XML}"

  echo "Building rst file from cdap-default.xml..."
  python "${DEFAULT_TOOL}" --generate --target "${target_includes_dir}/${DEFAULT_RST}"
  echo "Building rst file from cdap-default-deprecated.xml..."
  python "${DEFAULT_TOOL}" -g -i -s ${DEFAULT_DEPRECATED_XML} -t "${target_includes_dir}/${DEFAULT_DEPRECATED_RST}"

  echo "Copying files, changing references..."
  local source_rst="${target_includes_dir}/../../source/_includes/installation"
  local pattern="\|distribution\|"
  local distributions="cloudera ambari mapr packages"
  local types="configuration hdfs-permissions installation starting"
  local dist
  local type
  for dist in ${distributions}; do
    for type in ${types}; do
      rewrite_references_sed "${source_rst}/${type}.txt" "${target_includes_dir}/${dist}-${type}.rst" "${pattern}" "${dist}"
    done
    echo
  done

  distributions="mapr packages"
  for dist in ${distributions}; do
    type="ha-installation"
    rewrite_references_sed "${source_rst}/${type}.txt" "${target_includes_dir}/${dist}-${type}.rst" "${pattern}" "${dist}"
  done
  
  types="configuration hdfs-permissions"
  for type in ${types}; do
    rewrite_references_in_place_sed "${target_includes_dir}/mapr-${type}.rst" " su hdfs" " su mapr"
  done
  
  local source_pattern="(FQDN1:2181,FQDN2:2181)"
  local target_pattern=$(cat <<EOF
(FQDN1:5181,FQDN2:5181);
             note that the MapR default of 5181 is different than the ZooKeeper default of 2181
EOF
)
  rewrite_references_in_place_sed "${target_includes_dir}/mapr-configuration.rst" "${source_pattern}" "${target_pattern}"
  
  echo
}

run_command ${1}

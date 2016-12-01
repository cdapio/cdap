#!/usr/bin/env bash

# Copyright © 2014-2016 Cask Data, Inc.
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
DEFAULT_XML_MD5_HASH="3fc88d79a92bd28aa83a52e501b9a9b0"

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
  local types="installation configuration starting"
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
  echo  
}

run_command ${1}

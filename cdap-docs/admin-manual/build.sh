#!/usr/bin/env bash

# Copyright © 2014-2015 Cask Data, Inc.
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

source ../_common/common-build.sh

DEFAULT_XML="../../cdap-common/src/main/resources/cdap-default.xml"
DEFAULT_TOOL="../tools/doc-cdap-default.py"
DEFAULT_RST="cdap-default-table.rst"
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
  test_an_include d01d16f0b6aa1c0398b1840f08f2fb4b "${DEFAULT_XML}"

  echo "Building rst file from cdap-default.xml..." 
  python "${DEFAULT_TOOL}" -g -t "${target_includes_dir}/${DEFAULT_RST}"
  
  echo "Copying files, changing references..."
  local source_rst="${target_includes_dir}/../../source/_includes/installation"
  
  rewrite_references_sed "${source_rst}/installation.txt"          "${target_includes_dir}/ambari-installation.rst"          ".. _distribution-" ".. _ambari-"
  echo
  
  rewrite_references_sed "${source_rst}/configuration.txt"         "${target_includes_dir}/hadoop-configuration.rst"         ".. _distribution-" ".. _hadoop-"
  rewrite_references_sed "${source_rst}/installation.txt"          "${target_includes_dir}/hadoop-installation.rst"          ".. _distribution-" ".. _hadoop-"
  rewrite_references_sed "${source_rst}/starting-verification.txt" "${target_includes_dir}/hadoop-starting-verification.rst" ".. _distribution-" ".. _hadoop-"
  rewrite_references_sed "${source_rst}/upgrading.txt"             "${target_includes_dir}/hadoop-upgrading.rst"             ".. _distribution-" ".. _hadoop-"
  echo
  
  rewrite_references_sed "${source_rst}/configuration.txt"         "${target_includes_dir}/mapr-configuration.rst"         ".. _distribution-" ".. _mapr-"
  rewrite_references_sed "${source_rst}/installation.txt"          "${target_includes_dir}/mapr-installation.rst"          ".. _distribution-" ".. _mapr-"
  rewrite_references_sed "${source_rst}/starting-verification.txt" "${target_includes_dir}/mapr-starting-verification.rst" ".. _distribution-" ".. _mapr-"
  rewrite_references_sed "${source_rst}/upgrading.txt"             "${target_includes_dir}/mapr-upgrading.rst"             ".. _distribution-" ".. _mapr-"
}

run_command ${1}

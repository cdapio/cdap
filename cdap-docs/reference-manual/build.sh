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
# Builds the docs (all except javadocs and PDFs) from the .rst source files using Sphinx
# Builds the javadocs and copies them into place
# Zips everything up so it can be staged
# REST PDF is built as a separate target and checked in, as it is only used in SDK and not website
# Target for building the SDK
# Targets for both a limited and complete set of javadocs
# Targets not included in usage are intended for internal usage by script

source ../vars
source ../_common/common-build.sh

CLI_DOC_TOOL="../tools/docs-cli-commands.py"
CLI_INPUT_TXT="${PROJECT_PATH}/cdap-docs-gen/target/cdap-docs-cli.txt"
CLI_TABLE_RST="cdap-cli-table.rst"

CHECK_INCLUDES=${TRUE}

function download_includes() {
  local target_includes_dir=${1}
  echo "Copying CLI Docs: building rst file from cli-docs results..." 
  python "${CLI_DOC_TOOL}" "${CLI_INPUT_TXT}" "${target_includes_dir}/${CLI_TABLE_RST}"
  warnings=$?
  if [[ ${warnings} -eq 0 ]]; then
    echo "CLI rst file written to ${CLI_TABLE_RST}"
  else
    local m="Error ${warnings} building CLI docs table"
    echo_red_bold "${m}"
    set_message "${m}"
  fi
  return ${warnings}
}

function build_extras() {
  echo_red_bold "Building extras."

  if [[ -n ${USING_JAVADOCS} ]]; then
    echo "Copying Javadocs."
    rm -rf ${TARGET_PATH}/html/javadocs
    cp -r ${API_JAVADOCS} ${TARGET_PATH}/html/.
    warnings=$?
    if [[ ${warnings} -ne 0 ]]; then
      set_message "Unable to copy new Javadocs"
      return ${warnings}
    fi
    mv -f ${TARGET_PATH}/html/${API_DOCS} ${TARGET_PATH}/html/javadocs
    warnings=$?
    if [[ ${warnings} -ne 0 ]]; then
      set_message "Unable to move new Javadocs into place"
      return ${warnings}
    fi
    echo "Copied Javadocs."
  else
    echo "Not using Javadocs."
  fi

  cp ${SCRIPT_PATH}/licenses-pdf/*.pdf ${TARGET_PATH}/html/licenses
  warnings=$?
  if [[ ${warnings} -eq 0 ]]; then
    echo "Copied license PDFs."
  else
    local m="Error ${warnings} copying license PDFs."
    echo_red_bold "${m}"
    set_message "${m}"
    return ${warnings}
  fi
}

run_command ${1}

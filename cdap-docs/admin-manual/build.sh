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

function download_includes() {
  echo_red_bold "Check guarded files for changes"
  test_an_include 5337fe50a780b2e0118f2891ec2a475e "${DEFAULT_XML}"

  echo "Building rst file from cdap-default.xml..."
  local includes_dir=${1}
  python "${DEFAULT_TOOL}" -g -t "${includes_dir}/${DEFAULT_RST}"
}

run_command ${1}

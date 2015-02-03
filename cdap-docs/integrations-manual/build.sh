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

CHECK_INCLUDES=$TRUE

function pandoc_includes() {
  INCLUDES_DIR=$1
  version
  cd $SCRIPT_PATH
  local cloudera="$SCRIPT_PATH/$SOURCE/cloudera"
  rewrite $cloudera/configuring.txt  $INCLUDES_DIR/configuring.rst  "<version>"  $PROJECT_VERSION
}

run_command $1

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

source _common/common-build.sh

CHECK_INCLUDES=$TRUE

function download_java_code() {
  # Re-writes the links in the RST file to point to a local copy of any image links.
  local includes_dir=$1
  local source_dir=$2
#   local file_dir=$3
  local file_name=$3.java
  echo "Downloading using curl $file_name from $source_dir"

  if [ ! -d "$includes_dir" ]; then
    mkdir $includes_dir
  fi
 
#   mkdir $includes_dir/$file_dir
  curl $source_dir/$file_name --output $includes_dir/$file_name --silent
}

function pandoc_includes() {
  echo "Downloads source code includes..."
  version
  local project_version=$PROJECT_SHORT_VERSION
  local project_source=https://raw.githubusercontent.com/caskdata/cdap-apps/release/cdap-$project_version-compatible/Wise/src/main/java/co/cask/cdap/apps/wise

  download_java_code $1 $project_source BounceCountsMapReduce
  download_java_code $1 $project_source BounceCountStore
  download_java_code $1 $project_source PageViewStore
  download_java_code $1 $project_source WiseApp
  download_java_code $1 $project_source WiseFlow
  download_java_code $1 $project_source WiseWorkflow
  download_java_code $1 $project_source WiseService
}

run_command $1

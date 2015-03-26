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

function download_java_file() {
  # Downloads a Java file to the includes directory, and checks that it hasn't changed.
  # Uses md5 hashes to monitor if any files have changed.
  local includes_dir=$1
  local source_dir=$2
  local file_name=$3.java
  local md5_hash=$4
  local target=$includes_dir/$file_name
  
  if [ ! -d "$includes_dir" ]; then
    mkdir $includes_dir
    echo "Creating Includes Directory: $includes_dir"
  fi

  echo "Downloading using curl $file_name from $source_dir"
  curl $source_dir/$file_name --output $target --silent
  local new_md5_hash=`md5 -q $target`
  if [ "x$md5_hash" != "x$new_md5_hash" ]; then
    echo -e "$WARNING MD5 Hash for $file_name has changed! Compare files and update hash!"  
    echo "Old md5_hash: $md5_hash New md5_hash: $new_md5_hash"
  fi
}

function pandoc_includes() {
  echo "Download source code includes from GitHub..."
  version
  local project_version=$PROJECT_SHORT_VERSION
  local project_source=https://raw.githubusercontent.com/caskdata/cdap-apps/release/cdap-$project_version-compatible/Wise/src/main/java/co/cask/cdap/apps/wise
  # 1:Includes directory 2:GitHub directory 3:Java filename   4:MD5 hash of file
  download_java_file $1 $project_source BounceCountsMapReduce f2f8d36e4049ba69b40282057accf38a
  download_java_file $1 $project_source BounceCountStore      d476c15655c6a6c6cd7fe682dea4a8b7
  download_java_file $1 $project_source PageViewStore         576d76c60b8f5fddeee916a87318d209
  download_java_file $1 $project_source WiseApp               69825da7d5f6d1852fd5d28415418a45
  download_java_file $1 $project_source WiseFlow              2deba0633a0dcca14ef426929f543872
  download_java_file $1 $project_source WiseWorkflow          8fe51eed165e85d95c4f5e25953e3489
  download_java_file $1 $project_source WiseService           ed54e1e9952e4a880a9fc4216fdf7b4e
}

run_command $1

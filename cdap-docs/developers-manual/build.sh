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
# Builds the docs (all except javadocs and PDFs) from the .rst source files using Sphinx
# Builds the javadocs and copies them into place
# Zips everything up so it can be staged
# REST PDF is built as a separate target and checked in, as it is only used in SDK and not website
# Target for building the SDK
# Targets for both a limited and complete set of javadocs
# Targets not included in usage are intended for internal usage by script

source ../_common/common-build.sh

CHECK_INCLUDES=$TRUE

function download_readme_file_and_test() {
  # Downloads a README.rst file to a target directory, and checks that it hasn't changed.
  # Uses md5 hashes to monitor if any files have changed.

  local file_name='README.rst'
  
  local includes_dir=${1}
  local source_url=${2}
  local md5_hash=${3}
  local relative_path=${4}

  # Replace any path components with dashes
  local target_file_name=${relative_path//\//-}.rst

  echo "Downloading using curl ${file_name} from ${source_url}"
  curl ${source_url}/${relative_path}/${file_name} --output ${includes_dir}/${target_file_name} --silent
  test_an_include ${md5_hash} ${target_file_name}
}

function download_includes() {
  echo "Downloading source files includes from GitHub..."
  local github_url="https://raw.githubusercontent.com/caskdata"
  
  local includes_dir=${1}
  if [ ! -d "${includes_dir}" ]; then
    mkdir ${includes_dir}
    echo "Creating Includes Directory: ${includes_dir}"
  fi

  version
  local project_version=${PROJECT_SHORT_VERSION}

  if [ "x${GIT_BRANCH_TYPE}" == "xdevelop" ] || [ "x${GIT_BRANCH_TYPE}" == "xfeature" ] ; then
    local branch="develop"
  else
    local branch="release/cdap-${project_version}-compatible"
  fi

# cdap-clients
# https://raw.githubusercontent.com/caskdata/cdap-clients/develop/cdap-authentication-clients/java/README.rst
  local clients_url="${github_url}/cdap-clients/${branch}"

  download_readme_file_and_test ${includes_dir} ${clients_url} bf10a586e605be8191b3b554c425c3aa cdap-authentication-clients/java
  download_readme_file_and_test ${includes_dir} ${clients_url} f075935545e48a132d014c6a8d32122a cdap-authentication-clients/javascript
  download_readme_file_and_test ${includes_dir} ${clients_url} 1f8330e0370b3895c38452f9af72506a cdap-authentication-clients/python
  download_readme_file_and_test ${includes_dir} ${clients_url} 33b06b7ca1e423e93f2bb2c6f7d00e21 cdap-authentication-clients/ruby
  
# cdap-ingest
# https://raw.githubusercontent.com/caskdata/cdap-ingest/develop/cdap-file-drop-zone/README.rst
  local ingest_url="${github_url}/cdap-ingest/${branch}"

  download_readme_file_and_test ${includes_dir} ${ingest_url} b1ef01a18acd5408c7ac6578cd868a8f cdap-file-drop-zone
  download_readme_file_and_test ${includes_dir} ${ingest_url} 360e88f2fe639857c86b49f8d987a5a4 cdap-file-tailer
  download_readme_file_and_test ${includes_dir} ${ingest_url} 0d56d354a14260806e150732792ba96d cdap-flume
  download_readme_file_and_test ${includes_dir} ${ingest_url} a3b24fee83ef104c075e58e1401517f2 cdap-stream-clients/java
  download_readme_file_and_test ${includes_dir} ${ingest_url} 277ded1924cb8d9b52a007f262820002 cdap-stream-clients/javascript
  download_readme_file_and_test ${includes_dir} ${ingest_url} 682b6710598d7701908db3b048729a83 cdap-stream-clients/python
  download_readme_file_and_test ${includes_dir} ${ingest_url} 8f479cb5eb14ee97cb7501c1e8d9f9e0 cdap-stream-clients/ruby
}

function test_includes () {
  echo "All includes tested."
}

run_command ${1}

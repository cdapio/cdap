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

CDAP_CLIENTS_RELEASE_VERSION="1.2.0"
CDAP_INGEST_RELEASE_VERSION="1.3.0"
CHECK_INCLUDES=${TRUE}

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
  test_an_include ${md5_hash} ${includes_dir}/${target_file_name}
}

function download_includes() {
  echo "Downloading source files to be included from GitHub..."
  local github_url="https://raw.githubusercontent.com/caskdata"
  local includes_dir=${1}
  set_version

  local clients_branch="release/${CDAP_CLIENTS_RELEASE_VERSION}"
  local ingest_branch="release/${CDAP_INGEST_RELEASE_VERSION}"

  if [ "x${GIT_BRANCH_TYPE:0:7}" == "xdevelop" ]; then
    clients_branch="develop"
    ingest_branch="develop"
  fi

# cdap-clients
# https://raw.githubusercontent.com/caskdata/cdap-clients/develop/cdap-authentication-clients/java/README.rst
  local clients_url="${github_url}/cdap-clients/${clients_branch}"

  download_readme_file_and_test ${includes_dir} ${clients_url} f99720412e7085fdc3e350205ce21bcc cdap-authentication-clients/java
#   download_readme_file_and_test ${includes_dir} ${clients_url} f075935545e48a132d014c6a8d32122a cdap-authentication-clients/javascript
  download_readme_file_and_test ${includes_dir} ${clients_url} 1f8330e0370b3895c38452f9af72506a cdap-authentication-clients/python
#   download_readme_file_and_test ${includes_dir} ${clients_url} c16bf5ce7c1f0a2a4a680974a848cdd0 cdap-authentication-clients/ruby
  
# cdap-ingest
# https://raw.githubusercontent.com/caskdata/cdap-ingest/develop/cdap-file-drop-zone/README.rst
  local ingest_url="${github_url}/cdap-ingest/${ingest_branch}"

  download_readme_file_and_test ${includes_dir} ${ingest_url} c9b6db1741afa823c362237488c2d8f0 cdap-flume
  download_readme_file_and_test ${includes_dir} ${ingest_url} 08bb5c37085d354834860cb4ca66c121 cdap-stream-clients/java
#   download_readme_file_and_test ${includes_dir} ${ingest_url} 277ded1924cb8d9b52a007f262820002 cdap-stream-clients/javascript
  download_readme_file_and_test ${includes_dir} ${ingest_url} 3013f72ea3454e43adedda2aed40abc1 cdap-stream-clients/python
  download_readme_file_and_test ${includes_dir} ${ingest_url} 5fc88ec3a658062775403f5be30afbe9 cdap-stream-clients/ruby

  echo_red_bold "Check included example files for changes"
  test_an_include b53dd493c4e9c2cb89b593baa4139087 ../../cdap-examples/Purchase/src/main/java/co/cask/cdap/examples/purchase/PurchaseHistoryBuilder.java
  test_an_include 80216a08a2b3d480e4a081722408222f ../../cdap-examples/Purchase/src/main/java/co/cask/cdap/examples/purchase/PurchaseHistoryService.java
  test_an_include 29fe1471372678115e643b0ad431b28d ../../cdap-examples/Purchase/src/main/java/co/cask/cdap/examples/purchase/PurchaseStore.java
  test_an_include 7f83b852a8e7b4594094d4e88d80a0b4 ../../cdap-examples/SparkPageRank/src/main/java/co/cask/cdap/examples/sparkpagerank/SparkPageRankApp.java
  test_an_include b5aa09ede42a877e15df5e482607e256 ../../cdap-examples/WikipediaPipeline/src/main/java/co/cask/cdap/examples/wikipedia/TopNMapReduce.java
  test_an_include 62144353b40314640d74d4b675432620 ../../cdap-examples/WikipediaPipeline/src/main/scala/co/cask/cdap/examples/wikipedia/ClusteringUtils.scala
}

function test_includes () {
  echo "All includes tested."
}

run_command ${1}

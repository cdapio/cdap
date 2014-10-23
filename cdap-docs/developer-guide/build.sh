#!/usr/bin/env bash

# Copyright © 2014 Cask Data, Inc.
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

function pandoc_includes() {
  # Uses pandoc to translate the README markdown files to rst in the target directory
  INCLUDES_DIR=$1
  
  if [ $TEST_INCLUDES == $TEST_INCLUDES_LOCAL ]; then
    MD_CLIENTS="../../../cdap-clients"
    MD_INGEST="../../../cdap-ingest"
  else
    GITHUB_URL="https://raw.githubusercontent.com/caskdata"
    VERSION="v1.0.1" # after tagging
    MD_CLIENTS="$GITHUB_URL/cdap-clients/$VERSION"
    MD_INGEST="$GITHUB_URL/cdap-ingest/$VERSION"
  fi

  echo "Using $TEST_INCLUDES includes..."
  #   authentication-client java
  pandoc -t rst -r markdown $MD_CLIENTS/cdap-authentication-clients/java/README.md  -o $INCLUDES_DIR/cdap-authentication-clients-java.rst
  
  #   authentication-client python
  pandoc -t rst -r markdown $MD_CLIENTS/cdap-authentication-clients/python/README.md  -o $INCLUDES_DIR/cdap-authentication-clients-python.rst
  
  #   file-drop-zone
  pandoc -t rst -r markdown $MD_INGEST/cdap-file-drop-zone/README.md  -o $INCLUDES_DIR/cdap-file-drop-zone.rst
  
  #   file-tailer
  pandoc -t rst -r markdown $MD_INGEST/cdap-file-tailer/README.md  -o $INCLUDES_DIR/cdap-file-tailer.rst
  
  #   flume
  pandoc -t rst -r markdown $MD_INGEST/cdap-flume/README.md  -o $INCLUDES_DIR/cdap-flume.rst
  
  #   stream-client java
  pandoc -t rst -r markdown $MD_INGEST/cdap-stream-clients/java/README.md  -o $INCLUDES_DIR/cdap-stream-clients-java.rst
  
  #   stream-client python
  pandoc -t rst -r markdown $MD_INGEST/cdap-stream-clients/python/README.md  -o $INCLUDES_DIR/cdap-stream-clients-python.rst
}

function test_includes () {
  # List of includes to be tested
  test_an_include cdap-authentication-clients-java.rst
  test_an_include cdap-authentication-clients-python.rst
  test_an_include cdap-file-drop-zone.rst
  test_an_include cdap-file-tailer.rst
  test_an_include cdap-flume.rst
  test_an_include cdap-stream-clients-java.rst
  test_an_include cdap-stream-clients-python.rst
}

run_command $1

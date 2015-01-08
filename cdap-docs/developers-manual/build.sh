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
    # For the local to work, must have the local sources synced to the correct branch as the remote.
    MD_CLIENTS="../../../cdap-clients"
    MD_INGEST="../../../cdap-ingest"
  elif [ $TEST_INCLUDES == $TEST_INCLUDES_REMOTE ]; then
    # https://raw.githubusercontent.com/caskdata/cdap-clients/develop/cdap-authentication-clients/java/README.md
    # https://raw.githubusercontent.com/caskdata/cdap-ingest/release/1.0.0/cdap-file-drop-zone/README.md
    GITHUB_URL="https://raw.githubusercontent.com/caskdata"
    MD_CLIENTS="$GITHUB_URL/cdap-clients/release/1.1.0"
    MD_INGEST="$GITHUB_URL/cdap-ingest/release/1.0.0"
  fi

  local java_client_working="$INCLUDES_DIR/cdap-authentication-clients-java_working.rst"
  local java_client="$INCLUDES_DIR/cdap-authentication-clients-java.rst"

  if [ "x$TEST_INCLUDES" == "x$TEST_INCLUDES_LOCAL" -o "x$TEST_INCLUDES" == "x$TEST_INCLUDES_REMOTE" ]; then
    echo "Using $TEST_INCLUDES includes..."

    # authentication-client java
    pandoc -t rst -r markdown $MD_CLIENTS/cdap-authentication-clients/java/README.md    -o $java_client_working
    pandoc -t rst -r markdown $MD_CLIENTS/cdap-authentication-clients/python/README.md  -o $INCLUDES_DIR/cdap-authentication-clients-python.rst

    pandoc -t rst -r markdown $MD_INGEST/cdap-file-drop-zone/README.md        -o $INCLUDES_DIR/cdap-file-drop-zone.rst
    pandoc -t rst -r markdown $MD_INGEST/cdap-file-tailer/README.md           -o $INCLUDES_DIR/cdap-file-tailer.rst
    pandoc -t rst -r markdown $MD_INGEST/cdap-flume/README.md                 -o $INCLUDES_DIR/cdap-flume.rst
    pandoc -t rst -r markdown $MD_INGEST/cdap-stream-clients/java/README.md   -o $INCLUDES_DIR/cdap-stream-clients-java.rst
    pandoc -t rst -r markdown $MD_INGEST/cdap-stream-clients/python/README.md -o $INCLUDES_DIR/cdap-stream-clients-python.rst
  else
    echo -e "$WARNING Not testing includes: $TEST_INCLUDES includes..."
    local java_client_source="$SCRIPT_PATH/$SOURCE/$INCLUDES/cdap-authentication-clients-java.rst"
    cp -f $java_client_source $java_client_working
  fi
  
  # Fix version(s)
  local release_version="1.1.0-SNAPSHOT" # Version to be written into file
  rewrite $java_client_working $java_client "{version}" $release_version
  
  version
  cd $SCRIPT_PATH
  local get_start="$SCRIPT_PATH/$SOURCE/getting-started"
  rewrite $get_start/dev-env-version.txt               $INCLUDES_DIR/dev-env-versioned.rst         "<version>" $PROJECT_VERSION
  rewrite $get_start/start-stop-cdap-version.txt       $INCLUDES_DIR/start-stop-cdap-versioned.rst "<version>" $PROJECT_VERSION
  rewrite $get_start/standalone/standalone-version.txt $INCLUDES_DIR/standalone-versioned.rst      "<version>" $PROJECT_VERSION
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

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
CDAP_TUTORIAL="cdap-tutorial"
PROJECT=$CDAP_TUTORIAL

ARG_1="$1"
ARG_2="$2"
ARG_3="$3"

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
  local project_source=https://raw.githubusercontent.com/caskdata/cdap-apps/release/cdap-$project_version-compatible/Wise/src
  local project_main=$project_source/main/java/co/cask/cdap/apps/wise
  local project_test=$project_source/test/java/co/cask/cdap/apps/wise
  # 1:Includes directory 2:GitHub directory 3:Java filename   4:MD5 hash of file
  download_java_file $1 $project_main BounceCountsMapReduce f2f8d36e4049ba69b40282057accf38a
  download_java_file $1 $project_main BounceCountStore      d476c15655c6a6c6cd7fe682dea4a8b7
  download_java_file $1 $project_main PageViewStore         576d76c60b8f5fddeee916a87318d209
  download_java_file $1 $project_main WiseApp               69825da7d5f6d1852fd5d28415418a45
  download_java_file $1 $project_test WiseAppTest           7256c18cb80f59b4a9abcb5da320b337
  download_java_file $1 $project_main WiseFlow              2deba0633a0dcca14ef426929f543872
  download_java_file $1 $project_main WiseWorkflow          8fe51eed165e85d95c4f5e25953e3489
  download_java_file $1 $project_main WiseService           ed54e1e9952e4a880a9fc4216fdf7b4e
}


function build_docs_cdap_tutorial() {
  _build_docs "docs" $GOOGLE_ANALYTICS_WEB $WEB $FALSE
}

function _build_docs() {
  build $1
  build_zip $3
  display_version
  bell "Building $1 completed."
}

function clean() {
  cd $SCRIPT_PATH
  rm -rf $SCRIPT_PATH/$BUILD/*
  mkdir -p $SCRIPT_PATH/$BUILD/$HTML
  echo "Cleaned $BUILD directory"
}

function build_zip() {
  cd $SCRIPT_PATH
  make_zip $1
}

function make_zip() {
  version
  if [ "x$1" == "x" ]; then
    ZIP_DIR_NAME="$PROJECT-docs-$PROJECT_VERSION"
  else
    ZIP_DIR_NAME="$PROJECT-docs-$PROJECT_VERSION-$1"
  fi
  echo "ZIP_DIR_NAME: $ZIP_DIR_NAME"
  cd $SCRIPT_PATH/$BUILD
  mkdir $PROJECT_VERSION
  mv $HTML $PROJECT_VERSION/en
  # Add a redirect index.html file
  echo "$REDIRECT_EN_HTML" > $PROJECT_VERSION/index.html
  # Zip everything
  zip -qr $ZIP_DIR_NAME.zip $PROJECT_VERSION/* --exclude .DS_Store
}

function bell() {
  # Pass a message as $1
  echo -e "\a$1"
}

function usage() {
  cd $PROJECT_PATH
  PROJECT_PATH=`pwd`
  echo "Build script for '$PROJECT_CAPS' docs"
  echo "Usage: $SCRIPT < option > [source]"
  echo ""
  echo "  Options (select one)"
  echo "    docs-zip       Clean build of HTML docs, zipped for placing on docs.cask.co webserver"
  echo ""
  echo "  with"
  echo "    source         Path to $PROJECT source, if not $PROJECT_PATH"
  echo ""
}

function run_command() {
  case "$1" in
    docs-zip )          build_docs_cdap_tutorial; exit 1;;
    * )                 usage; exit 1;;
  esac
}

run_command $1

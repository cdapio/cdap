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
  
# Build script for guide docs
#
# Copies the original README from the mounted GitHub repo in the local filesystem
# running it through sed to modify all image links to be relative to the build/_includes directory.
# This modified README is included in the source RST files, and is used by Sphinx to build the HTML.

source ../_common/common-build.sh

CHECK_INCLUDES=$TRUE

function guide_rewrite() {
  INCLUDES_DIR=$1
  GUIDE=$2
  REDIRECT_T="\.\./\.\./\.\./\.\./\.\." # 5 redirects, escaped
  REDIRECT_S="../../../../.." # 5 redirects
  
  mkdir $INCLUDES_DIR/$GUIDE
  sed "s|image:: docs/images|image:: $REDIRECT_T/$GUIDE/docs/images|g" $INCLUDES_DIR/$REDIRECT_S/$GUIDE/README.rst > $INCLUDES_DIR/$GUIDE/README.rst
}

function pandoc_includes() {
  guide_rewrite $1 cdap-bi-guide
  guide_rewrite $1 cdap-flume-guide
  guide_rewrite $1 cdap-mapreduce-guide
  guide_rewrite $1 cdap-spark-guide
  guide_rewrite $1 cdap-timeseries-guide

  # Not yet completed
  #  guide_rewrite $1 cdap-flow-guide
  #  guide_rewrite $1 cdap-kafka-ingest-guide
  #  guide_rewrite $1 cdap-twitter-ingest-guide
}

run_command $1
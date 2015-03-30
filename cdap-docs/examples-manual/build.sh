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
  
# Build script for guide docs
#
# Copies the original README from the mounted GitHub repo in the local filesystem
# running it through sed to modify all image links to be relative to the build/_includes directory.
# This modified README is included in the source RST files, and is used by Sphinx to build the HTML.

source ../_common/common-build.sh

CHECK_INCLUDES=$TRUE

function guide_rewrite_sed() {
  echo "Re-writing using sed $1 $2"
  # Re-writes the links in the RST file to point to a local copy of any image links.
  local includes_dir=$1
  local guide=$2
  local project_version=$PROJECT_SHORT_VERSION
  
  local source1="https://raw.githubusercontent.com/cdap-guides"
  if [ "x$GIT_BRANCH_TYPE" == "xfeature" ]; then
    local source2="develop/README.rst"
  else
    local source2="release/cdap-$project_version-compatible/README.rst"
  fi

  local redirect="\.\./\.\./\.\./\.\./\.\." # Target, 5 redirects, escaped
  
  mkdir $includes_dir/$guide
  curl --silent $source1/$guide/$source2 --output $includes_dir/$guide/README_SOURCE.rst  
  sed -e "s|image:: docs/images|image:: $redirect/$guide/docs/images|g" -e "s|.. code:: |.. code-block:: |g" $includes_dir/$guide/README_SOURCE.rst > $includes_dir/$guide/README.rst
}

function pandoc_includes() {
  echo "Re-writes all the image links..."
  version
  guide_rewrite_sed $1 cdap-bi-guide 
  guide_rewrite_sed $1 cdap-flow-guide
  guide_rewrite_sed $1 cdap-flume-guide
  guide_rewrite_sed $1 cdap-kafka-ingest-guide
  guide_rewrite_sed $1 cdap-mapreduce-guide
  guide_rewrite_sed $1 cdap-spark-guide
  guide_rewrite_sed $1 cdap-timeseries-guide
  guide_rewrite_sed $1 cdap-twitter-ingest-guide
}

run_command $1

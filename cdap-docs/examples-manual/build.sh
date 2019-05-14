#!/usr/bin/env bash

# Copyright © 2014-2017 Cask Data, Inc.
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

source ../vars
source ../_common/common-build.sh

CHECK_INCLUDES=${TRUE}

function guide_rewrite_sed() {
  # Re-writes the links in the RST file to point to a local copy of any image links.
  # Looks for and downloads any image links
  echo "Re-writing using sed ${1} ${2}"
  local includes_dir=${1}
  local guide=${2}
  local project_version=${PROJECT_SHORT_VERSION}

  local source1="https://raw.githubusercontent.com/cdap-guides"
  if [ "x${GIT_BRANCH_CDAP_GUIDES}" != "x" ]; then
    local source2="${GIT_BRANCH_CDAP_GUIDES}"
  elif [ "x${GIT_BRANCH_TYPE:0:7}" == "xdevelop" ]; then
    local source2="develop"
  else
    local source2="release/cdap-${project_version}-compatible"
  fi
  local url="${source1}/${guide}/${source2}"

  local readme="README.rst"
  local readme_source="README_SOURCE.rst"
  local redirect="\.\./\.\./${TARGET}/_includes" # Target, 2 redirects, escaped

  if curl --output /dev/null --silent --head --fail "${url}/${readme}"; then
    mkdir ${includes_dir}/${guide}
    curl --silent ${url}/${readme} --output ${includes_dir}/${guide}/${readme_source}
    # Find and download any images
    local images=`grep -o ".. image:: .*" ${includes_dir}/${guide}/README_SOURCE.rst | cut -d ' ' -f 3`
    if [ "x${images}" != "x" ]; then
      for image in ${images}; do
        local image_file=`basename ${image}`
        curl --silent ${url}/${image} --output ${includes_dir}/${guide}/${image_file}
      done
    fi
    # For cdap-etl-adapter-guide, re-write links, image, and code
    if [ "${guide}" == "cdap-etl-guide" ]; then
    sed -e "s|image:: docs/images/|image:: ${redirect}/${guide}/|g" \
        -e "s|.. code:: |.. code-block:: |g" \
        -e "s|.. _\(.*\): \(.*\)|.. _\1: https://github.com/cdap-guides/${guide}/tree/${source2}/\2|g" \
        ${includes_dir}/${guide}/${readme_source} > ${includes_dir}/${guide}/${readme}
    else
      # Just rewrite image and code
      sed -e "s|image:: docs/images/|image:: ${redirect}/${guide}/|g" -e "s|.. code:: |.. code-block:: |g" \
      ${includes_dir}/${guide}/${readme_source} > ${includes_dir}/${guide}/${readme}
    fi
  else
    local m="URL does not exist: ${url}"
    echo_red_bold "${m}"
    set_message "${m}"
  fi
}

function download_includes() {
  echo_red_bold "Downloading source files includes from GitHub..."
  set_version

  echo_red_bold "Downloading files and any images and re-writing all the image links..."
  guide_rewrite_sed $1 cdap-bi-guide
  guide_rewrite_sed $1 cdap-cube-guide
  guide_rewrite_sed $1 cdap-etl-guide
  guide_rewrite_sed $1 cdap-flow-guide
  guide_rewrite_sed $1 cdap-flume-guide
  guide_rewrite_sed $1 cdap-kafka-ingest-guide
  guide_rewrite_sed $1 cdap-mapreduce-guide
  guide_rewrite_sed $1 cdap-spark-guide
  guide_rewrite_sed $1 cdap-timeseries-guide
  guide_rewrite_sed $1 cdap-twitter-ingest-guide
  guide_rewrite_sed $1 cdap-workflow-guide
}

run_command ${1}

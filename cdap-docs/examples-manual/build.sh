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

  local includes=${1}
  local includes_wise="${includes}/tutorial-wise"
  local project_version=${PROJECT_SHORT_VERSION}

  local source1="https://raw.githubusercontent.com/cdapio/cdap-apps"
  if [ "x${GIT_BRANCH_CDAP_APPS}" != "x" ]; then
    local source2="${GIT_BRANCH_CDAP_APPS}"
  elif [ "x${GIT_BRANCH_TYPE:0:7}" == "xdevelop" ]; then
    local source2="develop"
  else
    local source2="release/cdap-${project_version}-compatible"
  fi

  local project_source="${source1}/${source2}/Wise"
  local project_main=$project_source/src/main/java/io/cdap/cdap/apps/wise
  local project_test=$project_source/src/test/java/io/cdap/cdap/apps/wise
  local project_img=$project_source/docs/img

  # 1:Includes directory  2:GitHub directory 3:Java filename       4:MD5 hash of file
  download_file $includes_wise $project_main BounceCountsMapReduce.java 8e8dd188e7850e75140110243485a51a
  download_file $includes_wise $project_main BounceCountStore.java      d476c15655c6a6c6cd7fe682dea4a8b7
  download_file $includes_wise $project_main PageViewStore.java         7dc8d2fec04ce89fae4f0356db17e19d
  download_file $includes_wise $project_main WiseApp.java               23371436b588c3262fec14ec5d7aa6df
  download_file $includes_wise $project_test WiseAppTest.java           5145832dc315f4253fa6b2aac3ee9164
  download_file $includes_wise $project_main WiseFlow.java              94cb2ef13e10386d4c40c4252777d15e
  download_file $includes_wise $project_main WiseWorkflow.java          d24a138d3a96bfb41e6d166866b72291
  download_file $includes_wise $project_main WiseService.java           dccfeb2d5726a031b5aff9897ccf8257

  echo_red_bold "Downloading image files from GitHub..."
  download_file $includes_wise $project_img wise_architecture_diagram.png f01e52df149f10702d933d73935d9f29
  download_file $includes_wise $project_img wise_flow.png                 894828f13019dfbda5de43f514a8a49f

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

  echo_red_bold "Rewriting the Apps-Packs file"
  rewrite ${includes}/../../source/_includes/apps-packs.txt      ${includes}/apps-packs.txt      "<placeholder-version>" ${source2}
  echo_red_bold "Rewriting the Tutorial Index file"
  rewrite ${includes}/../../source/_includes/tutorials-index.txt ${includes}/tutorials-index.txt "<placeholder-version>" ${source2}
}

run_command ${1}

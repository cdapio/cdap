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
TUTORIAL_WISE="tutorial-wise"

function guide_rewrite_sed() {
  echo "Re-writing using sed $1 $2"
  # Re-writes the links in the RST file to point to a local copy of any image links.
  local includes_dir=$1
  local guide=$2
  local project_version=$PROJECT_SHORT_VERSION
  
  local source1="https://raw.githubusercontent.com/cdap-guides"
  local source2="release/cdap-$project_version-compatible"
  local rst_file="README.rst"
  local redirect="\.\./\.\./build/_includes" # Target, 2 redirects, escaped

  mkdir -p $includes_dir/$guide
  curl --silent $source1/$guide/$source2/$rst_file --output $includes_dir/$guide/SOURCE_$rst_file
   
  sed -e "s|image:: docs/images|image:: $redirect/$guide/docs/images|g" -e "s|.. code:: |.. code-block:: |g" $includes_dir/$guide/SOURCE_$rst_file > $includes_dir/$guide/$rst_file
}


function download_guide_image_file() {
  local includes_dir=$1
  local guide=$2
  local image=$3
  local md5_hash=$4
  local docs_images="docs/images"
  local guide_source=https://raw.githubusercontent.com/cdap-guides
  local guide_release=release/cdap-$project_version-compatible/$docs_images
  download_file $includes_dir/$guide/$docs_images $guide_source/$guide/$guide_release $image $md5_hash  
}

function download_file() {
  # Downloads a file to the includes directory, and checks that it hasn't changed.
  # Uses md5 hashes to monitor if any files have changed.
  local includes_dir=$1
  local source_dir=$2
  local file_name=$3
  local md5_hash=$4
  local target=$includes_dir/$file_name
  
  if [ ! -d "$includes_dir" ]; then
    mkdir -p $includes_dir
    echo "Creating Includes Directory: $includes_dir"
  fi

  echo "Downloading using curl $file_name"
#   echo "Downloading using curl from $source_dir/$file_name"
#   echo "  to $target"
  curl $source_dir/$file_name --output $target --silent
  
  local new_md5_hash
  if [[ "x$OSTYPE" == "xdarwin"* ]]; then
    new_md5_hash=`md5 -q $target`
  else
    new_md5_hash=`md5sum $target | awk '{print $1}'`
  fi
  
  if [ "x$md5_hash" != "x$new_md5_hash" ]; then
    echo -e "$WARNING MD5 Hash for $file_name has changed! Compare files and update hash!"  
    echo "Old md5_hash: $md5_hash New md5_hash: $new_md5_hash"
  fi
}

function pandoc_includes() {
  echo "Downloading source files includes from GitHub..."
  version
  local includes=$1/$TUTORIAL_WISE
  local project_version=$PROJECT_SHORT_VERSION
  local project_source=https://raw.githubusercontent.com/caskdata/cdap-apps/release/cdap-$project_version-compatible/Wise
  local project_main=$project_source/src/main/java/co/cask/cdap/apps/wise
  local project_test=$project_source/src/test/java/co/cask/cdap/apps/wise
  local project_img=$project_source/docs/img
  # 1:Includes directory 2:GitHub directory 3:Java filename   4:MD5 hash of file
  download_file $includes $project_main BounceCountsMapReduce.java f2f8d36e4049ba69b40282057accf38a
  download_file $includes $project_main BounceCountStore.java      d476c15655c6a6c6cd7fe682dea4a8b7
  download_file $includes $project_main PageViewStore.java         576d76c60b8f5fddeee916a87318d209
  download_file $includes $project_main WiseApp.java               69825da7d5f6d1852fd5d28415418a45
  download_file $includes $project_test WiseAppTest.java           7256c18cb80f59b4a9abcb5da320b337
  download_file $includes $project_main WiseFlow.java              2deba0633a0dcca14ef426929f543872
  download_file $includes $project_main WiseWorkflow.java          8fe51eed165e85d95c4f5e25953e3489
  download_file $includes $project_main WiseService.java           ed54e1e9952e4a880a9fc4216fdf7b4e

  echo "Downloading image files from GitHub..."
  download_file $includes $project_img wise_architecture_diagram.png f01e52df149f10702d933d73935d9f29
  download_file $includes $project_img wise_explore_page.png         5136132e4e3232a216c12e2fe9d1b0c4
  download_file $includes $project_img wise_flow.png                 4a79853f2b5a0ac45929d0966f7cd7f5
  download_file $includes $project_img wise_store_page.png           15bcd8dac10ab5d1c643fff7bdecc52d

  echo "Downloading Guide image files from GitHub..."
  download_guide_image_file $1 cdap-bi-guide             app-design.png           e950be372236d429cdfa029fb0ab8c29
  download_guide_image_file $1 cdap-bi-guide             edit-csv-input-file.png  171683816c803dc16263483aeb95f946
  download_guide_image_file $1 cdap-bi-guide             preview-data.png         cb5f61b742a9b12ebf28570807c15455
  download_guide_image_file $1 cdap-flow-guide           app-design.png           b6165a0f41ee365c34bafaeabc559ea6
  download_guide_image_file $1 cdap-flume-guide          app-design.png           cbbcce3eb5d129a0224fcff0d0fcdb12
  download_guide_image_file $1 cdap-kafka-ingest-guide   app-design.png           236ac98a8383b05b3b09b8e87299b261
  download_guide_image_file $1 cdap-mapreduce-guide      app-design.png           1e775d088d57333eaf901bce62cd2797
  download_guide_image_file $1 cdap-spark-guide          app-design.png           e343948ce9f0362b8eeb3dc3b10a7689
  download_guide_image_file $1 cdap-timeseries-guide     app-design.png           e2137c6bc5a9974d1bde7cfe68aff5aa
  download_guide_image_file $1 cdap-twitter-ingest-guide app-design.png           29de92e79a241f2659d29553b861e547

  echo "Downloading Guides READMEs from GitHub...and rewriting using sed"
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

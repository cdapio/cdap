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

CHECK_INCLUDES=${TRUE}
TUTORIAL_WISE="tutorial-wise"

function guide_rewrite_sed() {
  # Re-writes the links in the RST file to point to a local copy of any image links.
  # Looks for and downloads any image links
  echo "Re-writing using sed ${1} ${2}"
  local includes_dir=${1}
  local guide=${2}
  local project_version=${PROJECT_SHORT_VERSION}
  
  local source1="https://raw.githubusercontent.com/cdap-guides"
  if [ "x${GIT_BRANCH_TYPE:0:7}" == "xdevelop" ]; then
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
    # Rewrite image and code 
    sed -e "s|image:: docs/images/|image:: ${redirect}/${guide}/|g" -e "s|.. code:: |.. code-block:: |g" ${includes_dir}/${guide}/${readme_source} > ${includes_dir}/${guide}/${readme}
  else
    local m="URL does not exist: {$url}"
    echo_red_bold "${m}"
    set_message "${m}"
  fi  
}

function download_file() {
  # Downloads a file to the includes directory, and checks that it hasn't changed.
  # Uses md5 hashes to monitor if any files have changed.
  local includes_dir=${1}
  local source_dir=${2}
  local file_name=${3}
  local md5_hash=${4}
  local target=${includes_dir}/${file_name}
  
  if [ ! -d "${includes_dir}" ]; then
    mkdir ${includes_dir}
    echo "Creating Includes Directory: ${includes_dir}"
  fi

  echo "Downloading using curl ${file_name}"
  echo "from ${source_dir}"
  curl --silent ${source_dir}/${file_name} --output ${target}
  test_an_include ${md5_hash} ${target}
}

function download_includes() { 
  echo_red_bold "Downloading source files includes from GitHub..."
  set_version
  
  local includes=${1}/${TUTORIAL_WISE}
  local project_version=${PROJECT_SHORT_VERSION}

  local source1="https://raw.githubusercontent.com/caskdata/cdap-apps"
  if [ "x${GIT_BRANCH_TYPE:0:7}" == "xdevelop" ]; then
    local source2="develop"
  else
    local source2="release/cdap-${project_version}-compatible"
  fi

  local project_source="${source1}/${source2}/Wise"
  local project_main=$project_source/src/main/java/co/cask/cdap/apps/wise
  local project_test=$project_source/src/test/java/co/cask/cdap/apps/wise
  local project_img=$project_source/docs/img

  # 1:Includes directory 2:GitHub directory 3:Java filename   4:MD5 hash of file
  download_file $includes $project_main BounceCountsMapReduce.java 2f7baace8dd452825e8d47bb54b5e11e
  download_file $includes $project_main BounceCountStore.java      d476c15655c6a6c6cd7fe682dea4a8b7
  download_file $includes $project_main PageViewStore.java         7dc8d2fec04ce89fae4f0356db17e19d
  download_file $includes $project_main WiseApp.java               23371436b588c3262fec14ec5d7aa6df
  download_file $includes $project_test WiseAppTest.java           5145832dc315f4253fa6b2aac3ee9164
  download_file $includes $project_main WiseFlow.java              94cb2ef13e10386d4c40c4252777d15e
  download_file $includes $project_main WiseWorkflow.java          d24a138d3a96bfb41e6d166866b72291
  download_file $includes $project_main WiseService.java           dccfeb2d5726a031b5aff9897ccf8257

  echo_red_bold "Downloading image files from GitHub..."
  download_file $includes $project_img wise_architecture_diagram.png f01e52df149f10702d933d73935d9f29
  download_file $includes $project_img wise_explore_page.png         5136132e4e3232a216c12e2fe9d1b0c4
  download_file $includes $project_img wise_flow.png                 4a79853f2b5a0ac45929d0966f7cd7f5
  download_file $includes $project_img wise_store_page.png           15bcd8dac10ab5d1c643fff7bdecc52d

  echo_red_bold "Downloading files and any images and re-writing all the image links..."
  guide_rewrite_sed $1 cdap-bi-guide 
  guide_rewrite_sed $1 cdap-cube-guide 
  guide_rewrite_sed $1 cdap-etl-adapter-guide 
  guide_rewrite_sed $1 cdap-flow-guide
  guide_rewrite_sed $1 cdap-flume-guide
  guide_rewrite_sed $1 cdap-kafka-ingest-guide
  guide_rewrite_sed $1 cdap-mapreduce-guide
  guide_rewrite_sed $1 cdap-spark-guide
  guide_rewrite_sed $1 cdap-timeseries-guide
  guide_rewrite_sed $1 cdap-twitter-ingest-guide
  guide_rewrite_sed $1 cdap-workflow-guide
  
  echo_red_bold "Checking included example files for changes"
  
  # Group alphabetically each example separately, files from each example together
  
  test_an_include 55738256b6c668914e0dde5c0ec44bd5 ../../cdap-examples/CountRandom/src/main/java/co/cask/cdap/examples/countrandom/CountRandom.java
  test_an_include 9dcf41f03ee52e42b77588fe18f542d5 ../../cdap-examples/CountRandom/src/main/java/co/cask/cdap/examples/countrandom/CountRandomFlow.java
  test_an_include 288c590e1a9b010e1cd7e29a431e9071 ../../cdap-examples/CountRandom/src/main/java/co/cask/cdap/examples/countrandom/RandomSource.java
  test_an_include 77d244f968d508d9ea2d91e463065b68 ../../cdap-examples/CountRandom/src/main/java/co/cask/cdap/examples/countrandom/NumberSplitter.java
  test_an_include 9f963a17090976d2c15a4d092bd9e8de ../../cdap-examples/CountRandom/src/main/java/co/cask/cdap/examples/countrandom/NumberCounter.java
  
  test_an_include 16fabc25230c648f6aa157121145734f ../../cdap-examples/DataCleansing/src/main/java/co/cask/cdap/examples/datacleansing/DataCleansing.java
  test_an_include 7f665f50faa05e0141dc7b82d50179f5 ../../cdap-examples/DataCleansing/src/main/java/co/cask/cdap/examples/datacleansing/DataCleansingMapReduce.java

  test_an_include 8a7b4aacee88800cd82d96b07280cc64 ../../cdap-examples/FileSetExample/src/main/java/co/cask/cdap/examples/fileset/FileSetExample.java
  test_an_include 6943e3861b243c72ab212fe30bbaf5f0 ../../cdap-examples/FileSetExample/src/main/java/co/cask/cdap/examples/fileset/FileSetService.java
  test_an_include 738f54aa72df27c8a7022bd25c92e27f ../../cdap-examples/FileSetExample/src/main/java/co/cask/cdap/examples/fileset/WordCount.java
  
  test_an_include c2374d482454dc57c5944e557b544dba ../../cdap-examples/HelloWorld/src/main/java/co/cask/cdap/examples/helloworld/HelloWorld.java

  test_an_include a9a7fd53c199defff09e6e3c73e4e71f ../../cdap-examples/LogAnalysis/src/main/java/co/cask/cdap/examples/loganalysis/LogAnalysisApp.java
  
  test_an_include 38245f25cef322b873b729a1e1286235 ../../cdap-examples/Purchase/src/main/java/co/cask/cdap/examples/purchase/PurchaseApp.java
  test_an_include 29fe1471372678115e643b0ad431b28d ../../cdap-examples/Purchase/src/main/java/co/cask/cdap/examples/purchase/PurchaseStore.java
  test_an_include b53dd493c4e9c2cb89b593baa4139087 ../../cdap-examples/Purchase/src/main/java/co/cask/cdap/examples/purchase/PurchaseHistoryBuilder.java
  test_an_include 80216a08a2b3d480e4a081722408222f ../../cdap-examples/Purchase/src/main/java/co/cask/cdap/examples/purchase/PurchaseHistoryService.java

  test_an_include 950241d0199883f3bcb23d6f7da0551f ../../cdap-examples/SparkKMeans/src/main/java/co/cask/cdap/examples/sparkkmeans/SparkKMeansApp.java
  
  test_an_include 7f83b852a8e7b4594094d4e88d80a0b4 ../../cdap-examples/SparkPageRank/src/main/java/co/cask/cdap/examples/sparkpagerank/SparkPageRankApp.java

  test_an_include afe12d26b79607a846d3eaa58958ea5f ../../cdap-examples/SportResults/src/main/java/co/cask/cdap/examples/sportresults/SportResults.java
  test_an_include c488b6ad3b5708977a8a913460e4e650 ../../cdap-examples/SportResults/src/main/java/co/cask/cdap/examples/sportresults/UploadService.java
  test_an_include 5f7838093050321c558a0eae4b802c5f ../../cdap-examples/SportResults/src/main/java/co/cask/cdap/examples/sportresults/ScoreCounter.java
  
  test_an_include 41db6b1cd160fd92bf03395454414206 ../../cdap-examples/StreamConversion/src/main/java/co/cask/cdap/examples/streamconversion/StreamConversionApp.java
  test_an_include b254b6b2d34b9a39cbce0aca10d4fd52 ../../cdap-examples/StreamConversion/src/main/java/co/cask/cdap/examples/streamconversion/StreamConversionMapReduce.java
  
  test_an_include 2260781c7cef2938aa36c946f3a34341 ../../cdap-examples/UserProfiles/src/main/java/co/cask/cdap/examples/profiles/UserProfiles.java

  test_an_include 75aee2ce7b34eb125d41a295d5f3122d ../../cdap-examples/WebAnalytics/src/main/java/co/cask/cdap/examples/webanalytics/UniqueVisitor.java
  test_an_include 936d007286f0d6d59967c1b421850e37 ../../cdap-examples/WebAnalytics/src/main/java/co/cask/cdap/examples/webanalytics/UniqueVisitCount.java
  test_an_include 8ca118b98daab1cd34005dce37f24d4e ../../cdap-examples/WebAnalytics/src/main/java/co/cask/cdap/examples/webanalytics/WebAnalyticsFlow.java
  
  test_an_include 2c1898253d66d4a2a149d9012aa96328 ../../cdap-examples/WikipediaPipeline/src/main/java/co/cask/cdap/examples/wikipedia/WikipediaPipelineApp.java
  
  test_an_include 031d8480890e015158c5281945735d9f ../../cdap-examples/WordCount/src/main/java/co/cask/cdap/examples/wordcount/WordCount.java
}

run_command ${1}

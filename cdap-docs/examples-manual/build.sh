#!/usr/bin/env bash

# Copyright © 2014-2016 Cask Data, Inc.
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
  if [ "x${GIT_BRANCH_CDAP_APPS}" != "x" ]; then
    local source2="${GIT_BRANCH_CDAP_APPS}"
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
    local m="URL does not exist: {$url}"
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

  local source1="https://raw.githubusercontent.com/caskdata/cdap-apps"
  if [ "x${GIT_BRANCH_CDAP_APPS}" != "x" ]; then
    local source2="${GIT_BRANCH_CDAP_APPS}"
  elif [ "x${GIT_BRANCH_TYPE:0:7}" == "xdevelop" ]; then
    local source2="develop"
  else
    local source2="release/cdap-${project_version}-compatible"
  fi

  local project_source="${source1}/${source2}/Wise"
  local project_main=$project_source/src/main/java/co/cask/cdap/apps/wise
  local project_test=$project_source/src/test/java/co/cask/cdap/apps/wise
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
  
  echo_red_bold "Checking included example files for changes"
  
  # Group alphabetically each example separately, files from each example together

  test_an_include 0d1fdc6d75995c3c417886786ccc8997 ../../cdap-examples/ClicksAndViews/src/main/java/co/cask/cdap/examples/clicksandviews/ClicksAndViews.java
  test_an_include 6b6c72d2230e1f64e2827a49d92b9146 ../../cdap-examples/ClicksAndViews/src/main/java/co/cask/cdap/examples/clicksandviews/ClicksAndViewsMapReduce.java

  test_an_include 45cbe451f2821eddf2f1245eff468cee ../../cdap-examples/CountRandom/src/main/java/co/cask/cdap/examples/countrandom/CountRandom.java
  test_an_include 9dcf41f03ee52e42b77588fe18f542d5 ../../cdap-examples/CountRandom/src/main/java/co/cask/cdap/examples/countrandom/CountRandomFlow.java
  test_an_include 288c590e1a9b010e1cd7e29a431e9071 ../../cdap-examples/CountRandom/src/main/java/co/cask/cdap/examples/countrandom/RandomSource.java
  test_an_include 77d244f968d508d9ea2d91e463065b68 ../../cdap-examples/CountRandom/src/main/java/co/cask/cdap/examples/countrandom/NumberSplitter.java
  test_an_include 9f963a17090976d2c15a4d092bd9e8de ../../cdap-examples/CountRandom/src/main/java/co/cask/cdap/examples/countrandom/NumberCounter.java

  test_an_include fa746b8f3423736310316f2d48b0fe7d ../../cdap-examples/DataCleansing/src/main/java/co/cask/cdap/examples/datacleansing/DataCleansing.java
  test_an_include cc6989fe673cd88545d73cdaffad1bb1 ../../cdap-examples/DataCleansing/src/main/java/co/cask/cdap/examples/datacleansing/DataCleansingMapReduce.java

  test_an_include fe679b53a1dc757a15fd3fcafc2046fd ../../cdap-examples/FileSetExample/src/main/java/co/cask/cdap/examples/fileset/FileSetExample.java
  test_an_include 9e137848822e63101b699af03af7f45e ../../cdap-examples/FileSetExample/src/main/java/co/cask/cdap/examples/fileset/FileSetService.java
  test_an_include 4107ad528a3e9ca569a0cdee43b189c5 ../../cdap-examples/FileSetExample/src/main/java/co/cask/cdap/examples/fileset/WordCount.java
  
  test_an_include 4255a2e3d417e928ac5182ddd932139f ../../cdap-examples/HelloWorld/src/main/java/co/cask/cdap/examples/helloworld/HelloWorld.java

  test_an_include 4626aaec1bbc5bdbfd1de28cc721ec76 ../../cdap-examples/LogAnalysis/src/main/java/co/cask/cdap/examples/loganalysis/LogAnalysisApp.java

  test_an_include f90e4e4ce08851f45bdbe2563efb51dd ../../cdap-examples/Purchase/src/main/java/co/cask/cdap/examples/purchase/PurchaseApp.java
  test_an_include 29fe1471372678115e643b0ad431b28d ../../cdap-examples/Purchase/src/main/java/co/cask/cdap/examples/purchase/PurchaseStore.java
  test_an_include bb7344dd2c55c5ef70653ff0b5fcd6df ../../cdap-examples/Purchase/src/main/java/co/cask/cdap/examples/purchase/PurchaseHistoryBuilder.java
  test_an_include 80216a08a2b3d480e4a081722408222f ../../cdap-examples/Purchase/src/main/java/co/cask/cdap/examples/purchase/PurchaseHistoryService.java

  test_an_include 04abc21d3a3423cecc3b9c9619aa960d ../../cdap-examples/SpamClassifier/src/main/java/co/cask/cdap/examples/sparkstreaming/SpamClassifier.java

  test_an_include 3f25e035b2de8bd2d127733df1c58ff1 ../../cdap-examples/SparkKMeans/src/main/java/co/cask/cdap/examples/sparkkmeans/SparkKMeansApp.java
  
  test_an_include f9acd1e2ed73ba5e83b9a0cd6c75f988 ../../cdap-examples/SparkPageRank/src/main/java/co/cask/cdap/examples/sparkpagerank/SparkPageRankApp.java

  test_an_include 1dff2aa10d09f384965e343be7f83f76 ../../cdap-examples/SportResults/src/main/java/co/cask/cdap/examples/sportresults/SportResults.java
  test_an_include c488b6ad3b5708977a8a913460e4e650 ../../cdap-examples/SportResults/src/main/java/co/cask/cdap/examples/sportresults/UploadService.java
  test_an_include 6e0ed4027acafd163e8b891a1045717f ../../cdap-examples/SportResults/src/main/java/co/cask/cdap/examples/sportresults/ScoreCounter.java
  
  test_an_include 6e407b943f7d6c92877023628dceef96 ../../cdap-examples/StreamConversion/src/main/java/co/cask/cdap/examples/streamconversion/StreamConversionApp.java
  test_an_include 2ec634340cca4f453cfc05bb4c064130 ../../cdap-examples/StreamConversion/src/main/java/co/cask/cdap/examples/streamconversion/StreamConversionMapReduce.java

  test_an_include 58fe002977ee1a5613f465cc784d769d ../../cdap-examples/UserProfiles/src/main/java/co/cask/cdap/examples/profiles/UserProfiles.java

  test_an_include 75aee2ce7b34eb125d41a295d5f3122d ../../cdap-examples/WebAnalytics/src/main/java/co/cask/cdap/examples/webanalytics/UniqueVisitor.java
  test_an_include cec2fd083dabf4da2b178559653e0992 ../../cdap-examples/WebAnalytics/src/main/java/co/cask/cdap/examples/webanalytics/UniqueVisitCount.java
  test_an_include 8ca118b98daab1cd34005dce37f24d4e ../../cdap-examples/WebAnalytics/src/main/java/co/cask/cdap/examples/webanalytics/WebAnalyticsFlow.java
  
  test_an_include 38789a70a89c188443f7cfd05b2ea0db ../../cdap-examples/WikipediaPipeline/src/main/java/co/cask/cdap/examples/wikipedia/WikipediaPipelineApp.java
  
  test_an_include 5154138de03f9fab4311858225f18dca ../../cdap-examples/WordCount/src/main/java/co/cask/cdap/examples/wordcount/WordCount.java

  echo_red_bold "Rewriting the Apps-Packs file"
  rewrite ${includes}/../../source/_includes/apps-packs.txt      ${includes}/apps-packs.txt      "<placeholder-version>" ${source2}
  echo_red_bold "Rewriting the Tutorial Index file"
  rewrite ${includes}/../../source/_includes/tutorials-index.txt ${includes}/tutorials-index.txt "<placeholder-version>" ${source2}
}

run_command ${1}

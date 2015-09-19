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

source ../_common/common-build.sh

CHECK_INCLUDES=${TRUE}

function download_includes() {
  echo_red_bold "Checking guarded files for changes"

  # ETL Plugins

  # Batchsinks
  test_an_include 033951c7c752d4e5432038eb964af4d6 ../../cdap-app-templates/cdap-etl/cdap-etl-lib/src/main/java/co/cask/cdap/etl/batch/sink/BatchCubeSink.java
  test_an_include 99954f84668085177323a2207ac458b2 ../../cdap-app-templates/cdap-etl/cdap-etl-lib/src/main/java/co/cask/cdap/etl/batch/sink/BatchElasticsearchSink.java
  test_an_include 80424ab3bd082196130475c006b7cca0 ../../cdap-app-templates/cdap-etl/cdap-etl-lib/src/main/java/co/cask/cdap/etl/batch/sink/BatchWritableSink.java
  test_an_include c99bce3da0d3fc9bcd5320dee4d18bf9 ../../cdap-app-templates/cdap-etl/cdap-etl-lib/src/main/java/co/cask/cdap/etl/batch/sink/DBSink.java
  test_an_include 9bfd3e359b55b336ba1758ab61bf449f ../../cdap-app-templates/cdap-etl/cdap-etl-lib/src/main/java/co/cask/cdap/etl/batch/sink/FileBatchSink.java
  test_an_include 2746d991cfad8271977c3f648464acd2 ../../cdap-app-templates/cdap-etl/cdap-etl-lib/src/main/java/co/cask/cdap/etl/batch/sink/KVTableSink.java
  test_an_include 713b171856a5963278e6de6e639f6176 ../../cdap-app-templates/cdap-etl/cdap-etl-lib/src/main/java/co/cask/cdap/etl/batch/sink/SnapshotFileBatchAvroSink.java
  test_an_include d803b3fa4d4711859e3b8737941b9b26 ../../cdap-app-templates/cdap-etl/cdap-etl-lib/src/main/java/co/cask/cdap/etl/batch/sink/SnapshotFileBatchParquetSink.java
  test_an_include 28d604889dadd50870faadc3bac95ffb ../../cdap-app-templates/cdap-etl/cdap-etl-lib/src/main/java/co/cask/cdap/etl/batch/sink/SnapshotFileBatchSink.java
  test_an_include 8e51b83878c90f000e59ada65630457d ../../cdap-app-templates/cdap-etl/cdap-etl-lib/src/main/java/co/cask/cdap/etl/batch/sink/TableSink.java
  test_an_include 2417871dcd685d2ed75c10df712c9f8c ../../cdap-app-templates/cdap-etl/cdap-etl-lib/src/main/java/co/cask/cdap/etl/batch/sink/TimePartitionedFileSetDatasetAvroSink.java
  test_an_include 95ae9c116aa257efd5e3bb6cacaa4033 ../../cdap-app-templates/cdap-etl/cdap-etl-lib/src/main/java/co/cask/cdap/etl/batch/sink/TimePartitionedFileSetDatasetParquetSink.java
  test_an_include 3069701c1070f0546d6a73800f558e72 ../../cdap-app-templates/cdap-etl/cdap-etl-lib/src/main/java/co/cask/cdap/etl/batch/sink/TimePartitionedFileSetSink.java

  # Batchsources

  # Realtimesinks

  # Realtimesources
  
  # Shared-Plugins
  test_an_include 4fc697d071e894cfce67dbf62c9709d0 ../../cdap-app-templates/cdap-etl/cdap-etl-lib/src/main/java/co/cask/cdap/etl/validator/CoreValidator.java

  # Transforms
  
  # Commmon
  test_an_include 4fc697d071e894cfce67dbf62c9709d0 ../../cdap-app-templates/cdap-etl/cdap-etl-lib/src/main/java/co/cask/cdap/etl/common/DBConfig.java
  
  
}

run_command ${1}

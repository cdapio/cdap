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
  test_an_include f8191552bd02e0b90e668f1079377291 ../../cdap-app-templates/cdap-etl/cdap-etl-lib/src/main/java/co/cask/cdap/etl/batch/sink/BatchElasticsearchSink.java
  test_an_include c99bce3da0d3fc9bcd5320dee4d18bf9 ../../cdap-app-templates/cdap-etl/cdap-etl-lib/src/main/java/co/cask/cdap/etl/batch/sink/DBSink.java
  test_an_include 2746d991cfad8271977c3f648464acd2 ../../cdap-app-templates/cdap-etl/cdap-etl-lib/src/main/java/co/cask/cdap/etl/batch/sink/KVTableSink.java
  test_an_include 713b171856a5963278e6de6e639f6176 ../../cdap-app-templates/cdap-etl/cdap-etl-lib/src/main/java/co/cask/cdap/etl/batch/sink/SnapshotFileBatchAvroSink.java
  test_an_include d803b3fa4d4711859e3b8737941b9b26 ../../cdap-app-templates/cdap-etl/cdap-etl-lib/src/main/java/co/cask/cdap/etl/batch/sink/SnapshotFileBatchParquetSink.java
  test_an_include 8e51b83878c90f000e59ada65630457d ../../cdap-app-templates/cdap-etl/cdap-etl-lib/src/main/java/co/cask/cdap/etl/batch/sink/TableSink.java
  test_an_include 2417871dcd685d2ed75c10df712c9f8c ../../cdap-app-templates/cdap-etl/cdap-etl-lib/src/main/java/co/cask/cdap/etl/batch/sink/TimePartitionedFileSetDatasetAvroSink.java
  test_an_include 95ae9c116aa257efd5e3bb6cacaa4033 ../../cdap-app-templates/cdap-etl/cdap-etl-lib/src/main/java/co/cask/cdap/etl/batch/sink/TimePartitionedFileSetDatasetParquetSink.java

  # Batchsources
  test_an_include 67ee7fbac8e3971bd37e99186890097d ../../cdap-app-templates/cdap-etl/cdap-etl-lib/src/main/java/co/cask/cdap/etl/batch/source/DBSource.java
  test_an_include fd0399dc86a461466d119dc1e1cbe2f4 ../../cdap-app-templates/cdap-etl/cdap-etl-lib/src/main/java/co/cask/cdap/etl/batch/source/ElasticsearchSource.java
  test_an_include f85320e0da353c790434a770aba6d040 ../../cdap-app-templates/cdap-etl/cdap-etl-lib/src/main/java/co/cask/cdap/etl/batch/source/FileBatchSource.java
  test_an_include a3e5de66820f096813a6d58fdba86b52 ../../cdap-app-templates/cdap-etl/cdap-etl-lib/src/main/java/co/cask/cdap/etl/batch/source/KVTableSource.java
  test_an_include 9d5e7d1bc55730b5cb00180dda09adfe ../../cdap-app-templates/cdap-etl/cdap-etl-lib/src/main/java/co/cask/cdap/etl/batch/source/S3BatchSource.java
  test_an_include 1ad8f9d4d9a27f10cdc0304bddf84c2b ../../cdap-app-templates/cdap-etl/cdap-etl-lib/src/main/java/co/cask/cdap/etl/batch/source/StreamBatchSource.java
  test_an_include f8c07741ac3b09da7554b91351e7973b ../../cdap-app-templates/cdap-etl/cdap-etl-lib/src/main/java/co/cask/cdap/etl/batch/source/TableSource.java
  test_an_include 4bf9f88fac9ff72fc25642db21a89620 ../../cdap-app-templates/cdap-etl/cdap-etl-lib/src/main/java/co/cask/cdap/etl/batch/source/TimePartitionedFileSetDatasetAvroSource.java

  # Realtimesinks
  test_an_include 974ed9256c135872915a3f9061c5fc5c ../../cdap-app-templates/cdap-etl/cdap-etl-lib/src/main/java/co/cask/cdap/etl/realtime/sink/RealtimeCubeSink.java
  test_an_include a966a7f1edfce6465f04e6d82e1b9d16 ../../cdap-app-templates/cdap-etl/cdap-etl-lib/src/main/java/co/cask/cdap/etl/realtime/sink/RealtimeElasticsearchSink.java
  test_an_include 72438554aeb5f590ff356c5fdd5569b4 ../../cdap-app-templates/cdap-etl/cdap-etl-lib/src/main/java/co/cask/cdap/etl/realtime/sink/RealtimeTableSink.java
  test_an_include ff03b40c1dfdc25d75dc6d3547d7293b ../../cdap-app-templates/cdap-etl/cdap-etl-lib/src/main/java/co/cask/cdap/etl/realtime/sink/StreamSink.java

  # Realtimesources
  test_an_include 4c156d69fcc4d7b9145caab2dbed26ff ../../cdap-app-templates/cdap-etl/cdap-etl-lib/src/main/java/co/cask/cdap/etl/realtime/source/DataGeneratorSource.java
  test_an_include f02c834c73c738659cf92587f5c7c1b7 ../../cdap-app-templates/cdap-etl/cdap-etl-lib/src/main/java/co/cask/cdap/etl/realtime/source/JmsSource.java
  test_an_include 263ea7824545d65562b1f8e805e1272a ../../cdap-app-templates/cdap-etl/cdap-etl-lib/src/main/java/co/cask/cdap/etl/realtime/source/KafkaSource.java
  test_an_include 496fb353aa835ef2a60f2a76b56fdc01 ../../cdap-app-templates/cdap-etl/cdap-etl-lib/src/main/java/co/cask/cdap/etl/realtime/source/SqsSource.java
  test_an_include e8b987b6f648211ed183e18c68b41873 ../../cdap-app-templates/cdap-etl/cdap-etl-lib/src/main/java/co/cask/cdap/etl/realtime/source/TwitterSource.java

  # Transforms
  test_an_include 06ddd340ba65bbc068ab3e3cf2f346c1 ../../cdap-app-templates/cdap-etl/cdap-etl-lib/src/main/java/co/cask/cdap/etl/transform/LogParserTransform.java
  test_an_include 7b5386499cc1a646e5be38ab5269d076 ../../cdap-app-templates/cdap-etl/cdap-etl-lib/src/main/java/co/cask/cdap/etl/transform/ProjectionTransform.java
  test_an_include 100c530448d9719463f5bf9445321cca ../../cdap-app-templates/cdap-etl/cdap-etl-lib/src/main/java/co/cask/cdap/etl/transform/ScriptFilterTransform.java
  test_an_include bcbf3340f2cd9345f155dd76d30b6e78 ../../cdap-app-templates/cdap-etl/cdap-etl-lib/src/main/java/co/cask/cdap/etl/transform/ScriptTransform.java
  test_an_include c3eb291d7b7d4ca0934d151bed882dd3 ../../cdap-app-templates/cdap-etl/cdap-etl-lib/src/main/java/co/cask/cdap/etl/transform/StructuredRecordToGenericRecordTransform.java
  test_an_include 89654c4e1e8a30c28797e54c469cdebd ../../cdap-app-templates/cdap-etl/cdap-etl-lib/src/main/java/co/cask/cdap/etl/transform/ValidatorTransform.java

  # Shared-Plugins
  test_an_include 4fc697d071e894cfce67dbf62c9709d0 ../../cdap-app-templates/cdap-etl/cdap-etl-lib/src/main/java/co/cask/cdap/etl/validator/CoreValidator.java
}

run_command ${1}

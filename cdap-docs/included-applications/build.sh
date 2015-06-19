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
  local cdap_etl='../../cdap-app-templates/cdap-etl/cdap-etl-lib/src/main/java/co/cask/cdap/etl'

  # Batchsinks
  test_an_include 033951c7c752d4e5432038eb964af4d6 "${cdap_etl}/batch/sink/BatchCubeSink.java"
  test_an_include 2f530a1f6681762cf506a5ede9275171 "${cdap_etl}/batch/sink/DBSink.java"
  test_an_include 2746d991cfad8271977c3f648464acd2 "${cdap_etl}/batch/sink/KVTableSink.java"
  test_an_include 68c51d9aa208014e07c7fdbccc0a9edd "${cdap_etl}/batch/sink/S3AvroBatchSink.java"
  test_an_include 91cd228bfa26459e5f0d8bee7e8876d3 "${cdap_etl}/batch/sink/S3BatchSink.java"
  test_an_include 33c1d28212373d57e40d9079217af132 "${cdap_etl}/batch/sink/S3ParquetBatchSink.java"
  test_an_include 713b171856a5963278e6de6e639f6176 "${cdap_etl}/batch/sink/SnapshotFileBatchAvroSink.java"
  test_an_include d803b3fa4d4711859e3b8737941b9b26 "${cdap_etl}/batch/sink/SnapshotFileBatchParquetSink.java"
  test_an_include 9b6fc03c81c889035bc1bc99a8ade998 "${cdap_etl}/batch/sink/SnapshotFileBatchSink.java"
  test_an_include 8e51b83878c90f000e59ada65630457d "${cdap_etl}/batch/sink/TableSink.java"
  test_an_include 2417871dcd685d2ed75c10df712c9f8c "${cdap_etl}/batch/sink/TimePartitionedFileSetDatasetAvroSink.java"
  test_an_include 95ae9c116aa257efd5e3bb6cacaa4033 "${cdap_etl}/batch/sink/TimePartitionedFileSetDatasetParquetSink.java"

  # Batchsources
  test_an_include 014b7cb22cacc1fe1066ea14ed32b945 "${cdap_etl}/batch/source/DBSource.java"
  test_an_include f85320e0da353c790434a770aba6d040 "${cdap_etl}/batch/source/FileBatchSource.java"
  test_an_include a3e5de66820f096813a6d58fdba86b52 "${cdap_etl}/batch/source/KVTableSource.java"
  test_an_include 551c7f46347de3ea3bbc3ec45337487b "${cdap_etl}/batch/source/S3BatchSource.java"
  test_an_include 1ad8f9d4d9a27f10cdc0304bddf84c2b "${cdap_etl}/batch/source/StreamBatchSource.java"
  test_an_include f8c07741ac3b09da7554b91351e7973b "${cdap_etl}/batch/source/TableSource.java"
  test_an_include 4bf9f88fac9ff72fc25642db21a89620 "${cdap_etl}/batch/source/TimePartitionedFileSetDatasetAvroSource.java"

  # Realtimesinks
  test_an_include 974ed9256c135872915a3f9061c5fc5c "${cdap_etl}/realtime/sink/RealtimeCubeSink.java"
  test_an_include 72438554aeb5f590ff356c5fdd5569b4 "${cdap_etl}/realtime/sink/RealtimeTableSink.java"
  test_an_include ff03b40c1dfdc25d75dc6d3547d7293b "${cdap_etl}/realtime/sink/StreamSink.java"

  # Realtimesources
  test_an_include 4c156d69fcc4d7b9145caab2dbed26ff "${cdap_etl}/realtime/source/DataGeneratorSource.java"
  test_an_include c1d134466622468c36eaaf6a55677bb1 "${cdap_etl}/realtime/source/JmsSource.java"
  test_an_include c0605df8382ea941966785e5ad589c4a "${cdap_etl}/realtime/source/KafkaSource.java"
  test_an_include 496fb353aa835ef2a60f2a76b56fdc01 "${cdap_etl}/realtime/source/SqsSource.java"
  test_an_include e8b987b6f648211ed183e18c68b41873 "${cdap_etl}/realtime/source/TwitterSource.java"

  # Transforms
  test_an_include 06ddd340ba65bbc068ab3e3cf2f346c1 "${cdap_etl}/transform/LogParserTransform.java"
  test_an_include 7b5386499cc1a646e5be38ab5269d076 "${cdap_etl}/transform/ProjectionTransform.java"
  test_an_include 100c530448d9719463f5bf9445321cca "${cdap_etl}/transform/ScriptFilterTransform.java"
  test_an_include bcbf3340f2cd9345f155dd76d30b6e78 "${cdap_etl}/transform/ScriptTransform.java"
  test_an_include c3eb291d7b7d4ca0934d151bed882dd3 "${cdap_etl}/transform/StructuredRecordToGenericRecordTransform.java"
  test_an_include 89654c4e1e8a30c28797e54c469cdebd "${cdap_etl}/transform/ValidatorTransform.java"

  # Shared-Plugins
  test_an_include 4fc697d071e894cfce67dbf62c9709d0 "${cdap_etl}/validator/CoreValidator.java"
}

run_command ${1}

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
  test_an_include 7a43b2678e3340696a0de25233d48d2d "${cdap_etl}/batch/sink/DBSink.java"
  test_an_include 2746d991cfad8271977c3f648464acd2 "${cdap_etl}/batch/sink/KVTableSink.java"
  test_an_include 9f8483db2b24a8f6ab04c411e111e21a "${cdap_etl}/batch/sink/S3AvroBatchSink.java"
  test_an_include d13f0491f034a43cd9de4be7d3550e1e "${cdap_etl}/batch/sink/S3BatchSink.java"
  test_an_include 578f0e9344f71d4f6bb1ed198eb9331d "${cdap_etl}/batch/sink/S3ParquetBatchSink.java"
  test_an_include c48f5570829cd49f4518afe2602007d4 "${cdap_etl}/batch/sink/SnapshotFileBatchAvroSink.java"
  test_an_include 0c22ee5c57de83614d8235096e8efa8d "${cdap_etl}/batch/sink/SnapshotFileBatchParquetSink.java"
  test_an_include 288b88b520b32f16fb0d0dafab5c52be "${cdap_etl}/batch/sink/SnapshotFileBatchSink.java"
  test_an_include 8e51b83878c90f000e59ada65630457d "${cdap_etl}/batch/sink/TableSink.java"
  test_an_include ea3eb9208a4bfeaead70ea689fba5cae "${cdap_etl}/batch/sink/TimePartitionedFileSetDatasetAvroSink.java"
  test_an_include 95ae9c116aa257efd5e3bb6cacaa4033 "${cdap_etl}/batch/sink/TimePartitionedFileSetDatasetParquetSink.java"
  test_an_include 3069701c1070f0546d6a73800f558e72 "${cdap_etl}/batch/sink/TimePartitionedFileSetSink.java"

  # Batchsources
  test_an_include 794758bc0afbc404fa33143177afbb81 "${cdap_etl}/batch/source/DBSource.java"
  test_an_include adc1f5035473b6e544f1878da8668f3e "${cdap_etl}/batch/source/FileBatchSource.java"
  test_an_include a3e5de66820f096813a6d58fdba86b52 "${cdap_etl}/batch/source/KVTableSource.java"
  test_an_include 0214c7788410ba6da78e5172c2c061e0 "${cdap_etl}/batch/source/S3BatchSource.java"
  test_an_include 2af8f3fbf95a9d5f7becd9a7cbfaf6f9 "${cdap_etl}/batch/source/SnapshotFileBatchAvroSource.java"
  test_an_include 91525581c2d6c657c89eb6fac7b5d470 "${cdap_etl}/batch/source/SnapshotFileBatchParquetSource.java"
  test_an_include 1ad8f9d4d9a27f10cdc0304bddf84c2b "${cdap_etl}/batch/source/StreamBatchSource.java"
  test_an_include f8c07741ac3b09da7554b91351e7973b "${cdap_etl}/batch/source/TableSource.java"
  test_an_include 7b14245a1938f1eb187f43e3ad0e63cb "${cdap_etl}/batch/source/TimePartitionedFileSetDatasetAvroSource.java"
  test_an_include e6b5844d4048eaecba34ab0399be1075 "${cdap_etl}/batch/source/TimePartitionedFileSetDatasetParquetSource.java"
  test_an_include d4964d588df295600587fd7e654d75db "${cdap_etl}/batch/source/TimePartitionedFileSetSource.java"

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
  test_an_include 50f4c36bf7a0fcb5acf0fc72df049afb "${cdap_etl}/transform/ScriptFilterTransform.java"
  test_an_include 91b2b4ae0e5a4e58a411b387e88d1c4e "${cdap_etl}/transform/ScriptTransform.java"
  test_an_include c3eb291d7b7d4ca0934d151bed882dd3 "${cdap_etl}/transform/StructuredRecordToGenericRecordTransform.java"
  test_an_include c2d115c4993b10f1a787c789665626d4 "${cdap_etl}/transform/ValidatorTransform.java"

  # Shared-Plugins
  test_an_include 4fc697d071e894cfce67dbf62c9709d0 "${cdap_etl}/validator/CoreValidator.java"
}

run_command ${1}

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

function download_docs_file() {
  # Downloads a docs file to the includes directory
  # https://raw.githubusercontent.com/caskdata/hydrator-plugins/develop/cassandra-plugins/docs/Cassandra-batchsink.md
  # download_docs_file target_includes_dir source_base_url source_dir file_name
  local target_includes_dir="${1}/${3}"
  local source_dir="${3}"
  local source_url="${2}/${3}/docs/${4}"
  local file_name=${4}
  local target=${target_includes_dir}/${file_name}
  
  if [ ! -d "${target_includes_dir}" ]; then
    mkdir -p ${target_includes_dir}
    echo "Creating Includes Directory: ${target_includes_dir}"
  fi

  if curl --output /dev/null --silent --head --fail "${source_url}"; then
    echo "Downloading using curl ${file_name}"
    echo "from ${source_dir}"
    curl --silent ${source_url} --output ${target}
  else
    echo_red_bold "URL does not exist: ${source_url}"
  fi   
}

function download_includes() {
#   echo_red_bold "Checking guarded files for changes"
# 
#   # ETL Plugins
#   local cdap_etl='../../cdap-app-templates/cdap-etl/cdap-etl-lib/src/main/java/co/cask/cdap/etl'
# 
#   # Batchsinks
#   test_an_include 033951c7c752d4e5432038eb964af4d6 "${cdap_etl}/batch/sink/BatchCubeSink.java"
#   test_an_include 2f530a1f6681762cf506a5ede9275171 "${cdap_etl}/batch/sink/DBSink.java"
#   test_an_include 2746d991cfad8271977c3f648464acd2 "${cdap_etl}/batch/sink/KVTableSink.java"
#   test_an_include 93c409eb8069daa76693638cb34388ce "${cdap_etl}/batch/sink/S3AvroBatchSink.java"
#   test_an_include e9827888762f1d583e905d49619c1fe6 "${cdap_etl}/batch/sink/S3BatchSink.java"
#   test_an_include 58f13539632d13993069b6caeff15c03 "${cdap_etl}/batch/sink/S3ParquetBatchSink.java"
#   test_an_include c48f5570829cd49f4518afe2602007d4 "${cdap_etl}/batch/sink/SnapshotFileBatchAvroSink.java"
#   test_an_include 4d8181cc90ae3faf29a4502fae52eaa4 "${cdap_etl}/batch/sink/SnapshotFileBatchParquetSink.java"
#   test_an_include 288b88b520b32f16fb0d0dafab5c52be "${cdap_etl}/batch/sink/SnapshotFileBatchSink.java"
#   test_an_include 8e51b83878c90f000e59ada65630457d "${cdap_etl}/batch/sink/TableSink.java"
#   test_an_include 1075ca865257ad570a5e530f07e1358b "${cdap_etl}/batch/sink/TimePartitionedFileSetDatasetAvroSink.java"
#   test_an_include 6752ffa48d3e0303d5b9b59745360ad5 "${cdap_etl}/batch/sink/TimePartitionedFileSetDatasetParquetSink.java"
#   test_an_include 3069701c1070f0546d6a73800f558e72 "${cdap_etl}/batch/sink/TimePartitionedFileSetSink.java"
# 
#   # Batchsources
#   test_an_include b5fab5cd4722347f53f27443b4cdc96c "${cdap_etl}/batch/source/DBSource.java"
#   test_an_include 22fb2d61f2c3a37336c86e77fbc22616 "${cdap_etl}/batch/source/FileBatchSource.java"
#   test_an_include a3e5de66820f096813a6d58fdba86b52 "${cdap_etl}/batch/source/KVTableSource.java"
#   test_an_include 329b73ee4e2bba510f006f1b80ef0089 "${cdap_etl}/batch/source/S3BatchSource.java"
#   test_an_include 2af8f3fbf95a9d5f7becd9a7cbfaf6f9 "${cdap_etl}/batch/source/SnapshotFileBatchAvroSource.java"
#   test_an_include cef2765da51af1eac905d920b7b92522 "${cdap_etl}/batch/source/SnapshotFileBatchParquetSource.java"
#   test_an_include e7137b4c3e38ada2e10409d2b946bac6 "${cdap_etl}/batch/source/StreamBatchSource.java"
#   test_an_include f8c07741ac3b09da7554b91351e7973b "${cdap_etl}/batch/source/TableSource.java"
#   test_an_include 8835caf7e8c6c100904e88fc10b41ba2 "${cdap_etl}/batch/source/TimePartitionedFileSetDatasetAvroSource.java"
#   test_an_include 03a4aa20385b9fb084938dc3df18b4c7 "${cdap_etl}/batch/source/TimePartitionedFileSetDatasetParquetSource.java"
#   test_an_include d7231465fb80b3e945d8ea755535fe24 "${cdap_etl}/batch/source/TimePartitionedFileSetSource.java"
# 
#   # Realtimesinks
#   test_an_include 974ed9256c135872915a3f9061c5fc5c "${cdap_etl}/realtime/sink/RealtimeCubeSink.java"
#   test_an_include 72438554aeb5f590ff356c5fdd5569b4 "${cdap_etl}/realtime/sink/RealtimeTableSink.java"
#   test_an_include ff03b40c1dfdc25d75dc6d3547d7293b "${cdap_etl}/realtime/sink/StreamSink.java"
# 
#   # Realtimesources
#   test_an_include ea55a0dfc9f6e085fba25b47435ff064 "${cdap_etl}/realtime/source/DataGeneratorSource.java"
#   test_an_include c1d134466622468c36eaaf6a55677bb1 "${cdap_etl}/realtime/source/JmsSource.java"
#   test_an_include c0605df8382ea941966785e5ad589c4a "${cdap_etl}/realtime/source/KafkaSource.java"
#   test_an_include 62c19ecd2d694d3291b104645ad529a1 "${cdap_etl}/realtime/source/SqsSource.java"
#   test_an_include e8b987b6f648211ed183e18c68b41873 "${cdap_etl}/realtime/source/TwitterSource.java"
# 
#   # Transforms
#   test_an_include 06ddd340ba65bbc068ab3e3cf2f346c1 "${cdap_etl}/transform/LogParserTransform.java"
#   test_an_include e2aa3c8d77b8f229e2642f5e1f0975bc "${cdap_etl}/transform/ProjectionTransform.java"
#   test_an_include 480bc77f42c8c155a54deb6377ef05a2 "${cdap_etl}/transform/ScriptFilterTransform.java"
#   test_an_include 0e09daa8c8e7b008f1b19ca1da224884 "${cdap_etl}/transform/ScriptTransform.java"
#   test_an_include c3eb291d7b7d4ca0934d151bed882dd3 "${cdap_etl}/transform/StructuredRecordToGenericRecordTransform.java"
#   test_an_include a6acefc657d22f22133e931d92cfefa0 "${cdap_etl}/transform/ValidatorTransform.java"
# 
#   # Shared-Plugins
#   test_an_include 4fc697d071e894cfce67dbf62c9709d0 "${cdap_etl}/validator/CoreValidator.java"
  
  echo_red_bold "Downloading Markdown doc file includes from GitHub repo caskdata/hydrator-plugins..."
  # https://raw.githubusercontent.com/caskdata/hydrator-plugins/develop/cassandra-plugins/docs/Cassandra-batchsink.md
  set_version

  local includes="${1}/hydrator-plugins"
  local project_version=${PROJECT_SHORT_VERSION}

  local base_source="https://raw.githubusercontent.com/caskdata/hydrator-plugins"
  if [ "x${GIT_BRANCH_TYPE:0:7}" == "xdevelop" ]; then
    local hydrator_branch="develop"
  else
    local hydrator_branch="release/cdap-${project_version}-compatible"
  fi
  # FIXME: temp until branch created
  hydrator_branch="develop"
  
  local project_main="${base_source}/${hydrator_branch}"
  
  # 1:Includes directory 2:GitHub source directory 3:Hydrator directory 4:Markdown filename

  download_docs_file $includes $project_main cassandra-plugins Cassandra-batchsink.md
  download_docs_file $includes $project_main cassandra-plugins Cassandra-batchsource.md
  download_docs_file $includes $project_main cassandra-plugins Cassandra-realtimesink.md
  
  download_docs_file $includes $project_main core-plugins AmazonSQS-realtimesource.md
  download_docs_file $includes $project_main core-plugins Cube-batchsink.md
  download_docs_file $includes $project_main core-plugins Cube-realtimesink.md
  download_docs_file $includes $project_main core-plugins DataGenerator-realtimesource.md
  download_docs_file $includes $project_main core-plugins Database-batchsink.md
  download_docs_file $includes $project_main core-plugins Database-batchsource.md
  download_docs_file $includes $project_main core-plugins File-batchsource.md
  download_docs_file $includes $project_main core-plugins JMS-realtimesource.md
  download_docs_file $includes $project_main core-plugins KVTable-batchsink.md
  download_docs_file $includes $project_main core-plugins KVTable-batchsource.md
  download_docs_file $includes $project_main core-plugins Kafka-realtimesource.md
  download_docs_file $includes $project_main core-plugins Logparser-transform.md
  download_docs_file $includes $project_main core-plugins Projection-transform.md
  download_docs_file $includes $project_main core-plugins S3-batchsource.md
  download_docs_file $includes $project_main core-plugins S3Avro-batchsink.md
  download_docs_file $includes $project_main core-plugins S3Parquet-batchsink.md
  download_docs_file $includes $project_main core-plugins Script-transform.md
  download_docs_file $includes $project_main core-plugins ScriptFilter-transform.md
  download_docs_file $includes $project_main core-plugins SnapshotAvro-batchsink.md
  download_docs_file $includes $project_main core-plugins SnapshotAvro-batchsource.md
  download_docs_file $includes $project_main core-plugins SnapshotParquet-batchsink.md
  download_docs_file $includes $project_main core-plugins SnapshotParquet-batchsource.md
  download_docs_file $includes $project_main core-plugins Stream-batchsource.md
  download_docs_file $includes $project_main core-plugins Stream-realtimesink.md
  download_docs_file $includes $project_main core-plugins StructuredRecordToGenericRecord-transform.md
  download_docs_file $includes $project_main core-plugins TPFSAvro-batchsink.md
  download_docs_file $includes $project_main core-plugins TPFSAvro-batchsource.md
  download_docs_file $includes $project_main core-plugins TPFSParquet-batchsink.md
  download_docs_file $includes $project_main core-plugins TPFSParquet-batchsource.md
  download_docs_file $includes $project_main core-plugins Table-batchsink.md
  download_docs_file $includes $project_main core-plugins Table-batchsource.md
  download_docs_file $includes $project_main core-plugins Table-realtimesink.md
  download_docs_file $includes $project_main core-plugins Twitter-realtimesource.md
  download_docs_file $includes $project_main core-plugins Validator-transform.md
  
  download_docs_file $includes $project_main elasticsearch-plugins Elasticsearch-batchsink.md
  download_docs_file $includes $project_main elasticsearch-plugins Elasticsearch-batchsource.md
  download_docs_file $includes $project_main elasticsearch-plugins Elasticsearch-realtimesink.md
  
  download_docs_file $includes $project_main hive-plugins Hive-batchsink.md
  download_docs_file $includes $project_main hive-plugins Hive-batchsource.md
  
  download_docs_file $includes $project_main kafka-plugins KafkaProducer-realtimesink.md
  
  download_docs_file $includes $project_main mongodb-plugins MongoDB-batchsink.md
  download_docs_file $includes $project_main mongodb-plugins MongoDB-batchsource.md
  download_docs_file $includes $project_main mongodb-plugins MongoDB-realtimesink.md
  
  download_docs_file $includes $project_main python-evaluator-transform PythonEvaluator-transform.md
  
  download_docs_file $includes $project_main transform-plugins CSVFormatter-transform.md
  download_docs_file $includes $project_main transform-plugins CSVParser-transform.md
  download_docs_file $includes $project_main transform-plugins CloneRecord-transform.md
  download_docs_file $includes $project_main transform-plugins Compressor-transform.md
  download_docs_file $includes $project_main transform-plugins Decoder-transform.md
  download_docs_file $includes $project_main transform-plugins Decompressor-transform.md
  download_docs_file $includes $project_main transform-plugins Encoder-transform.md
  download_docs_file $includes $project_main transform-plugins Hasher-transform.md
  download_docs_file $includes $project_main transform-plugins JSONFormatter-transform.md
  download_docs_file $includes $project_main transform-plugins JSONParser-transform.md
  download_docs_file $includes $project_main transform-plugins StreamFormatter-transform.md
}

run_command ${1}

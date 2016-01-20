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
  
# Build script for docs

source ../vars
source ../_common/common-build.sh

CHECK_INCLUDES=${TRUE}

function download_md_doc_file() {
  # Downloads a Markdown docs file to a directory
  # https://raw.githubusercontent.com/caskdata/hydrator-plugins/develop/cassandra-plugins/docs/Cassandra-batchsink.md
  # goes to
  # hydrator-plugins/batchsinks/cassandra.md
  # 1:Includes dir 2:GitHub Hydrator source dir 3:Hydrator dir 4:Type 5:Target filename 6:Source Markdown filename
  # download_md_doc_file base_target  base_source      source_dir        source_file_name
  # download_md_doc_file $base_target $hydrator_source cassandra-plugins Cassandra-batchsink.md 
  local base_target="${1}"
  local base_source="${2}"
  local source_dir="${3}"
  local source_file_name="${4}" # JavaScript-transform.md
  local source_name="${source_file_name%-*}"

  local type=$(echo "${source_file_name#*-}" | tr [:upper:] [:lower:]) # batchsink
  type="${type%.md}" # strip suffix
  
  type_capital="$(echo ${type:0:1} | tr [:lower:] [:upper:])${type:1}"
  type_plural="${type}s" # types are plural
  local target_file_name=$(echo "${source_file_name%-*}.md" | tr [:upper:] [:lower:]) # cassandra

  local source_url="${base_source}/${source_dir}/docs/${source_file_name}"
  local target_dir="${base_target}/${type_plural}"
  local target="${target_dir}/${target_file_name}"
  
  if [ ! -d "${base_target}" ]; then
    mkdir -p ${base_target}
    echo "Creating Includes Directory: ${base_target}"
  fi
  if [ ! -d "${target_dir}" ]; then
    mkdir -p ${target_dir}
    echo "Creating Includes Directory: ${target_dir}"
  fi

  if curl --output /dev/null --silent --head --fail "${source_url}"; then
    echo "Downloading ${source_file_name} from ${source_dir} to ${type_plural}/${target_file_name}"
    curl --silent ${source_url} --output ${target}
    # FIXME if file does not begin with a "#" character, append "# title\n" to start
    local first=$(head -1 ${target})
    if [ "x${first:0:2}" != "x# " ]; then
      local m="Markdown file missing initial title: ${source_file_name}: ${source_name} ${type_capital}"
      echo_red_bold "${m}"
      set_message "${m}"
      echo -e "# ${source_name} ${type_capital}\n$(cat ${target})" > ${target}
    fi    
  else
    local m="URL does not exist: ${source_url}"
    echo_red_bold "${m}"
    set_message "${m}"
  fi   
}

function download_includes() {
  echo_red_bold "Downloading Markdown doc file includes from GitHub repo caskdata/hydrator-plugins..."
  set_version
  
  # Copy the source _includes files so they can be populated with the markdown files
  local hydrator_plugins="hydrator-plugins"
  local base_target="${1}/${hydrator_plugins}"
  cp -R "${SCRIPT_PATH}/source/_includes/${hydrator_plugins}" "${1}"
  
  local base_source="https://raw.githubusercontent.com/caskdata/hydrator-plugins"
  if [ "x${GIT_BRANCH_TYPE:0:7}" == "xdevelop" ]; then
    local hydrator_branch="develop"
  else
    local hydrator_branch="${GIT_BRANCH_CDAP_HYDRATOR}"
  fi
  
  local hydrator_source="${base_source}/${hydrator_branch}"
  echo_red_bold "Using $hydrator_source"

  # 1:Includes dir 2:GitHub Hydrator source dir 3:Hydrator dir 4:Type 5:Target filename 6:Source Markdown filename

  download_md_doc_file $base_target $hydrator_source cassandra-plugins Cassandra-batchsink.md 
  download_md_doc_file $base_target $hydrator_source cassandra-plugins Cassandra-batchsource.md 
  download_md_doc_file $base_target $hydrator_source cassandra-plugins Cassandra-realtimesink.md 

  download_md_doc_file $base_target $hydrator_source core-plugins AmazonSQS-realtimesource.md
  download_md_doc_file $base_target $hydrator_source core-plugins Cube-batchsink.md
  download_md_doc_file $base_target $hydrator_source core-plugins Cube-realtimesink.md
  download_md_doc_file $base_target $hydrator_source core-plugins DataGenerator-realtimesource.md
  download_md_doc_file $base_target $hydrator_source core-plugins Database-batchsink.md
  download_md_doc_file $base_target $hydrator_source core-plugins Database-batchsource.md
  download_md_doc_file $base_target $hydrator_source core-plugins File-batchsource.md
  download_md_doc_file $base_target $hydrator_source core-plugins JMS-realtimesource.md
  download_md_doc_file $base_target $hydrator_source core-plugins JavaScript-transform.md
  download_md_doc_file $base_target $hydrator_source core-plugins KVTable-batchsink.md
  download_md_doc_file $base_target $hydrator_source core-plugins KVTable-batchsource.md
  download_md_doc_file $base_target $hydrator_source core-plugins Kafka-realtimesource.md
  download_md_doc_file $base_target $hydrator_source core-plugins Logparser-transform.md
  download_md_doc_file $base_target $hydrator_source core-plugins Projection-transform.md
  download_md_doc_file $base_target $hydrator_source core-plugins S3-batchsource.md
  download_md_doc_file $base_target $hydrator_source core-plugins S3Avro-batchsink.md
  download_md_doc_file $base_target $hydrator_source core-plugins S3Parquet-batchsink.md
  download_md_doc_file $base_target $hydrator_source core-plugins Script-transform.md
  download_md_doc_file $base_target $hydrator_source core-plugins ScriptFilter-transform.md
  download_md_doc_file $base_target $hydrator_source core-plugins SnapshotAvro-batchsink.md
  download_md_doc_file $base_target $hydrator_source core-plugins SnapshotAvro-batchsource.md
  download_md_doc_file $base_target $hydrator_source core-plugins SnapshotParquet-batchsink.md
  download_md_doc_file $base_target $hydrator_source core-plugins SnapshotParquet-batchsource.md
  download_md_doc_file $base_target $hydrator_source core-plugins Stream-batchsource.md
  download_md_doc_file $base_target $hydrator_source core-plugins Stream-realtimesink.md
  download_md_doc_file $base_target $hydrator_source core-plugins StructuredRecordToGenericRecord-transform.md
  download_md_doc_file $base_target $hydrator_source core-plugins TPFSAvro-batchsink.md
  download_md_doc_file $base_target $hydrator_source core-plugins TPFSAvro-batchsource.md
  download_md_doc_file $base_target $hydrator_source core-plugins TPFSParquet-batchsink.md
  download_md_doc_file $base_target $hydrator_source core-plugins TPFSParquet-batchsource.md
  download_md_doc_file $base_target $hydrator_source core-plugins Table-batchsink.md
  download_md_doc_file $base_target $hydrator_source core-plugins Table-batchsource.md
  download_md_doc_file $base_target $hydrator_source core-plugins Table-realtimesink.md
  download_md_doc_file $base_target $hydrator_source core-plugins Twitter-realtimesource.md
  download_md_doc_file $base_target $hydrator_source core-plugins Validator-transform.md
  
  download_md_doc_file $base_target $hydrator_source database-plugins Database-batchsink.md
  download_md_doc_file $base_target $hydrator_source database-plugins Database-batchsource.md
  download_md_doc_file $base_target $hydrator_source database-plugins Teradata-batchsource.md
  
  download_md_doc_file $base_target $hydrator_source elasticsearch-plugins Elasticsearch-batchsink.md
  download_md_doc_file $base_target $hydrator_source elasticsearch-plugins Elasticsearch-batchsource.md
  download_md_doc_file $base_target $hydrator_source elasticsearch-plugins Elasticsearch-realtimesink.md
  
  download_md_doc_file $base_target $hydrator_source hbase-plugins HBase-batchsink.md
  download_md_doc_file $base_target $hydrator_source hbase-plugins HBase-batchsource.md
  
  download_md_doc_file $base_target $hydrator_source hdfs-plugins HDFS-batchsink.md
  
  download_md_doc_file $base_target $hydrator_source hive-plugins Hive-batchsink.md
  download_md_doc_file $base_target $hydrator_source hive-plugins Hive-batchsource.md
  
  download_md_doc_file $base_target $hydrator_source kafka-plugins KafkaProducer-realtimesink.md
  
  download_md_doc_file $base_target $hydrator_source mongodb-plugins MongoDB-batchsink.md
  download_md_doc_file $base_target $hydrator_source mongodb-plugins MongoDB-batchsource.md
  download_md_doc_file $base_target $hydrator_source mongodb-plugins MongoDB-realtimesink.md
  
  download_md_doc_file $base_target $hydrator_source python-evaluator-transform PythonEvaluator-transform.md
  
  download_md_doc_file $base_target $hydrator_source transform-plugins CSVFormatter-transform.md
  download_md_doc_file $base_target $hydrator_source transform-plugins CSVParser-transform.md
  download_md_doc_file $base_target $hydrator_source transform-plugins CloneRecord-transform.md
  download_md_doc_file $base_target $hydrator_source transform-plugins Compressor-transform.md
  download_md_doc_file $base_target $hydrator_source transform-plugins Decoder-transform.md
  download_md_doc_file $base_target $hydrator_source transform-plugins Decompressor-transform.md
  download_md_doc_file $base_target $hydrator_source transform-plugins Encoder-transform.md
  download_md_doc_file $base_target $hydrator_source transform-plugins Hasher-transform.md
  download_md_doc_file $base_target $hydrator_source transform-plugins JSONFormatter-transform.md
  download_md_doc_file $base_target $hydrator_source transform-plugins JSONParser-transform.md
  download_md_doc_file $base_target $hydrator_source transform-plugins StreamFormatter-transform.md

}

run_command ${1}

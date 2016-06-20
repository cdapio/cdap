#!/usr/bin/env bash

# Copyright © 2016 Cask Data, Inc.
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

EXTRACT_TABLE_TOOL="../tools/docs-extract-table.py"

CHECK_INCLUDES=${TRUE}

RETURN_STRING="\
"
VERSION_STRING="Hydrator Version "

function get_hydrator_version() {
  local base_target="${1}"
  local base_source="${2}"
  local source_url="${base_source}/pom.xml"
  local target="${base_target}/pom.xml"
  curl --silent ${source_url} --output ${target}
  HYDRATOR_VERSION=$(grep "<version>" ${target})
  HYDRATOR_VERSION=${HYDRATOR_VERSION#*<version>}
  HYDRATOR_VERSION=${HYDRATOR_VERSION%%</version>*}
  export HYDRATOR_VERSION
}

function download_md_doc_file() {
  return
  # Downloads a Markdown docs file to a directory
  #
  # https://raw.githubusercontent.com/caskdata/hydrator-plugins/develop/cassandra-plugins/docs/Cassandra-batchsink.md
  # goes to
  # hydrator-plugins/batchsinks/cassandra.md
  #
  # download_md_doc_file base_target  base_source      source_dir        source_file_name       append_file (optional)
  # download_md_doc_file $base_target $hydrator_source cassandra-plugins Cassandra-batchsink.md append.txt
#   local base_target="${1}"
#   local base_source="${2}"
#   local source_dir="${3}"
#   local source_file_name="${4}" # JavaScript-transform.md
#   local append_file="${5}"
#   local source_name="${source_file_name%-*}"
# 
#   local type=$(echo "${source_file_name#*-}" | tr [:upper:] [:lower:]) # batchsink
#   type="${type%.md}" # strip suffix
#   
#   type_capital="$(echo ${type:0:1} | tr [:lower:] [:upper:])${type:1}"
#   type_plural="${type}s" # types are plural
#   local target_file_name=$(echo "${source_file_name%-*}.md" | tr [:upper:] [:lower:]) # cassandra
# 
#   local source_url="${base_source}/${source_dir}/docs/${source_file_name}"
#   local target_dir="${base_target}/${type_plural}"
#   local target="${target_dir}/${target_file_name}"
#   
#   if [ ! -d "${base_target}" ]; then
#     mkdir -p ${base_target}
#     echo "Creating Includes Directory: ${base_target}"
#   fi
#   if [ ! -d "${target_dir}" ]; then
#     mkdir -p ${target_dir}
#     echo "Creating Includes Directory: ${target_dir}"
#   fi
# 
#   if curl --output /dev/null --silent --head --fail "${source_url}"; then
#     echo "Downloading ${source_file_name} from ${source_dir} to ${type_plural}/${target_file_name}"
#     curl --silent ${source_url} --output ${target}
#     # FIXME if file does not begin with a "#" character, append "# title\n" to start
#     local first=$(head -1 ${target})
#     if [ "x${first:0:2}" != "x# " ]; then
#       local m="Markdown file missing initial title: ${source_file_name}: ${source_name} ${type_capital}"
#       echo_red_bold "${m}"
#       set_message "${m}"
#       echo -e "# ${source_name} ${type_capital}\n$(cat ${target})" > ${target}
#     fi
#     if [[ "x${append_file}" != "x" ]]; then
#       echo "  Appending ${append_file} to ${target_file_name}"
#       cat ${base_target}/${append_file} >> ${target}
#     fi
#     echo "${RETURN_STRING}" >> ${target}
#     echo "${VERSION_STRING}${HYDRATOR_VERSION}" >> ${target}
#   else
#     local m="URL does not exist: ${source_url}"
#     echo_red_bold "${m}"
#     set_message "${m}"
#   fi
}

function download_md_file() {
#   echo "download_md_file: BASE_TARGET: $BASE_TARGET"
#   echo "download_md_file: HYDRATOR_SOURCE: $HYDRATOR_SOURCE"
  local source_dir="${1}"
  local source_file_name="${2}"
  local append_file="${3}"
  local source_name="${source_file_name%-*}"

  local type=$(echo "${source_file_name#*-}" | tr [:upper:] [:lower:]) # batchsink
  type="${type%.md}" # strip suffix
  
  local type_capital="$(echo ${type:0:1} | tr [:lower:] [:upper:])${type:1}"
  local type_plural="${type}s" # types are plural
  local target_file_name=$(echo "${source_file_name%-*}.md" | tr [:upper:] [:lower:]) # cassandra

  # Determine category (batch or realtime) and type (source, sink, transform) of plugin from name
  local plugin_category
  local plugin_type
  if [[ "x${type:0:5}" == "xbatch" ]]; then
    plugin_category="batch"
    plugin_type="${type:5}"
  elif [[ "x${type:0:8}" == "xrealtime" ]]; then
    plugin_category="realtime"
    plugin_type="${type:8}"
  else
  # type transform ?
    plugin_category=''
    plugin_type="transform"
  fi
#   echo "download_md_file: plugin_category: $plugin_category"
#   echo "download_md_file: plugin_type: $plugin_type"
    
  local source_url="${HYDRATOR_SOURCE}/${source_dir}/docs/${source_file_name}"
  if [[ "x${plugin_category}" == "x" ]]; then
    local target_dir="${BASE_TARGET}/${plugin_type}"
  else
    local target_dir="${BASE_TARGET}/${plugin_category}/${plugin_type}"
  fi
  local target="${target_dir}/${target_file_name}"
  
#   if [ ! -d "${BASE_TARGET}" ]; then
#     mkdir -p ${BASE_TARGET}
#     echo "Creating Includes Directory: ${BASE_TARGET}"
#   fi
  if [ ! -d "${target_dir}" ]; then
    mkdir -p ${target_dir}
    echo "Creating Includes Directory: ${target_dir}"
  fi

  if curl --output /dev/null --silent --head --fail "${source_url}"; then
    echo "Downloading ${source_file_name} from ${source_dir} to ${type_plural}/${target_file_name}"
    #     Downloading Cassandra-batchsink.md from cassandra-plugins to batchsinks/cassandra.md
    curl --silent ${source_url} --output ${target}
    # If file does not begin with a "#" character, append "# title\n" to start
    local first=$(head -1 ${target})
    if [ "x${first:0:2}" != "x# " ]; then
      local m="Markdown file missing initial title: ${source_file_name}: ${source_name} ${type_capital}"
      echo_red_bold "${m}"
      set_message "${m}"
      echo -e "# ${source_name} ${type_capital}\n$(cat ${target})" > ${target}
    fi
    if [[ "x${append_file}" != "x" ]]; then
      echo "  Appending ${append_file} to ${target_file_name}"
      cat ${BASE_TARGET}/_includes/${append_file} >> ${target}
    fi
    echo "\n${VERSION_STRING}${HYDRATOR_VERSION}" >> ${target}
  else
    local m="URL does not exist: ${source_url}"
    echo_red_bold "${m}"
    set_message "${m}"
  fi

}

function extract_table() {
  # Extracts a table written in Markdown from a file so it can be reused in reST files
  local base_target="${1}"
  local source_file="${2}"
  local target_file="${3}"

  local extract_table_input="${base_target}/${source_file}"
  local extract_table_output="${base_target}/${target_file}"

  echo "Extracting table from ${extract_table_input}"
  echo "Writing table info to ${extract_table_output}"
  python "${EXTRACT_TABLE_TOOL}" "${extract_table_input}" "${extract_table_output}"
}

function download_includes() {
  echo_red_bold "Downloading Markdown doc file includes from GitHub repo caskdata/hydrator-plugins..."
  set_version

  # Copy the source _includes files so they can be populated with the markdown files
  local hydrator_plugins="hydrator-plugins"
  local plugins="plugins"
  BASE_TARGET="${1}/${plugins}"
  cp -R "${SCRIPT_PATH}/source/_includes/${plugins}" "${1}"
  
  local base_source="https://raw.githubusercontent.com/caskdata/${hydrator_plugins}"
  if [ "x${GIT_BRANCH_TYPE:0:7}" == "xdevelop" ]; then
    local hydrator_branch="develop"
  else
    local hydrator_branch="${GIT_BRANCH_CDAP_HYDRATOR}"
  fi
  
  HYDRATOR_SOURCE="${base_source}/${hydrator_branch}"
  echo_red_bold "Using ${HYDRATOR_SOURCE}"
  get_hydrator_version ${BASE_TARGET} ${HYDRATOR_SOURCE}
  
  # Original Downloads
  
  # Parameter          1            2                3                 4                      5
  # Definition         base_target  base_source      source_dir        source_file_name       append_file (optional)
  download_md_doc_file $base_target $hydrator_source cassandra-plugins Cassandra-batchsink.md 
  download_md_doc_file $base_target $hydrator_source cassandra-plugins Cassandra-batchsource.md 
  download_md_doc_file $base_target $hydrator_source cassandra-plugins Cassandra-realtimesink.md 

  download_md_doc_file $base_target $hydrator_source core-plugins AmazonSQS-realtimesource.md
  download_md_doc_file $base_target $hydrator_source core-plugins AzureBlobStore-batchsource.md
  download_md_doc_file $base_target $hydrator_source core-plugins Cube-batchsink.md
  download_md_doc_file $base_target $hydrator_source core-plugins Cube-realtimesink.md
  download_md_doc_file $base_target $hydrator_source core-plugins DataGenerator-realtimesource.md
  download_md_doc_file $base_target $hydrator_source core-plugins File-batchsource.md
  download_md_doc_file $base_target $hydrator_source core-plugins JMS-realtimesource.md
  download_md_doc_file $base_target $hydrator_source core-plugins JavaScript-transform.md
  download_md_doc_file $base_target $hydrator_source core-plugins KVTable-batchsink.md
  download_md_doc_file $base_target $hydrator_source core-plugins KVTable-batchsource.md
  download_md_doc_file $base_target $hydrator_source core-plugins LogParser-transform.md
  download_md_doc_file $base_target $hydrator_source core-plugins Projection-transform.md
  download_md_doc_file $base_target $hydrator_source core-plugins PythonEvaluator-transform.md
  download_md_doc_file $base_target $hydrator_source core-plugins S3-batchsource.md
  download_md_doc_file $base_target $hydrator_source core-plugins S3Avro-batchsink.md
  download_md_doc_file $base_target $hydrator_source core-plugins S3Parquet-batchsink.md
  download_md_doc_file $base_target $hydrator_source core-plugins Script-transform.md
  download_md_doc_file $base_target $hydrator_source core-plugins ScriptFilter-transform.md
  download_md_doc_file $base_target $hydrator_source core-plugins SnapshotAvro-batchsink.md
  download_md_doc_file $base_target $hydrator_source core-plugins SnapshotAvro-batchsource.md
  download_md_doc_file $base_target $hydrator_source core-plugins SnapshotParquet-batchsource.md
  download_md_doc_file $base_target $hydrator_source core-plugins SnapshotParquet-batchsink.md
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

  download_md_doc_file $base_target $hydrator_source database-plugins Database-batchsink.md database-batchsink-append.txt
  download_md_doc_file $base_target $hydrator_source database-plugins Database-batchsource.md database-batchsource-append.txt
  
  download_md_doc_file $base_target $hydrator_source elasticsearch-plugins Elasticsearch-batchsink.md
  download_md_doc_file $base_target $hydrator_source elasticsearch-plugins Elasticsearch-batchsource.md
  download_md_doc_file $base_target $hydrator_source elasticsearch-plugins Elasticsearch-realtimesink.md
  
  download_md_doc_file $base_target $hydrator_source hbase-plugins HBase-batchsink.md
  download_md_doc_file $base_target $hydrator_source hbase-plugins HBase-batchsource.md
  
  download_md_doc_file $base_target $hydrator_source hdfs-plugins HDFS-batchsink.md
  
  download_md_doc_file $base_target $hydrator_source hive-plugins Hive-batchsink.md
  download_md_doc_file $base_target $hydrator_source hive-plugins Hive-batchsource.md
  
  download_md_doc_file $base_target $hydrator_source kafka-plugins Kafka-realtimesource.md
  download_md_doc_file $base_target $hydrator_source kafka-plugins KafkaProducer-realtimesink.md
  
  download_md_doc_file $base_target $hydrator_source mongodb-plugins MongoDB-batchsink.md
  download_md_doc_file $base_target $hydrator_source mongodb-plugins MongoDB-batchsource.md
  download_md_doc_file $base_target $hydrator_source mongodb-plugins MongoDB-realtimesink.md
  
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

#   extract_table ${base_target} transforms/validator.md validator-extract.txt

  # New Downloads
  # Uses: $base_target  $hydrator_source
  
  # Parameter      1                 2                         3
  # Definition     source_dir        source_file_name          append_file (optional)
  download_md_file cassandra-plugins Cassandra-batchsink.md 
  download_md_file cassandra-plugins Cassandra-batchsource.md 
  download_md_file cassandra-plugins Cassandra-realtimesink.md 

  download_md_file database-plugins Database-batchsink.md database-batchsink-append.md.txt
  download_md_file database-plugins Database-batchsource.md database-batchsource-append.md.txt

  download_md_file transform-plugins StreamFormatter-transform.md

}

run_command ${1}

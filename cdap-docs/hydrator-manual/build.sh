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

CHECK_INCLUDES=${TRUE}

EXTRACT_TABLE_TOOL="../tools/docs-extract-table.py"

DOUBLE_RETURN_STRING="\

"
SINGLE_RETURN_STRING="\
"
TRIPLE_RETURN_STRING="\


"
RULE="${SINGLE_RETURN_STRING}---${DOUBLE_RETURN_STRING}"

# PRE_POST_RUN="pre-post-run"
PRE_POST_RUN="post-run-plugin"
PLUGIN_TYPE_STRING="Hydrator Plugin Type:"
VERSION_STRING="Hydrator Version:"

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

function download_md_file() {
  local source_dir="${1}"
  local source_file_name="${2}"
  local append_file="${3}"
  local plugin_category="${4}"
  local plugin_type="${5}" # NOTE: singular types: "sink", not "sinks"
  
  local source_url="${HYDRATOR_SOURCE}/${source_dir}/docs/${source_file_name}"

  local source_name="${source_file_name%-*}"
  local type=$(echo "${source_file_name#*-}" | tr [:upper:] [:lower:]) # batchsink
  type="${type%.md}" # strip suffix
  
  local type_capital="$(echo ${type:0:1} | tr [:lower:] [:upper:])${type:1}"
  local target_file_name=$(echo "${source_file_name%-*}.md" | tr [:upper:] [:lower:]) # cassandra

  # Determine from name of the plugin file the:
  # category (batch, realtime, shared-plugin, postaction) and 
  # type (source, sink, transform, aggregator)
  # Defining these in the parameters overrides this
  if [[ "x${plugin_category}${plugin_type}" == "x" ]]; then
    if [[ "x${type:0:5}" == "xbatch" ]]; then
      plugin_category="batch"
      plugin_type="${type:5}"
    elif [[ "x${type:0:8}" == "xrealtime" ]]; then
      plugin_category="realtime"
      plugin_type="${type:8}"
      
    # FIXME: this type "postaction" is going away
    elif [[ "x${type}" == "xpostaction" ]]; then
#       plugin_category="${PRE_POST_RUN}"
      plugin_category=''
      plugin_type="post-run-plugin"
    # END FIXME
    
    # FIXME: these types "prerun" and  "postrun" are currently not used
#     elif [[ "x${type}" == "xprerun" ]]; then
#       plugin_category="${PRE_POST_RUN}"
#       plugin_type="pre-run"
#     elif [[ "x${type}" == "xpostrun" ]]; then
#       plugin_category=''
#       plugin_type="post-run"
    # END FIXME
    elif [[ "x${type}" == "xaction" ]]; then
      plugin_category=''
      plugin_type="action"
    else
      # assume of type transform; to be copied to both batch and realtime
      plugin_category=''
      plugin_type="transform"
    fi
  fi
    
  if [[ "x${plugin_category}" != "x" ]]; then
    if [[ ( "${plugin_type}" == "sink" ) || ( "${plugin_type}" == "source" ) ]]; then
      local target_dir="${plugin_category}/${plugin_type}s"
    else
      local target_dir="${plugin_category}/transforms"
    fi
    local target_dir_extra=''
  elif [[ "${plugin_type}" == "transform" ]]; then
    # Directories are plural, though types are singular
    local target_dir="batch/${plugin_type}s"
    local target_dir_extra="realtime/${plugin_type}s"
  elif [[ ( "${plugin_type}" == "action" ) || ( "${plugin_type}" == "${PRE_POST_RUN}" ) ]]; then
    local target_dir="${plugin_type}s"
    local target_dir_extra='' 
  fi
  
  local target="${BASE_TARGET}/${target_dir}/${target_file_name}"
  local target_extra=''
  if [[ "x${target_dir_extra}" != "x" ]]; then
    target_extra="${BASE_TARGET}/${target_dir_extra}/${target_file_name}"
  fi

  # Create display names for log output
  local fifty_spaces="                                                  "
  local display_source_file_name="${source_file_name}${fifty_spaces}"
  display_source_file_name="${display_source_file_name:0:50}"
  local display_source_dir="${source_dir}${fifty_spaces}"
  display_source_dir="${display_source_dir:0:25}"

  if curl --output /dev/null --silent --head --fail "${source_url}"; then
    echo "Downloading ${display_source_file_name} from ${display_source_dir} to ${target_dir}/${target_file_name}"
    #     Downloading Cassandra-batchsink.md from cassandra-plugins to batch/sink/cassandra.md
    curl --silent ${source_url} --output ${target}
    if [ -e "${target}" ]; then
      # If file does not begin with a "#" character, append "# title\n" to start
      local first=$(head -1 ${target})
      if [[ "x${first:0:2}" != "x# " ]]; then
        local m="Markdown file missing initial title: ${source_file_name}: ${source_name} ${type_capital}"
        echo_red_bold "${m}"
        set_message "${m}"
        echo "# ${source_name} ${type_capital}${DOUBLE_RETURN_STRING}$(cat ${target})" > ${target}
      fi
      if [[ "x${append_file}" != "x" ]]; then
        echo "  Appending ${append_file} to ${target_file_name}"
        cat ${BASE_TARGET}/${append_file} >> ${target}
      fi
      echo "${DOUBLE_RETURN_STRING}${RULE}- ${PLUGIN_TYPE_STRING} ${type}${DOUBLE_RETURN_STRING}- ${VERSION_STRING} ${HYDRATOR_VERSION}" >> ${target}
      if [[ "x${target_dir_extra}" != "x" ]]; then
        cp ${target} ${target_extra}
        echo "  Copied    ${display_source_file_name} from ${display_source_dir} to ${target_dir_extra}/${target_file_name}"
      fi
    else
      local m="File does not exist: ${target}"
      echo_red_bold "From ${source_url}"
      echo_red_bold "${m}"
      set_message "${m}"
    fi
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
  local hydrator_plugins="hydrator-plugins"
  local plugins="plugins"
  local current_directory=$(pwd)

  set_version

  if [ "x${LOCAL_INCLUDES}" == "x${TRUE}" ]; then
    echo_red_bold "Copying local copies of Markdown doc file includes..."
    local base_source="file://${PROJECT_PATH}/../${hydrator_plugins}"
    HYDRATOR_SOURCE="${base_source}"
  else
    echo_red_bold "Downloading Markdown doc file includes from GitHub repo caskdata/hydrator-plugins..."
    local base_source="https://raw.githubusercontent.com/caskdata/${hydrator_plugins}"
    if [ "x${GIT_BRANCH_TYPE:0:7}" == "xdevelop" ]; then
      local hydrator_branch="develop"
    else
      local hydrator_branch="${GIT_BRANCH_CASK_HYDRATOR}"
    fi
    HYDRATOR_SOURCE="${base_source}/${hydrator_branch}"
  fi

  # Copy the source _includes files so they can be populated with the markdown files
  BASE_TARGET="${1}/${plugins}"
  cp -R "${SCRIPT_PATH}/source/_includes/${plugins}" "${1}"
  
  echo_red_bold "Using ${HYDRATOR_SOURCE}"
  get_hydrator_version ${BASE_TARGET} ${HYDRATOR_SOURCE}
  
  # Uses: $BASE_TARGET  $HYDRATOR_SOURCE
  # Parameter      1                 2                         3
  # Definition     source_dir        source_file_name          append_file (optional)
  
  download_md_file cassandra-plugins Cassandra-batchsink.md
  download_md_file cassandra-plugins Cassandra-batchsource.md 
  download_md_file cassandra-plugins Cassandra-realtimesink.md 

  download_md_file copybookreader-plugins CopybookReader-batchsource.md 

  download_md_file core-plugins AmazonSQS-realtimesource.md
  download_md_file core-plugins AzureBlobStore-batchsource.md
  download_md_file core-plugins Cube-batchsink.md
  download_md_file core-plugins Cube-realtimesink.md
  download_md_file core-plugins DataGenerator-realtimesource.md
  download_md_file core-plugins Deduplicate-batchaggregator.md
  download_md_file core-plugins Distinct-batchaggregator.md
  download_md_file core-plugins Email-postaction.md
  download_md_file core-plugins Excel-batchsource.md
  download_md_file core-plugins File-batchsource.md
  download_md_file core-plugins FTP-batchsource.md
  download_md_file core-plugins GroupByAggregate-batchaggregator.md
  download_md_file core-plugins HDFSFileMoveAction-action.md
  download_md_file core-plugins JavaScript-transform.md
  download_md_file core-plugins JMS-realtimesource.md
  download_md_file core-plugins Joiner-batchjoiner.md
  download_md_file core-plugins KVTable-batchsink.md
  download_md_file core-plugins KVTable-batchsource.md
  download_md_file core-plugins LogParser-transform.md
  download_md_file core-plugins Projection-transform.md
  download_md_file core-plugins PythonEvaluator-transform.md
  download_md_file core-plugins RowDenormalizer-batchaggregator.md
  download_md_file core-plugins S3-batchsource.md
  download_md_file core-plugins S3Avro-batchsink.md
  download_md_file core-plugins S3Parquet-batchsink.md
  download_md_file core-plugins ScriptFilter-transform.md
  download_md_file core-plugins SnapshotAvro-batchsink.md
  download_md_file core-plugins SnapshotAvro-batchsource.md
  download_md_file core-plugins SnapshotParquet-batchsink.md
  download_md_file core-plugins SnapshotParquet-batchsource.md
  download_md_file core-plugins SSHAction-action.md
  download_md_file core-plugins Stream-batchsource.md
  download_md_file core-plugins Stream-realtimesink.md
  download_md_file core-plugins StructuredRecordToGenericRecord-transform.md
  download_md_file core-plugins Table-batchsink.md
  download_md_file core-plugins Table-batchsource.md
  download_md_file core-plugins Table-realtimesink.md
  download_md_file core-plugins TPFSAvro-batchsink.md
  download_md_file core-plugins TPFSAvro-batchsource.md
  download_md_file core-plugins TPFSParquet-batchsink.md
  download_md_file core-plugins TPFSParquet-batchsource.md
  download_md_file core-plugins Twitter-realtimesource.md
  download_md_file core-plugins Validator-transform.md
  download_md_file core-plugins XMLReader-batchsource.md

  download_md_file database-plugins Database-batchsink.md _includes/database-batchsink-append.md.txt
  download_md_file database-plugins Database-batchsource.md _includes/database-batchsource-append.md.txt
  download_md_file database-plugins DatabaseQuery-postaction.md

  download_md_file elasticsearch-plugins Elasticsearch-batchsink.md
  download_md_file elasticsearch-plugins Elasticsearch-batchsource.md
  download_md_file elasticsearch-plugins Elasticsearch-realtimesink.md
  
  download_md_file hbase-plugins HBase-batchsink.md
  download_md_file hbase-plugins HBase-batchsource.md
  
  download_md_file hdfs-plugins HDFS-batchsink.md
  
  download_md_file hive-plugins Hive-batchsink.md
  download_md_file hive-plugins Hive-batchsource.md
  
  download_md_file http-plugins HTTPCallback-postaction.md
  download_md_file http-plugins HTTPPoller-realtimesource.md

  download_md_file kafka-plugins Kafka-realtimesource.md
  download_md_file kafka-plugins KafkaProducer-realtimesink.md
  
  download_md_file mongodb-plugins MongoDB-batchsink.md
  download_md_file mongodb-plugins MongoDB-batchsource.md
  download_md_file mongodb-plugins MongoDB-realtimesink.md
  
  download_md_file spark-plugins Kafka-streamingsource.md
  # Currently only for batch
  download_md_file spark-plugins NaiveBayesClassifier-sparkcompute.md '' "batch" "transform"
  download_md_file spark-plugins NaiveBayesTrainer-sparksink.md       '' "batch" "transform"
  
  download_md_file transform-plugins CloneRecord-transform.md
  download_md_file transform-plugins Compressor-transform.md
  download_md_file transform-plugins CSVFormatter-transform.md
  download_md_file transform-plugins CSVParser-transform.md
  download_md_file transform-plugins Decoder-transform.md
  download_md_file transform-plugins Decompressor-transform.md
  download_md_file transform-plugins Encoder-transform.md
  download_md_file transform-plugins Hasher-transform.md
  download_md_file transform-plugins JSONFormatter-transform.md
  download_md_file transform-plugins JSONParser-transform.md
  download_md_file transform-plugins Normalize-transform.md
  download_md_file transform-plugins StreamFormatter-transform.md
  download_md_file transform-plugins ValueMapper-transform.md
  download_md_file transform-plugins XMLParser-transform.md
  download_md_file transform-plugins XMLToJSON-transform.md

  extract_table ${BASE_TARGET} "batch/transforms/validator.md" _includes/validator-extract.txt
}

run_command ${1}

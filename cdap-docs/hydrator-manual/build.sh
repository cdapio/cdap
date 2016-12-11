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
NON_TRANSFORM_TYPES="analytic action source sink ${PRE_POST_RUN}"
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
  local plugin_type="${4}" # NOTE: singular types: "sink", not "sinks"
  local target_file_name="${5}"
  
  local source_url="${HYDRATOR_SOURCE}/${source_dir}/docs/${source_file_name}"

  local source_name="${source_file_name%-*}"
  local type=$(echo "${source_file_name#*-}" | tr [:upper:] [:lower:]) # batchsink
  type="${type%.md}" # strip suffix
  
  local type_capital="$(echo ${type:0:1} | tr [:lower:] [:upper:])${type:1}"
  if [[ "x${target_file_name}" == "x" ]]; then
    local target_file_name=$(echo "${source_file_name}" | tr [:upper:] [:lower:]) # cassandra-batchsink.md
  fi
  
  # Determine from name of the plugin file the:
  # type (source, sink, transform, shared-plugin, postaction)
  # Defining this in the parameters overrides this
  if [[ "x${plugin_type}" == "x" ]]; then
    # FIXME: this type "postaction" maybe going away
    if [[ "x${type}" == "xpostaction" ]]; then
      plugin_type="post-run-plugin"
    # END FIXME
    # FIXME: these types "prerun" and  "postrun" are currently not used
#     elif [[ "x${type}" == "xprerun" ]]; then
#       plugin_type="pre-run"
#     elif [[ "x${type}" == "xpostrun" ]]; then
#       plugin_type="post-run"
    # END FIXME
    elif [[ "x${type}" == "xaction" ]]; then
      plugin_type="${type}"
    elif [[ "x${type}" == "xtransform" ]]; then
      plugin_type="${type}"
    elif [[ "x${type: -6}" == "xsource" ]]; then
      plugin_type="${type: -6}"
    elif [[ "x${type: -4}" == "xsink" ]]; then
      plugin_type="${type: -  4}"
    else
      # assume of type transform; to be copied to both batch and realtime
      plugin_type="transform"
    fi
  fi
  
  local target_dir="${plugin_type}s"

  echo ${NON_TRANSFORM_TYPES} | grep -q ${plugin_type}
  if [ "$?" == "0" ]; then
    local target_dir="${plugin_type}s"
  else
    local target_dir="transforms"
  fi

  local target="${BASE_TARGET}/${target_dir}/${target_file_name}"

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
#         echo "# ${source_name} ${type_capital}${DOUBLE_RETURN_STRING}$(cat ${target})" > ${target}
        echo "# ${source_name}${DOUBLE_RETURN_STRING}$(cat ${target})" > ${target}
      else
        # Remove title suffixes
        tail -n +2 "${target}" > "${target}.tmp" && mv "${target}.tmp" "${target}"
        # Strip trailing whitespace
        first="${first%"${first##*[![:space:]]}"}"
        # Strip trailing items of interest
        first=${first% Batch Sink}
        first=${first% Batch Source}
        first=${first% Post-run Action}
        first=${first% Post Action}
        first=${first% Real-time Sink}
        first=${first% Real-time Source}
        first=${first% Action}
        first=${first% Source}
        first=${first% Transform}
        echo "${first}${DOUBLE_RETURN_STRING}$(cat ${target})" > ${target}
      fi
      if [[ "x${append_file}" != "x" ]]; then
        echo "  Appending ${append_file} to ${target_file_name}"
        cat ${BASE_TARGET}/${append_file} >> ${target}
      fi
      echo "${DOUBLE_RETURN_STRING}${RULE}- ${PLUGIN_TYPE_STRING} ${type}${DOUBLE_RETURN_STRING}- ${VERSION_STRING} ${HYDRATOR_VERSION}" >> ${target}
    else
      local m="File does not exist for ${target}"
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
  # Parameter      1                 2                         3 (optional)  4 (optional)  5 (optional)
  # Definition     source_dir        source_file_name          append_file   target_file   target_dir
  
  download_md_file cassandra-plugins Cassandra-batchsink.md
  download_md_file cassandra-plugins Cassandra-batchsource.md 
  download_md_file cassandra-plugins Cassandra-realtimesink.md 
  download_md_file core-plugins AmazonSQS-realtimesource.md
  download_md_file core-plugins AzureBlobStore-batchsource.md
  download_md_file core-plugins Cube-batchsink.md
  download_md_file core-plugins Cube-realtimesink.md
  download_md_file core-plugins DataGenerator-realtimesource.md
  download_md_file core-plugins Deduplicate-batchaggregator.md '' "analytic"
  download_md_file core-plugins Distinct-batchaggregator.md '' "analytic"
  download_md_file core-plugins Email-postaction.md
  download_md_file core-plugins Excel-batchsource.md
  download_md_file core-plugins File-batchsource.md
  download_md_file core-plugins FTP-batchsource.md
  download_md_file core-plugins GroupByAggregate-batchaggregator.md '' "analytic"
  download_md_file core-plugins HDFSDelete-action.md
  download_md_file core-plugins HDFSMove-action.md
  download_md_file core-plugins JavaScript-transform.md
  download_md_file core-plugins JMS-realtimesource.md
  download_md_file core-plugins Joiner-batchjoiner.md '' "analytic"
  download_md_file core-plugins KVTable-batchsink.md
  download_md_file core-plugins KVTable-batchsource.md
  download_md_file core-plugins LogParser-transform.md
  download_md_file core-plugins Projection-transform.md
  download_md_file core-plugins PythonEvaluator-transform.md
  download_md_file core-plugins RowDenormalizer-batchaggregator.md '' "analytic"
  download_md_file core-plugins S3-batchsource.md
  download_md_file core-plugins S3Avro-batchsink.md
  download_md_file core-plugins S3Parquet-batchsink.md
  download_md_file core-plugins ScriptFilter-transform.md
  download_md_file core-plugins SnapshotAvro-batchsink.md
  download_md_file core-plugins SnapshotAvro-batchsource.md
  download_md_file core-plugins SnapshotParquet-batchsink.md
  download_md_file core-plugins SnapshotParquet-batchsource.md
  download_md_file core-plugins SSH-action.md
  download_md_file core-plugins Stream-batchsource.md
  download_md_file core-plugins Stream-realtimesink.md
  download_md_file core-plugins StructuredRecordToGenericRecord-transform.md
  download_md_file core-plugins Table-batchsink.md
  download_md_file core-plugins Table-batchsource.md
  download_md_file core-plugins Table-realtimesink.md
  download_md_file core-plugins TPFSAvro-batchsink.md
  download_md_file core-plugins TPFSAvro-batchsource.md
  download_md_file core-plugins TPFSOrc-batchsink.md
  download_md_file core-plugins TPFSParquet-batchsink.md
  download_md_file core-plugins TPFSParquet-batchsource.md
  download_md_file core-plugins Twitter-realtimesource.md
  download_md_file core-plugins Validator-transform.md
  download_md_file core-plugins Window-windower.md
  download_md_file core-plugins WindowsShareCopy-action.md
  download_md_file core-plugins XMLReader-batchsource.md
  download_md_file database-plugins Database-batchsink.md _includes/database-batchsink-append.md.txt
  download_md_file database-plugins Database-batchsource.md _includes/database-batchsource-append.md.txt
  download_md_file database-plugins Database-action.md
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
  download_md_file solrsearch-plugins SolrSearch-batchsink.md
  download_md_file solrsearch-plugins SolrSearch-realtimesink.md
  download_md_file spark-plugins DecisionTreePredictor-sparkcompute.md '' "analytic"
  download_md_file spark-plugins DecisionTreeTrainer-sparksink.md '' "analytic"
  download_md_file spark-plugins Kafka-streamingsource.md
  download_md_file spark-plugins LogisticRegressionClassifier-sparkcompute.md '' "analytic"
  download_md_file spark-plugins LogisticRegressionTrainer-sparksink.md '' "analytic"
  download_md_file spark-plugins NaiveBayesClassifier-sparkcompute.md '' "analytic"
  download_md_file spark-plugins NaiveBayesTrainer-sparksink.md       '' "analytic" "DPB-naivebayestrainer-sparksink.md" # Currently only for batch
  download_md_file spark-plugins NGramTransform-sparkcompute.md '' "analytic"
  download_md_file spark-plugins Tokenizer-sparkcompute.md '' "analytic"
  download_md_file spark-plugins Twitter-streamingsource.md
  download_md_file transform-plugins CloneRecord-transform.md
  download_md_file transform-plugins Compressor-transform.md
  download_md_file transform-plugins CSVFormatter-transform.md
  download_md_file transform-plugins CSVParser-transform.md
  download_md_file transform-plugins Decoder-transform.md
  download_md_file transform-plugins Decompressor-transform.md
  download_md_file transform-plugins Decryptor-transform.md
  download_md_file transform-plugins Encoder-transform.md
  download_md_file transform-plugins Encryptor-transform.md
  download_md_file transform-plugins Hasher-transform.md
  download_md_file transform-plugins JSONFormatter-transform.md
  download_md_file transform-plugins JSONParser-transform.md
  download_md_file transform-plugins Normalize-transform.md
  download_md_file transform-plugins StreamFormatter-transform.md
  download_md_file transform-plugins ValueMapper-transform.md
  download_md_file transform-plugins XMLParser-transform.md
  download_md_file transform-plugins XMLToJSON-transform.md

  extract_table ${BASE_TARGET} "transforms/validator-transform.md" _includes/validator-extract.txt
}

run_command ${1}

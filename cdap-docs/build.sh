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

# Builds all manuals
# Builds each of these individually, and then packages them into a single zip file for distribution.
# _common directory holds common files and scripts.

# Optional Parameters (passed via env variable or exported in shell):
# BELL: Set it to for the bell function to make a sound when called
# COLOR_LOGS: Set it for color output by Sphinx and these scripts
# NO_JAVADOCS: Set it to not build Javadocs, no matter which actions are used (for testing)

source ./vars
source _common/common-build.sh

ARG_1=${1}
ARG_2=${2}

function usage() {
  echo "Build script for '${PROJECT_CAPS}' docs"
  echo "Usage: ${SCRIPT} <action> [source]"
  echo 
  echo "  Action (select one)"
  echo "    docs-all  Clean build of everything: HTML and Javadocs, GitHub and Web versions, zipped"
  echo 
  echo "    docs-github-only  Clean build of HTML, zipped, with GitHub code, skipping Javadocs"
  echo "    docs-web-only     Clean build of HTML, zipped, with docs.cask.co code, skipping Javadocs"
  echo "    docs-outer        Dirty build of HTML with docs.cask.co code, skipping re-building inner doc, zipping, and Javadocs"
  echo "    docs              Dirty build of HTML with docs.cask.co code, skipping zipping and Javadocs"
  echo 
  echo "    docs-github       Clean build of HTML and Javadocs, zipped, for placing on GitHub"
  echo "    docs-web          Clean build of HTML and Javadocs, zipped, for placing on docs.cask.co webserver"
  echo 
  echo "    clean             Clean up any previous build's target directories"
  echo "    docs-cli          Build CLI input file used in the documentation"
  echo "    javadocs          Build Javadocs used in documentation"
  echo "    javadocs-all      Build Javadocs for all modules"
  echo "    licenses          Clean build of License Dependency PDFs"
  echo "    sdk               Build CDAP SDK (includes the Hydrator plugins if Hydrator plugins source is at"
  echo "                        '${HYDRATOR_PLUGINS_PATH}'"
  echo "                        or the environment variable 'HYDRATOR_PLUGINS_PATH' has been set)"
  echo 
  echo "    version           Print the version information"
  echo 
  echo "  with"
  echo "    source    Path to ${PROJECT} source, if not '${PROJECT_PATH}'"
  echo "              Path is relative to '${SCRIPT_PATH}/../..'"
  echo 
}

function error_usage() {
  if [ $1 ]; then
    echo_red_bold "Unknown action: " $1
  fi
  usage
}

function set_project_path() {
  if [ "x${ARG_2}" == "x" ]; then
    PROJECT_PATH="${SCRIPT_PATH}/.."
  else
    PROJECT_PATH="${SCRIPT_PATH}/../../${ARG_2}"
  fi
  if [[ "x${HYDRATOR_PLUGINS_PATH}" == "x" ]]; then
    HYDRATOR_PLUGINS_PATH="${PROJECT_PATH}/../${HYDRATOR_PLUGINS}"
  fi
}

function set_debug() {
  if [ "x${DEBUG}" == "x" ]; then
    DEBUG="${FALSE}"
  fi
}

function setup() {
  # Check that we're starting in the correct directory
  local quiet={$1}
  E_WRONG_DIRECTORY=85
  if [[ "x${MANUAL}" == "x" || "x${CDAP_DOCS}" == "x" ]]; then
    echo "Manual or CDAP_DOCS set incorrectly: are you in the correct directory?"
    exit ${E_WRONG_DIRECTORY}
  fi
  if [[ " ${MANUALS[@]}" =~ "${MANUAL} " || "${MANUAL}" == "${CDAP_DOCS}" ]]; then
    if [[ "x${quiet}" == "x" ]]; then
      echo "Check for starting directory: Using \"${MANUAL}\""
    fi
    set_project_path
    set_debug
    return 0
  else  
    echo "Did not find MANUAL \"${MANUAL}\": are you in the correct directory?"
    exit ${E_WRONG_DIRECTORY}
  fi
  exit 1
}

function run_command() {
  case ${1} in
    clean )             clean_targets;;
    docs-all )          build_all;;
    docs )              build_docs ${DOCS};;
    docs-outer )        build_docs ${DOCS_OUTER};;
    docs-github-only )  build_docs ${GITHUB_ONLY};;
    docs-web-only )     build_docs ${WEB_ONLY};;
    docs-github )       build_docs ${GITHUB};;
    docs-web )          build_docs ${WEB};;

    docs-first-pass )   build_docs_first_pass ;;
    docs-github-part )  build_docs_github_part;;
    docs-web-part )     build_docs_web_part;;
    
    docs-cli )          build_docs_cli;;
    javadocs )          build_javadocs ${DOCS};;
    javadocs-all )      build_javadocs ${ALL};;
    licenses )          build_license_dependency_pdfs;;
    sdk )               build_standalone;;
    version )           print_version;;
    docs-test )         docs_test;;
    * )                 error_usage ${1};;
  esac
}

function build_all() {
  local warnings
  echo "========================================================"
  echo "========================================================"
  echo "Building All: GitHub and Web Docs"
  echo "--------------------------------------------------------"
  echo
  clean_targets
  clear_messages_set_messages_file
  run_command docs-first-pass ${ARG_2} 
  clear_messages_set_messages_file
  run_command javadocs
  run_command docs-cli
  run_command docs-github-part ${ARG_2} 
  stash_github_zip 
  run_command docs-web-part ${ARG_2} 
  restore_github_zip
  display_version
  display_messages_file
  warnings=$?
  cleanup_messages_file
  echo "--------------------------------------------------------"
  ring_bell "Completed \"build all\""
  echo "========================================================"
  echo "========================================================"
  echo
  exit ${warnings}
}

function build_docs() {
  local warnings
  local doc_type=${1}
  local source_path=${ARG_2}
  local javadocs="${WITHOUT}"
  local cli_docs="${WITHOUT}"
  if [ "${doc_type}" == "${GITHUB}" -o "${doc_type}" == "${WEB}" ]; then
    javadocs="${WITH}"
    cli_docs="${WITH}"
  fi
  if [ "${doc_type}" == "${WEB_ONLY}" -o "${doc_type}" == "${GITHUB_ONLY}" ]; then
    cli_docs="${WITH}"
  fi  
  echo "========================================================"
  echo "========================================================"
  echo "Building \"${doc_type}\""
  echo "(${cli_docs} CLI docs, ${javadocs} Javadocs)"
  echo "--------------------------------------------------------"
  echo
  if [[ "${doc_type}" != "${DOCS}" && "${doc_type}" != "${DOCS_OUTER}" ]]; then
    clean_targets
  fi
  clear_messages_set_messages_file
  if [ "${doc_type}" != "${DOCS_OUTER}" ]; then
    run_command docs-first-pass ${source_path}
  fi
  if [ "${doc_type}" == "${DOCS}" -o "${doc_type}" == "${DOCS_OUTER}" ]; then
    if [ "${javadocs}" != "${WITH}" -o "${cli_docs}" != "${WITH}" ]; then
      check_build_rst
    fi
    build_docs_outer_level ${source_path}
    copy_docs_inner_level
  else
    clear_messages_set_messages_file
  fi
  if [ "${javadocs}" == "${WITH}" -o "${cli_docs}" == "${WITH}" ]; then
    if [ "${javadocs}" == "${WITH}" ]; then
      run_command javadocs
    fi
    if [ "${cli_docs}" == "${WITH}" ]; then
      run_command docs-cli
    fi
  else
    check_build_rst
  fi
  if [ "${doc_type}" == "${GITHUB}" -o "${doc_type}" == "${GITHUB_ONLY}" ]; then
    run_command docs-github-part ${source_path}
  elif [ "${doc_type}" == "${WEB}" -o "${doc_type}" == "${WEB_ONLY}" ]; then
    run_command docs-web-part ${source_path}
  fi
  display_version
  display_messages_file
  warnings=$?
  cleanup_messages_file
  echo "--------------------------------------------------------"
  ring_bell "Completed build of \"${doc_type}\""
  echo "========================================================"
  echo "========================================================"
  echo
  return ${warnings}
}

function build_javadocs() {
  local javadoc_type=${1}
  echo "========================================================"
  echo "Building Javadocs: '${javadoc_type}'"
  echo "--------------------------------------------------------"
  echo
  if [ ${NO_JAVADOCS} ]; then
    echo_red_bold "Javadocs disabled."
  else
    build_javadocs_api ${javadoc_type}
    USING_JAVADOCS="true"
    export USING_JAVADOCS
  fi
  local warnings=$?
  echo "--------------------------------------------------------"
  echo "Completed Build of Javadocs"
  echo "========================================================"
  echo
  return ${warnings}
}

function build_javadocs_api() {
  local javadoc_type=${1}
  set_mvn_environment
  local javadoc_run="mvn javadoc:aggregate -P release"
  if [ "${javadoc_type}" == "${DOCS}" ]; then
    javadoc_run="mvn clean site -P templates"
  fi
  local debug_flag=""
  if [ "${DEBUG}" == "${TRUE}" ]; then
    debug_flag="-X"
  fi
  local start=`date`
  MAVEN_OPTS="-Xmx1024m -XX:MaxPermSize=128m" mvn clean install -P examples,templates,release -DskipTests -Dgpg.skip=true && ${javadoc_run} -DskipTests -DisOffline=false ${debug_flag}
  echo "Javadocs Build Start: ${start}"
  echo "                 End: `date`"
}

function build_docs_cli() {
  echo "========================================================"
  echo "Building CLI Input File for docs"
  echo "--------------------------------------------------------"
  echo
  local warnings
  if [ ${NO_CLI_DOCS} ]; then
    echo_red_bold "Building CLI input file disabled."
  else
    local target_txt=${SCRIPT_PATH}/../cdap-docs-gen/${TARGET}/cdap-docs-cli.txt
    set_version
    set_mvn_environment
    mvn package -pl cdap-docs-gen -am -DskipTests
    warnings=$?
    if [ "${warnings}" == "0" ]; then
      echo "Completed building of CLI"
      java -cp cdap-docs-gen/target/cdap-docs-gen-${PROJECT_VERSION}.jar:cdap-cli/target/cdap-cli-${PROJECT_VERSION}.jar co.cask.cdap.docgen.cli.GenerateCLIDocsTable > ${target_txt}
      warnings=$?
      if [ "${warnings}" == "0" ]; then
        echo "CLI input file written to ${target_txt}"
        USING_CLI_DOCS="true"
      else
        echo "Error building CLI input file: ${warnings}"
      fi
    else
      echo "Error building CLI itself: ${warnings}"
    fi
    export USING_CLI_DOCS
  fi
  echo "--------------------------------------------------------"
  echo "Completed Build of CLI Input File for docs"
  echo "========================================================"
  echo
  return ${warnings}
}

function build_docs_first_pass() {
  echo "========================================================"
  echo "Building First Pass of Docs"
  echo "--------------------------------------------------------"
  echo
  build_docs_inner_level build-docs
  echo "--------------------------------------------------------"
  echo "Completed Building First Pass of Docs"
  echo "========================================================"
  echo
}

function build_docs_github_part() {
  echo "========================================================"
  echo "Building GitHub Docs"
  echo "--------------------------------------------------------"
  echo
  # Don't add the zip_extras (.htaccess files) to Github
  _build_docs build-github ${GOOGLE_TAG_MANAGER_CODE_GITHUB} ${GITHUB}
  return $?
}

function stash_github_zip() {
  echo "========================================================"
  echo "Stashing GitHub Zip"
  echo "========================================================"
  echo
  TARGET_TEMP=$(mktemp -d ${TARGET_PATH}/.cdap-docs-github-$$.XXXX)
  mv ${TARGET_PATH}/*.zip ${TARGET_TEMP}
  echo
}

function restore_github_zip() {
  echo "========================================================"
  echo "Restoring GitHub Zip"
  echo "========================================================"
  echo
  mv ${TARGET_TEMP}/*.zip ${TARGET_PATH}
  rm -rf ${TARGET_TEMP}
  echo
}

function build_docs_web_part() {
  echo "========================================================"
  echo "Building Web Docs"
  echo "--------------------------------------------------------"
  echo
  _build_docs build-web ${GOOGLE_TAG_MANAGER_CODE_WEB} ${WEB} ${TRUE}
  return $?
}

function _build_docs() {
  local doc_target=${1}
  local google_analytics_code=${2}
  local zip_target=${3}
  local zip_extras=${4}
  echo
  echo "========================================================"
  echo "========================================================"
  echo "Building target \"${doc_target}\"..."
  echo "--------------------------------------------------------"
  build_docs_inner_level ${doc_target}
  build_docs_outer_level ${google_analytics_code}
  copy_docs_inner_level
  build_zip ${zip_target}
  zip_extras ${zip_extras}
  echo
  echo "--------------------------------------------------------"
  echo "Building target \"${doc_target}\" completed."
  echo "========================================================"
  echo "========================================================"
  echo
}

function build_docs_inner_level() {
  for i in ${MANUALS}; do
    echo "========================================================"
    echo "Building \"${i}\", target \"${1}\"..."
    echo "--------------------------------------------------------"
    echo
    cd $SCRIPT_PATH/${i}
    ./build.sh ${1} ${ARG_2}
    echo
  done
}

function build_docs_outer_level() {
  local google_code=${1}
  echo "========================================================"
  echo "Building outer-level docs..."
  echo "--------------------------------------------------------"
  echo
  clean_outer_level
  set_version
  
  # Copies placeholder file and renames it
  for i in ${MANUALS}; do
    echo "Copying source for ${i} ..."
    mkdir -p ${TARGET_PATH}/${SOURCE}/${i}
    rewrite ${SCRIPT_PATH}/${COMMON_PLACEHOLDER} ${TARGET_PATH}/${SOURCE}/${i}/index.rst "<placeholder-title>" ${i}
    echo
  done  

  # Build outer-level docs
  cp ${SCRIPT_PATH}/${COMMON_HIGHLEVEL_PY}  ${TARGET_PATH}/${SOURCE}/conf.py
  cp -R ${SCRIPT_PATH}/${COMMON_IMAGES}     ${TARGET_PATH}/${SOURCE}/
  cp ${SCRIPT_PATH}/${COMMON_SOURCE}/*.rst  ${TARGET_PATH}/${SOURCE}/
  
  local google_options
  if [ "x${google_code}" != "x" ]; then
    google_options="-A html_google_tag_manager_code=${google_code}"
  fi
  ${SPHINX_BUILD} -w ${TARGET}/${SPHINX_MESSAGES} ${google_options} ${TARGET_PATH}/${SOURCE} ${TARGET_PATH}/${HTML}
  consolidate_messages
  echo
}
  
function copy_docs_inner_level() {
  echo "========================================================"
  echo "Copying lower-level documentation..."
  echo "--------------------------------------------------------"
  echo

  for i in ${MANUALS}; do
    echo "Copying html for ${i}..."
    rm -rf ${TARGET_PATH}/${HTML}/${i}
    cp -r ${SCRIPT_PATH}/${i}/${TARGET}/${HTML} ${TARGET_PATH}/${HTML}/${i}
    echo
  done

  local project_dir
  # Rewrite 404 file, using branch if not a release
  if [ "x${GIT_BRANCH_TYPE}" == "xfeature" ]; then
    project_dir=${PROJECT_VERSION}-${GIT_BRANCH}
  else
    project_dir=${PROJECT_VERSION}
  fi
  local source_404="${TARGET_PATH}/${HTML}/404.html"
  rewrite ${source_404} "src=\"_static"  "src=\"/cdap/${project_dir}/en/_static"
  rewrite ${source_404} "src=\"_images"  "src=\"/cdap/${project_dir}/en/_images"
  rewrite ${source_404} "/href=\"http/!s|href=\"|href=\"/cdap/${project_dir}/en/|g"
  rewrite ${source_404} "action=\"search.html"  "action=\"/cdap/${project_dir}/en/search.html"
  echo
}

function build_zip() {
  set_project_path
  make_zip ${1}
}

function zip_extras() {
  if [[ "x${1}" == "x" ]]; then
    return
  fi
  echo "Adding .htaccess file (404 file)"
  rewrite ${SCRIPT_PATH}/${COMMON_SOURCE}/${HTACCESS} ${TARGET_PATH}/${PROJECT_VERSION}/.${HTACCESS} "<version>" "${PROJECT_VERSION}"
  cd ${TARGET_PATH}
  zip -qr ${ZIP_DIR_NAME}.zip ${PROJECT_VERSION}/.${HTACCESS}
}

function clean_targets() {
  # Removes all outer- and (sometimes) inner-level build ${TARGET} directories
  rm -rf ${TARGET_PATH}
  mkdir ${TARGET_PATH}
  echo "Cleaned ${TARGET_PATH} directory"
  echo
  if [ "${doc_type}" != "${DOCS_OUTER}" ]; then
    for i in ${MANUALS}; do
      rm -rf ${SCRIPT_PATH}/${i}/${TARGET}/*
      echo "Cleaned ${SCRIPT_PATH}/${i}/${TARGET} directories"
      echo
    done
  fi
}

function clean_outer_level() {
  rm -rf ${TARGET_PATH}/*
  mkdir -p ${TARGET_PATH}/${HTML}
  mkdir -p ${TARGET_PATH}/${SOURCE}
  echo "Cleaned ${TARGET_PATH}/* directories"
  echo
}

function build_license_dependency_pdfs() {
  cd ${SCRIPT_PATH}/reference-manual
  ./build.sh license-pdfs ${ARG_2} 
}

function build_standalone() {
  local add_artifacts=""
  if [ -d ${HYDRATOR_PLUGINS_PATH} ]; then
    build_hydrator_plugins
    local errors=$?
    if [ "${errors}" == "0" ]; then
      add_artifacts="-Dadditional.artifacts.dir=${HYDRATOR_PLUGINS_PATH}"
    fi
  else
    echo "No HYDRATOR_PLUGINS_PATH at ${HYDRATOR_PLUGINS_PATH}"
  fi
  set_mvn_environment
  MAVEN_OPTS="-Xmx1024m -XX:MaxPermSize=128m" mvn clean package \
  -pl cdap-standalone,cdap-app-templates/cdap-etl,cdap-app-templates/cdap-data-quality,cdap-examples \
  -am -amd -DskipTests -P examples,templates,dist,release,unit-tests ${add_artifacts}
}

function build_hydrator_plugins() {
  local errors=0
  set_mvn_environment
  if [ -d ${HYDRATOR_PLUGINS_PATH} ]; then
    cd ${HYDRATOR_PLUGINS_PATH}
    echo "HYDRATOR_PLUGINS_PATH: ${HYDRATOR_PLUGINS_PATH}"
    mvn clean package -DskipTests
    errors=$?
  else
    echo "No HYDRATOR_PLUGINS_PATH at ${HYDRATOR_PLUGINS_PATH}"
  fi
  return ${errors}
}
  
function print_version() {
  cd ${SCRIPT_PATH}/developers-manual
  ./build.sh display-version ${ARG_2} 
}

function ring_bell() {
  # Pass a message as ${1}
  if [[ "x${BELL}" != "x" ]]; then
    echo "$(tput bel)${1}"
  else
    echo "${1}"
  fi
}

function docs_test2() {
  echo_red_bold "Test2..."
  echo "NO_JAVADOCS: ${NO_JAVADOCS}"
  echo "End of Test2 function"
}

function docs_test() {
  echo_red_bold "Test..."
  echo "${WARNING}"
  ring_bell "A test message"
  echo "${RED_BOLD}Red bold${NO_COLOR}text"
  echo "${BOLD}bold${RED}Red${NO_COLOR}text"
  echo "${RED}Red${NO_COLOR}text"
  local recover=$(docs_test2)
  echo "recover: "
  echo "${recover}"
  local last_line=$(echo "${recover}" | tail -n1)
  echo "last_line: ${last_line}"
  echo "Test completed."
}

setup quiet
run_command  ${1}

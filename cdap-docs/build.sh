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
#
# Builds all manuals
# Builds each of these individually, and then packages them into a single zip file for distribution.
# The '_common' directory holds common files and scripts.
#
# Optional Parameters (passed via environment variable or exported in shell):
# BELL: Set for the bell function to make a sound when called
# COLOR_LOGS: Set for color output by Sphinx and these scripts
# USE_LOCAL: Set to use local copies of source, rather than downloading referenced files

cd $(cd $(dirname ${BASH_SOURCE[0]}); pwd -P)
source ./vars
source _common/common-build.sh

function usage() {
  local warnings
  if [[ -n $1 ]]; then
    echo_red_bold "Unknown action: " $1
    warnings=1
    echo
  fi
  echo "Build script for 'cdap' docs"
  echo "Usage: ${SCRIPT} <action>"
  echo 
  echo "  Action (select one)"
  echo
  echo "    docs-set          Clean build of HTML, CLI, and Javadocs, zipped, ready for deploying"
  echo "    docs-all          alias to \"docs-set\""
  echo "    docs-web-only     Clean build of HTML, CLI, zipped, skipping Javadocs"
  echo 
  echo "    docs              Dirty build of HTML, skipping CLI, Javadocs, or zipping"
  echo 
  echo "    clean             Clean up any previous build's target directories"
  echo "    docs-cli          Build CLI input file for documentation"
  echo "    docs-package      Package (zip up) docs"
  echo "    javadocs          Build Javadocs for documentation"
  echo "    javadocs-all      Build Javadocs for all modules"
  echo "    licenses          Clean build of License Dependency PDFs"
  echo "    version           Print the version information"
  echo 
  return ${warnings}
}

function run_command() {
  case ${1} in
    clean )             clean_targets;;
    docs )              build_docs_only; warnings=$?;;
    docs-all )          build_docs_set; warnings=$?;;
    docs-cli )          build_docs_cli;;
    docs-first-pass )   build_docs_first_pass;;
    docs-second-pass )  build_docs_second_pass;;
    docs-package )      build_docs_package;;
    docs-set )          build_docs_set; warnings=$?;;
    docs-web-only )     build_docs_web_only; warnings=$?;;
    javadocs )          build_javadocs docs;;
    javadocs-all )      build_javadocs all;;
    licenses )          build_license_dependency_pdfs;;
    version )           print_version;;
    * )                 usage ${1}; warnings=$?;;
  esac
  return ${warnings}
}

function display_start_title() {
  echo "========================================================"
  echo "========================================================"
  echo "${1}"
  echo "--------------------------------------------------------"
  echo
}
  
function display_end_title() {
  echo "--------------------------------------------------------"
  echo "Completed \"${1}\""
  echo "========================================================"
  echo "========================================================"
  echo
}

function display_end_title_bell() {
  echo "--------------------------------------------------------"
  ring_bell "Completed \"${1}\""
  echo "========================================================"
  echo "========================================================"
  echo
}

function build_docs_set() {
  _build_docs docs_set "Building Docs Set: docs_set"
}

function build_docs_only() {
  _build_docs docs_only "Building Docs Only: docs_only"
}

function build_docs_web_only() {
  _build_docs docs_web_only "Building Docs, CLI, and Zip: docs_web_only"
}

function _build_docs() {
  local warnings
  local type=${1}
  local title=${2}
  display_start_title "${title}"

  clear_messages_file
  if [[ ${type} == "docs_set" ]] || [[ ${type} == "docs_web_only" ]]; then
    clean_targets
    build_docs_first_pass
    clear_messages_file
    if [[ ${type} == "docs_set" ]]; then
      build_javadocs docs
    fi
    build_docs_cli
  fi
  build_docs_second_pass
  
  if [[ ${type} == "docs_set" ]] || [[ ${type} == "docs_web_only" ]]; then
    build_docs_package
  fi
  
  set_and_display_version
  display_messages
  warnings=$?
  cleanup_messages_file
  display_end_title_bell "${title}"
  return ${warnings}
}

function build_docs_first_pass() {
  local title="Building Docs First Pass"
  display_start_title "${title}"

  build_docs_inner_level build-docs
  
  display_end_title ${title}
}

function build_docs_second_pass() {
  local title="Building Docs Second Pass"
  display_start_title "${title}"

  build_docs_inner_level build-web
  build_docs_outer_level ${GOOGLE_TAG_MANAGER_CODE}
  copy_docs_inner_level

  display_end_title ${title}
}

function build_javadocs() {
  # Used by reference manual to know to copy the javadocs
  USING_JAVADOCS="true"
  export USING_JAVADOCS
  local javadoc_type=${1}
  local title="Building Javadocs: '${javadoc_type}'"
  display_start_title "${title}"
  local warnings
  check_build_rst
  set_environment    
  if [[ ${DEBUG} == ${TRUE} ]]; then
    local debug_flag="-X"
  else
    local debug_flag=''
  fi
  if [[ ${javadoc_type} == ${ALL} ]]; then
    javadoc_run="javadoc:aggregate"
  else
    javadoc_run="site"
  fi
  local start=`date`
  cd ${PROJECT_PATH}
  MAVEN_OPTS="-Xmx4g -XX:MaxPermSize=256m" # match other CDAP builds
  mvn clean package ${javadoc_run} -P examples,templates,release -DskipTests -Dgpg.skip=true -DisOffline=false ${debug_flag}
  warnings=$?
  if [[ ${warnings} -eq 0 ]]; then
    echo
    echo "Javadocs Build Start: ${start}"
    echo "                 End: `date`"
  else
    echo "Error building Javadocs: ${warnings}"
  fi
  display_end_title ${title}
  return ${warnings}
}

function build_docs_cli() {
  local title="Building CLI Input File for docs"
  display_start_title "${title}"
  local warnings
  if [[ -n ${NO_CLI_DOCS} ]]; then
    echo_red_bold "Building CLI input file disabled. '${NO_CLI_DOCS}'"
  else
    local target_txt=${SCRIPT_PATH}/../cdap-docs-gen/${TARGET}/cdap-docs-cli.txt
    set_version
    check_build_rst
    set_environment
    cd ${PROJECT_PATH}
    mvn package -pl cdap-docs-gen -am -DskipTests
    warnings=$?
    if [[ ${warnings} -eq 0 ]]; then
      ${JAVA} -cp cdap-docs-gen/target/cdap-docs-gen-${PROJECT_VERSION}.jar:cdap-cli/target/cdap-cli-${PROJECT_VERSION}.jar co.cask.cdap.docgen.cli.GenerateCLIDocsTable > ${target_txt}
      warnings=$?
      echo
      echo "Completed building of CLI"
      if [[ ${warnings} -eq 0 ]]; then
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
  display_end_title ${title}
  return ${warnings}
}

function build_docs_inner_level() {
# Change to each manual, and run the local ./build.sh from there.
# Each manual can (and does) have a customised build script, using the common-build.sh as a base.
  for i in ${MANUALS}; do
    echo "========================================================"
    echo "Building \"${i}\", target \"${1}\"..."
    echo "--------------------------------------------------------"
    echo
    cd $SCRIPT_PATH/${i}
    ./build.sh ${1}
    echo
  done
}

function build_docs_outer_level() {
  local google_options
  if [[ -n ${1} ]]; then
    google_options="-A html_google_tag_manager_code=${1}"
  fi
  local title="Building outer-level docs...tag code ${1}"
  display_start_title "${title}"
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
  
  ${SPHINX_BUILD} -w ${TARGET}/${SPHINX_MESSAGES} ${google_options} ${TARGET_PATH}/${SOURCE} ${TARGET_PATH}/${HTML}
  consolidate_messages
  echo
}
 
function copy_docs_inner_level() {
  local title="Copying lower-level documentation"
  display_start_title "${title}"
  for i in ${MANUALS}; do
    echo "Copying html for ${i}..."
    rm -rf ${TARGET_PATH}/${HTML}/${i}
    cp -r ${SCRIPT_PATH}/${i}/${TARGET}/${HTML} ${TARGET_PATH}/${HTML}/${i}
    echo
  done

  local project_dir
  # Rewrite 404 file, using branch if not a release
  if [[ ${GIT_BRANCH_TYPE} == "feature" ]]; then
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

function build_docs_package() {
  local title="Packaging docs"
  display_start_title "${title}"
  set_project_path
  set_version
  echo "Set project path and version"
  local zip_dir_name="${PROJECT}-docs-${PROJECT_VERSION}-web"
  cd ${TARGET_PATH}
  docs_change_py="${SCRIPT_PATH}/tools/docs-change.py"
  echo "Removing old directories and zips"
  rm -rf ${PROJECT_VERSION} *.zip
  echo "Creating ${PROJECT_VERSION}"
  mkdir ${PROJECT_VERSION} && cp -r html ${PROJECT_VERSION}/en
  errors=$?
  if [[ ${errors} -ne 0 ]]; then
      echo "Could not create ${PROJECT_VERSION}"
      return ${errors}   
  fi
  echo "Adding a redirect index.html file"
  cp ${SCRIPT_PATH}/${COMMON_SOURCE}/redirect.html ${PROJECT_VERSION}/index.html
  errors=$?
  if [[ ${errors} -ne 0 ]]; then
      echo "Could not add redirect file"
      return ${errors}   
  fi
  echo "Adding .htaccess file (404 file)"
  rewrite ${SCRIPT_PATH}/${COMMON_SOURCE}/htaccess ${TARGET_PATH}/${PROJECT_VERSION}/.htaccess "<version>" "${PROJECT_VERSION}"
  errors=$?
  if [[ ${errors} -ne 0 ]]; then
      echo "Could not create a .htaccess file"
      return ${errors}   
  fi
  echo "Canonical numbered version ${zip_dir_name}"
  python ${docs_change_py} ${TARGET_PATH}/${PROJECT_VERSION}
  errors=$?
  if [[ ${errors} -ne 0 ]]; then
      echo "Could not change doc set ${TARGET_PATH}/${PROJECT_VERSION}"
      return ${errors}   
  fi
  echo "Creating zip ${zip_dir_name}"
  zip -qr ${zip_dir_name}.zip ${PROJECT_VERSION}/* ${PROJECT_VERSION}/.htaccess --exclude *.DS_Store* *.buildinfo*
  errors=$?
  if [[ ${errors} -ne 0 ]]; then
      echo "Could not create zipped doc set ${TARGET_PATH}/${PROJECT_VERSION}"
      return ${errors}   
  fi
  display_end_title ${title}
}

function clean_targets() {
  # Removes all outer- and (sometimes) inner-level build ${TARGET} directories
  rm -rf ${TARGET_PATH}
  mkdir ${TARGET_PATH}
  echo "Cleaned ${TARGET_PATH} directory"
  echo
  if [[ ${doc_type} != ${DOCS_OUTER} ]]; then
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
  ./build.sh license-pdfs
}
  
function print_version() {
  cd ${SCRIPT_PATH}/developers-manual
  ./build.sh display-version
}

function set_and_display_version() {
  set_version
  display_version
}

function set_project_path() {
  PROJECT_PATH="${SCRIPT_PATH}/.."
  PROJECT_PATH=$(cd ${PROJECT_PATH} && pwd -P)
  SOURCE_PATH="${SCRIPT_PATH}/../../"
  SOURCE_PATH=$(cd ${SOURCE_PATH} && pwd -P)
}

function setup() {
  # Check that we're starting in the correct directory
  local quiet={$1}
  E_WRONG_DIRECTORY=85
  if [[ -z ${MANUAL} ]] || [[ -z ${CDAP_DOCS} ]]; then
    echo "Manual or CDAP_DOCS set incorrectly: are you in the correct directory?"
    exit ${E_WRONG_DIRECTORY}
  fi
  if [[ " ${MANUALS[@]}" =~ "${MANUAL} " || ${MANUAL} == ${CDAP_DOCS} ]]; then
    if [[ -z ${quiet} ]]; then
      echo "Check for starting directory: Using \"${MANUAL}\""
    fi
    set_project_path
    if [[ -z ${DEBUG} ]]; then
      DEBUG="${FALSE}"
    fi
    return 0
  else  
    echo "Did not find MANUAL \"${MANUAL}\": are you in the correct directory?"
    exit ${E_WRONG_DIRECTORY}
  fi
  return 1
}

setup quiet
if [[ $? -ne 0 ]]; then
    exit $?   
fi
run_command ${1}

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

# Builds all manuals
# Builds each of these individually, and then packages them into a single zip file for distribution.
# _common directory holds common files and scripts.

# Optional Parameter (passed via Bamboo env variable or exported in shell)
# BELL (set it to either 'yes' or 'true', if you want the bell function to make a sound when called)

source ./vars
source _common/common-build.sh

ARG_1="${1}"
ARG_2="${2}"
ARG_3="${3}"

function set_project_path() {
  if [ "x${ARG_2}" == "x" ]; then
    PROJECT_PATH="${SCRIPT_PATH}/../"
  else
    PROJECT_PATH="${SCRIPT_PATH}/../../${ARG_2}"
  fi
}

function check_starting_directory() {
  E_WRONG_DIRECTORY=85

  if [[ "x${MANUAL}" == "x" || "x${CDAP_DOCS}" == "x" ]]; then
    echo "Manual or CDAP_DOCS set incorrectly: are you in the correct directory?"
    exit ${E_WRONG_DIRECTORY}
  fi
  
  if [[ " ${MANUALS[@]}" =~ "${MANUAL} " || "${MANUAL}" == "${CDAP_DOCS}" ]]; then
    echo "Using \"${MANUAL}\""
    return 0
  else  
    echo "Did not find MANUAL \"${MANUAL}\": are you in the correct directory?"
    exit ${E_WRONG_DIRECTORY}
  fi
  
  exit 1
}

function usage() {
  cd ${PROJECT_PATH}
  PROJECT_PATH=`pwd`
  echo "Build script for '${PROJECT_CAPS}' docs"
  echo "Usage: ${SCRIPT} < option > [source test_includes]"
  echo ""
  echo "  Options (select one)"
  echo "    all            Clean build of everything: HTML docs and Javadocs, GitHub and Web versions"
  echo ""
  echo "    doc            Clean build of just the outer level HTML docs, skipping Javadocs and zipping"
  echo "    docs           Clean build of just the HTML docs, skipping Javadocs, zipped for placing on docs.cask.co webserver"
  echo "    docs-github    Clean build of HTML docs and Javadocs, zipped for placing on GitHub"
  echo "    docs-web       Clean build of HTML docs and Javadocs, zipped for placing on docs.cask.co webserver"
  echo ""
  echo "    javadocs       Build Javadocs"
  echo "    json           Build JSON file (json-versions.js)"
  echo "    print-json     Prints what would be the JSON file (json-versions.js)"
  echo "    licenses       Clean build of License Dependency PDFs"
  echo "    sdk            Build SDK"
  echo "    version        Print the version information"
  echo ""
  echo "    clean          Clean up (previous builds)"
  echo ""
  echo "  with"
  echo "    source         Path to ${PROJECT} source, if not ${PROJECT_PATH}"
  echo "    test_includes  local, remote or neither (default: remote); must specify source if used"
  echo ""
}

function run_command() {
  case "${1}" in
    all )               build_all;;
    clean )             clean_builds;;
    doc )               build_docs_outer_level;;
    docs )              build_docs;;
    docs-github-part )  build_docs_github ${ARG_2} ${ARG_3};;
    docs-github )       build_docs_github ${ARG_2} ${ARG_3};;
    docs-web-part )     build_docs_web ${ARG_2} ${ARG_3};;
    docs-web )          build_docs_web ${ARG_2} ${ARG_3}; exit 0;;
    javadocs )          build_javadocs; exit 0;;
    json )              build_json; exit 0;;
    licenses )          build_license_depends; exit 0;;
    print-json )        print_json; exit 0;;
    sdk )               build_sdk; exit 0;;
    version )           print_version; exit 0;;
    test )              test; exit 0;;
    * )                 usage; exit 0;;
  esac
}

function clean() {
  cd ${SCRIPT_PATH}
  rm -rf ${SCRIPT_PATH}/${BUILD}/*
  mkdir -p ${SCRIPT_PATH}/${BUILD}/${HTML}
  mkdir -p ${SCRIPT_PATH}/${BUILD}/${SOURCE}
  echo "Cleaned ${BUILD} directory"
  echo ""
}

function copy_source() {
  echo "Copying source for ${1} (${2}) ..."
  cd ${SCRIPT_PATH}
  mkdir -p ${SCRIPT_PATH}/${BUILD}/${SOURCE}/${1}
  rewrite ${COMMON_PLACEHOLDER} ${BUILD}/${SOURCE}/${1}/index.rst "<placeholder>" "${2}"
  echo ""
}

function copy_html() {
  echo "Copying html for ${1}..."
  cd ${SCRIPT_PATH}
  rm -rf ${SCRIPT_PATH}/${BUILD}/${HTML}/${1}
  cp -r ${1}/${BUILD}/${HTML} ${BUILD}/${HTML}/${1}
  echo ""
}

function build_docs_outer_level() {
  echo ""
  echo "========================================================"
  echo "Building outer-level docs..."
  echo "--------------------------------------------------------"
  echo ""
  clean
  version
  
  # Copies placeholder file and renames it
  copy_source introduction          "Introduction"
  copy_source developers-manual     "Developers’ Manual"
  copy_source application-templates "Application Templates"
  copy_source admin-manual          "Administration Manual"
  copy_source integrations          "Integrations"
  copy_source examples-manual       "Examples, Guides, and Tutorials"
  copy_source reference-manual      "Reference Manual"

  # Build outer-level docs
  cd ${SCRIPT_PATH}
  cp ${COMMON_HIGHLEVEL_PY}  ${BUILD}/${SOURCE}/conf.py
  cp -R ${COMMON_IMAGES}     ${BUILD}/${SOURCE}/
  cp ${COMMON_SOURCE}/*.rst  ${BUILD}/${SOURCE}/
  
  if [ "x${1}" == "x" ]; then
    sphinx-build -b html -d build/doctrees build/source build/html
  else
    sphinx-build -D googleanalytics_id=${1} -D googleanalytics_enabled=1 -b html -d build/doctrees build/source build/html
  fi
}
  
function copy_docs_lower_level() {
  echo ""
  echo "========================================================"
  echo "Copying lower-level documentation..."
  echo "--------------------------------------------------------"
  echo ""

  for i in ${MANUALS}; do
    copy_html ${i}
  done

  local project_dir
  # Rewrite 404 file, using branch if not a release
  if [ "x${GIT_BRANCH_TYPE}" == "xfeature" ]; then
    project_dir=${PROJECT_VERSION}-${GIT_BRANCH}
  else
    project_dir=${PROJECT_VERSION}
  fi
  rewrite ${BUILD}/${HTML}/404.html "src=\"_static"  "src=\"/cdap/${project_dir}/en/_static"
  rewrite ${BUILD}/${HTML}/404.html "src=\"_images"  "src=\"/cdap/${project_dir}/en/_images"
  rewrite ${BUILD}/${HTML}/404.html "/href=\"http/!s|href=\"|href=\"/cdap/${project_dir}/en/|g"
  rewrite ${BUILD}/${HTML}/404.html "action=\"search.html"  "action=\"/cdap/${project_dir}/en/search.html"
}

################################################## current

function build_all() {
  echo ""
  echo "========================================================"
  echo "========================================================"
  echo "Building All: GitHub and Web Docs."
  echo "--------------------------------------------------------"
  echo ""
  echo "========================================================"
  echo "Building GitHub Docs."
  echo "--------------------------------------------------------"
  echo ""
  run_command docs-github-part ${ARG_2} ${ARG_3}
  echo "Stashing GitHub Docs."
  cd ${SCRIPT_PATH}
  mkdir -p ${SCRIPT_PATH}/${BUILD_TEMP}
  mv ${SCRIPT_PATH}/${BUILD}/*.zip ${SCRIPT_PATH}/${BUILD_TEMP}
  echo ""
  echo "========================================================"
  echo "Building Web Docs."
  echo "--------------------------------------------------------"
  echo ""
  run_command docs-web-part ${ARG_2} ${ARG_3}
  echo ""
  echo "========================================================"
  echo "Replacing GitHub Docs."
  echo "--------------------------------------------------------"
  echo ""
  mv ${SCRIPT_PATH}/${BUILD_TEMP}/*.zip ${SCRIPT_PATH}/${BUILD}
  rm -rf ${SCRIPT_PATH}/${BUILD_TEMP}
  echo ""
  echo "--------------------------------------------------------"
  bell "Completed \"build_all\"."
  echo "========================================================"
  echo "========================================================"
  echo ""
  exit 0
}

function build_javadocs() {
  # Uses function from common
  build_javadocs_api
}

function build_docs_javadocs() {
  build_docs_inner_level "build"
}


function build_json() {
  version
  if [ -d ${SCRIPT_PATH}/${BUILD}/${SOURCE} ]; then
    cd ${SCRIPT_PATH}/${BUILD}/${SOURCE}
    JSON_FILE=`python -c 'import conf; conf.print_json_versions_file();'`
    local json_file_path=${SCRIPT_PATH}/${BUILD}/${PROJECT_VERSION}/${JSON_FILE}
    python -c 'import conf; conf.print_json_versions();' > ${json_file_path}
  else
    echo "Could not find '${SCRIPT_PATH}/${BUILD}/${SOURCE}'; can not build JSON file"
  fi
}

function print_json() {
  version
  if [ -d ${SCRIPT_PATH}/${BUILD}/${SOURCE} ]; then
    cd ${SCRIPT_PATH}/${BUILD}/${SOURCE}
    python -c 'import conf; conf.pretty_print_json_versions();'
  else
    echo "Could not find '${SCRIPT_PATH}/${BUILD}/${SOURCE}'; can not print JSON file"
  fi
}

function build_docs() {
  _build_docs "docs" ${GOOGLE_ANALYTICS_WEB} ${WEB} ${TRUE}
  return $?
}

function build_docs_github() {
  _build_docs "build-github" ${GOOGLE_ANALYTICS_GITHUB} ${GITHUB} ${FALSE}
  return $?
}

function build_docs_web() {
  _build_docs "build-web" ${GOOGLE_ANALYTICS_WEB} ${WEB} ${TRUE}
  return $?
}

function _build_docs() {
  echo ""
  echo "========================================================"
  echo "========================================================"
  echo "Building target \"${1}\"..."
  echo "--------------------------------------------------------"
  clear_messages
  build_docs_inner_level ${1}
  build_docs_outer_level ${2}
  copy_docs_lower_level
  build_zip ${3}
  zip_extras ${4}
  display_version
  display_messages_file
  local warnings="$?"
  cleanup_messages_file
  echo ""
  echo "--------------------------------------------------------"
  bell "Building target \"${1}\" completed."
  echo "========================================================"
  echo "========================================================"
  echo ""
  return ${warnings}
}

function build_docs_inner_level() {
  for i in ${MANUALS}; do
    build_specific_doc ${i} ${1}
  done
}

function build_specific_doc() {
  echo ""
  echo "========================================================"
  echo "Building \"${1}\", target \"${2}\"..."
  echo "--------------------------------------------------------"
  echo ""
  cd $SCRIPT_PATH/${1}
  ./build.sh ${2} ${ARG_2} ${ARG_3}
}

function build_zip() {
  cd $SCRIPT_PATH
  set_project_path
  make_zip ${1}
}

function zip_extras() {
  if [ "x${1}" == "x${FALSE}" ]; then
    return
  fi
  # Add JSON file
  build_json
  # Add .htaccess file (404 file)
  cd ${SCRIPT_PATH}
  rewrite ${COMMON_SOURCE}/${HTACCESS} ${BUILD}/${PROJECT_VERSION}/.${HTACCESS} "<version>" "${PROJECT_VERSION}"
  cd ${SCRIPT_PATH}/${BUILD}
  zip -qr ${ZIP_DIR_NAME}.zip ${PROJECT_VERSION}/${JSON_FILE} ${PROJECT_VERSION}/.${HTACCESS}
}

function build_sdk() {
  cd developers-manual
  ./build.sh sdk ${ARG_2} ${ARG_3}
}

function build_license_depends() {
  cd reference-manual
  ./build.sh license-pdfs ${ARG_2} ${ARG_3}
}

function print_version() {
  cd developers-manual
  ./build.sh version ${ARG_2} ${ARG_3}
}

function bell() {
  # Pass a message as ${1}
  if [[ "x${BELL}" == "xyes" || "x${BELL}" == "x${TRUE}" ]]; then
    echo -e "\a${1}"
  else
    echo -e "${1}"
  fi
}

function test() {
  echo "Test..."
  bell "A test message"
  echo "Test completed."
}

function clean_builds() {
  # Removes all upper- and lower-level build directories

  echo ""
  rm -rf ${SCRIPT_PATH}/${BUILD}/*
  echo "Cleaned ${SCRIPT_PATH}/${BUILD} directory"

  echo ""
  for i in ${MANUALS}; do
    rm -rf ${SCRIPT_PATH}/${i}/${BUILD}/*
    echo "Cleaned ${SCRIPT_PATH}/${i}/${BUILD} directory"
    echo ""
  done
}

check_starting_directory

set_project_path

run_command  ${1}

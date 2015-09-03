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

function usage() {
  cd ${PROJECT_PATH}
  PROJECT_PATH=`pwd`
  echo "Build script for '${PROJECT_CAPS}' docs"
  echo "Usage: ${SCRIPT} <option> [source]"
  echo ""
  echo "  Option (select one)"
  echo "    docs-all  Clean build of everything: HTML and Javadocs, GitHub and Web versions, zipped"
  echo ""
  echo "    docs-github-only  Clean build of HTML, zipped, with GitHub code, skipping Javadocs"
  echo "    docs-web-only     Clean build of HTML, zipped, with docs.cask.co code, skipping Javadocs"
  echo "    docs              Dirty build of HTML with docs.cask.co code, skipping zipping and Javadocs"
  echo ""
  echo "    docs-github  Clean build of HTML and Javadocs, zipped, for placing on GitHub"
  echo "    docs-web     Clean build of HTML and Javadocs, zipped, for placing on docs.cask.co webserver"
  echo ""
  echo "    clean     Clean up any previous build's target directories"
  echo "    javadocs  Build Javadocs"
  echo "    licenses  Clean build of License Dependency PDFs"
  echo "    sdk       Build CDAP SDK"
  echo "    version   Print the version information"
  echo ""
  echo "  with"
  echo "    source    Path to ${PROJECT} source, if not '${PROJECT_PATH}'"
  echo "              Path is relative to '${SCRIPT_PATH}/../..'"
  echo ""
}

function set_project_path() {
  if [ "x${ARG_2}" == "x" ]; then
    PROJECT_PATH="${SCRIPT_PATH}/../"
  else
    PROJECT_PATH="${SCRIPT_PATH}/../../${ARG_2}"
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
    return 0
  else  
    echo "Did not find MANUAL \"${MANUAL}\": are you in the correct directory?"
    exit ${E_WRONG_DIRECTORY}
  fi
  exit 1
}

function run_command() {
  case "${1}" in
    docs-all )          build_all;;
    docs )              build_docs ${DOCS};;
    docs-github-only )  build_docs ${GITHUB_ONLY};;
    docs-web-only )     build_docs ${WEB_ONLY};;
    docs-github )       build_docs ${GITHUB};;
    docs-web )          build_docs ${WEB};;

    docs-first-pass )   build_docs_first_pass ;;
    docs-github-part )  build_docs_github_part;;
    docs-web-part )     build_docs_web_part;;
    
    clean )             clean_all_targets;;
    javadocs )          build_javadocs;;
    licenses )          build_license_dependency_pdfs;;
    sdk )               build_standalone;;
    version )           print_version;;
    doc-test )          doc_test;;
    * )                 usage;;
    doc )               build_docs_outer_level;;
  esac
}

function build_all() {
  echo "========================================================"
  echo "========================================================"
  echo "Building All: GitHub and Web Docs"
  echo "--------------------------------------------------------"
  echo ""
  clean_all_targets
  run_command javadocs 
  run_command docs-first-pass ${ARG_2} 
  run_command docs-github-part ${ARG_2} 
  _stash_github_zip 
  run_command docs-web-part ${ARG_2} 
  _restore_github_zip
  echo "--------------------------------------------------------"
  _bell "Completed \"build all\""
  echo "========================================================"
  echo "========================================================"
  echo ""
  exit 0
}

function build_docs() {
  local doc_type=${1}
  local source_path=${ARG_2}
  local javadocs="${WITHOUT}"
  if [ "${doc_type}" == "${GITHUB}" -o "${doc_type}" == "${WEB}" ]; then
    javadocs="${WITH}"
  fi
  echo "========================================================"
  echo "========================================================"
  echo "Building \"${doc_type}\" (${javadocs} Javadocs)"
  echo "--------------------------------------------------------"
  echo ""
  if [ "${doc_type}" != "${DOCS}" ]; then
    clean_all_targets
  fi
  run_command docs-first-pass ${source_path}
  if [ "${doc_type}" == "${DOCS}" ]; then
    build_docs_outer_level ${source_path} 
    copy_docs_inner_level
  fi
  local warnings="$?"
  if [ "${warnings}" != "0" ]; then
    return ${warnings}
  fi
  if [ "${javadocs}" == "${WITH}" ]; then
    run_command javadocs
  fi
  if [ "${doc_type}" == "${GITHUB}" -o "${doc_type}" == "${GITHUB_ONLY}" ]; then
    run_command docs-github-part ${source_path} 
  elif [ "${doc_type}" == "${WEB}" -o "${doc_type}" == "${WEB_ONLY}" ]; then
    run_command docs-web-part ${source_path} 
  fi
  echo "--------------------------------------------------------"
  _bell "Completed build of \"${doc_type}\""
  echo "========================================================"
  echo "========================================================"
  echo ""
  exit 0
}

function build_javadocs() {
  echo "========================================================"
  echo "Building Javadocs"
  echo "--------------------------------------------------------"
  echo ""
  build_javadocs_api
  echo "--------------------------------------------------------"
  echo "Completed Build of Javadocs"
  echo "========================================================"
  echo ""
}

function build_javadocs_api() {
  set_mvn_environment
  MAVEN_OPTS="-Xmx1024m -XX:MaxPermSize=128m" mvn clean install -P examples,templates,release -DskipTests -Dgpg.skip=true && mvn clean site -DskipTests -P templates -DisOffline=false
}

function build_docs_first_pass() {
  echo "========================================================"
  echo "Building First Pass of Docs"
  echo "--------------------------------------------------------"
  echo ""
  build_docs_inner_level "docs"
  echo "--------------------------------------------------------"
  echo "Completed Building First Pass of Docs"
  echo "========================================================"
  echo ""
  return $?
}

function build_docs_github_part() {
  echo "========================================================"
  echo "Building GitHub Docs"
  echo "--------------------------------------------------------"
  echo ""
  _build_docs "build-github" ${GOOGLE_ANALYTICS_GITHUB} ${GITHUB} ${FALSE}
  return $?
}

function _stash_github_zip() {
  echo "========================================================"
  echo "Stashing GitHub Zip"
  echo "========================================================"
  echo ""
  cd ${SCRIPT_PATH}
  TARGET_TEMP=$(mktemp -d /tmp/cdap-docs.XXXX)
  mv ${SCRIPT_PATH}/${TARGET}/*.zip ${TARGET_TEMP}
  echo ""
}

function _restore_github_zip() {
  echo "========================================================"
  echo "Restoring GitHub Zip"
  echo "========================================================"
  echo ""
  mv ${TARGET_TEMP}/*.zip ${SCRIPT_PATH}/${TARGET}
  rm -rf ${TARGET_TEMP}
  echo ""
}

function build_docs_web_part() {
  echo "========================================================"
  echo "Building Web Docs"
  echo "--------------------------------------------------------"
  echo ""
  _build_docs "build-web" ${GOOGLE_ANALYTICS_WEB} ${WEB} ${FALSE}
  return $?
}

function _build_docs() {
  local doc_target=${1}
  local google_analytics_code=${2}
  local zip_target=${3}
  local zip_extras=${4}
  echo ""
  echo "========================================================"
  echo "========================================================"
  echo "Building target \"${doc_target}\"..."
  echo "--------------------------------------------------------"
  clear_messages_set_messages_file
  build_docs_inner_level ${doc_target}
  build_docs_outer_level ${google_analytics_code}
  copy_docs_inner_level
  _build_zip ${zip_target}
  _zip_extras ${zip_extras}
  echo ""
  display_version
  display_messages_file
  local warnings="$?"
  cleanup_messages_file
  echo ""
  echo "--------------------------------------------------------"
  _bell "Building target \"${doc_target}\" completed."
  echo "========================================================"
  echo "========================================================"
  echo ""
  return ${warnings}
}

function build_docs_inner_level() {
  for i in ${MANUALS}; do
    echo "========================================================"
    echo "Building \"${i}\", target \"${1}\"..."
    echo "--------------------------------------------------------"
    echo ""
    cd $SCRIPT_PATH/${i}
    ./build.sh ${1} ${ARG_2}
    echo ""
  done
}

function copy_source() {
  echo "Copying source for ${1} (${2}) ..."
  cd ${SCRIPT_PATH}
  mkdir -p ${SCRIPT_PATH}/${TARGET}/${SOURCE}/${1}
  rewrite ${COMMON_PLACEHOLDER} ${TARGET}/${SOURCE}/${1}/index.rst "<placeholder>" "${2}"
  echo ""
}

function build_docs_outer_level() {
  local google_code=${1}
  echo "========================================================"
  echo "Building outer-level docs..."
  echo "--------------------------------------------------------"
  echo ""
  clean_outer_level
  set_version
  
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
  cp ${COMMON_HIGHLEVEL_PY}  ${TARGET}/${SOURCE}/conf.py
  cp -R ${COMMON_IMAGES}     ${TARGET}/${SOURCE}/
  cp ${COMMON_SOURCE}/*.rst  ${TARGET}/${SOURCE}/
  
  local google_options
  if [ "x${google_code}" != "x" ]; then
    google_options="-D googleanalytics_id=${google_code} -D googleanalytics_enabled=1"
  fi
  ${SPHINX_BUILD} ${google_options} ${TARGET}/${SOURCE} ${TARGET}/html
  echo ""
}
  
function copy_docs_inner_level() {
  echo "========================================================"
  echo "Copying lower-level documentation..."
  echo "--------------------------------------------------------"
  echo ""

  for i in ${MANUALS}; do
    echo "Copying html for ${i}..."
    cd ${SCRIPT_PATH}
    rm -rf ${SCRIPT_PATH}/${TARGET}/${HTML}/${i}
    cp -r ${i}/${TARGET}/${HTML} ${TARGET}/${HTML}/${i}
    echo ""
  done

  local project_dir
  # Rewrite 404 file, using branch if not a release
  if [ "x${GIT_BRANCH_TYPE}" == "xfeature" ]; then
    project_dir=${PROJECT_VERSION}-${GIT_BRANCH}
  else
    project_dir=${PROJECT_VERSION}
  fi
  local source_404="${TARGET}/${HTML}/404.html"
  rewrite ${source_404} "src=\"_static"  "src=\"/cdap/${project_dir}/en/_static"
  rewrite ${source_404} "src=\"_images"  "src=\"/cdap/${project_dir}/en/_images"
  rewrite ${source_404} "/href=\"http/!s|href=\"|href=\"/cdap/${project_dir}/en/|g"
  rewrite ${source_404} "action=\"search.html"  "action=\"/cdap/${project_dir}/en/search.html"
  echo ""
}

function _build_zip() {
  cd $SCRIPT_PATH
  set_project_path
  make_zip ${1}
}

function _zip_extras() {
  if [ "x${1}" == "x${FALSE}" ]; then
    return
  fi
  # Add .htaccess file (404 file)
  cd ${SCRIPT_PATH}
  rewrite ${COMMON_SOURCE}/${HTACCESS} ${TARGET}/${PROJECT_VERSION}/.${HTACCESS} "<version>" "${PROJECT_VERSION}"
  cd ${SCRIPT_PATH}/${TARGET}
  zip -qr ${ZIP_DIR_NAME}.zip ${PROJECT_VERSION}/.${HTACCESS}
}

function clean_outer_level() {
  cd ${SCRIPT_PATH}
  rm -rf ${SCRIPT_PATH}/${TARGET}/*
  mkdir -p ${SCRIPT_PATH}/${TARGET}/${HTML}
  mkdir -p ${SCRIPT_PATH}/${TARGET}/${SOURCE}
  echo "Cleaned ${SCRIPT_PATH}/${TARGET}/* directories"
  echo ""
}

function clean_all_targets() {
  # Removes all outer- and inner-level build ${TARGET} directories
  cd ${SCRIPT_PATH}
  rm -rf ${SCRIPT_PATH}/${TARGET}/*
  echo "Cleaned ${SCRIPT_PATH}/${TARGET} directories"
  echo ""
  for i in ${MANUALS}; do
    rm -rf ${SCRIPT_PATH}/${i}/${TARGET}/*
    echo "Cleaned ${SCRIPT_PATH}/${i}/${TARGET} directories"
    echo ""
  done
}

function build_license_dependency_pdfs() {
  cd ${SCRIPT_PATH}/reference-manual
  ./build.sh license-pdfs ${ARG_2} 
  #${ARG_3}
}

function build_standalone() {
#   cd ${SCRIPT_PATH}/developers-manual
#   ./build.sh sdk ${ARG_2} 
#   ${ARG_3}
  set_mvn_environment
  MAVEN_OPTS="-Xmx1024m -XX:MaxPermSize=128m" mvn clean package -pl cdap-standalone,cdap-app-templates/cdap-etl,cdap-examples -am -amd -DskipTests -P examples,templates,dist,release,unit-tests
}

function print_version() {
  cd ${SCRIPT_PATH}/developers-manual
  ./build.sh version ${ARG_2} 
  #${ARG_3}
}

function _bell() {
  # Pass a message as ${1}
  if [[ "x${BELL}" == "xyes" || "x${BELL}" == "x${TRUE}" ]]; then
    echo -e "\a${1}"
  else
    echo -e "${1}"
  fi
}

function doc_test() {
  echo "Test..."
  _bell "A test message"
  echo "Test completed."
}

setup "quiet"
run_command  ${1}

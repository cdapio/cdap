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
  
# Common code for Build script for docs
# Not called directly; included in either the main build.sh or the individual manual's build.sh

# Optional Parameters (passed via env variable or exported in shell):
# BELL: Set it to for the bell function to make a sound when called
# COLOR_LOGS: Set it for color output by Sphinx and these scripts

API="cdap-api"
APIDOCS="apidocs"
APIS="apis"
BUILD_PDF="build-pdf"
CDAP_DOCS="cdap-docs"
HTML="html"
HYDRATOR_PLUGINS="hydrator-plugins"
INCLUDES="_includes"
JAVADOCS="javadocs"
LICENSES="licenses"
LICENSES_PDF="licenses-pdf"
PROJECT="cdap"
PROJECT_CAPS="CDAP"
REFERENCE="reference-manual"
SOURCE="source"
SPHINX_MESSAGES="warnings.txt"
TARGET="target"

FALSE="false"
TRUE="true"

# Redirect placed in top to redirect to 'en' directory
REDIRECT_EN_HTML=$(cat <<EOF
<!DOCTYPE HTML>
<html lang="en-US">
    <head>
        <meta charset="UTF-8">
        <meta http-equiv="refresh" content="0;url=en/index.html">
        <script type="text/javascript">
            window.location.href = "en/index.html"
        </script>
        <title></title>
    </head>
    <body>
    </body>
</html>
EOF
)

SCRIPT=$(basename ${0})
SCRIPT_PATH=$(pwd)
MANUAL=$(basename ${SCRIPT_PATH})

DOC_GEN_PY="${SCRIPT_PATH}/../tools/doc-gen.py"
TARGET_PATH="${SCRIPT_PATH}/${TARGET}"
SOURCE_PATH="${SCRIPT_PATH}/${SOURCE}"

if [ "x${2}" == "x" ]; then
  PROJECT_PATH="${SCRIPT_PATH}/../.."
else
  PROJECT_PATH="${SCRIPT_PATH}/../../../${2}"
fi

API_JAVADOCS="${PROJECT_PATH}/target/site/${APIDOCS}"

CHECK_INCLUDES=''
LOCAL_INCLUDES=''

if [[ "x${COLOR_LOGS}" != "x" ]]; then
  SPHINX_COLOR=''
  RED="$(tput setaf 1)"
  BOLD="$(tput bold)"
  RED_BOLD="$(tput setaf 1; tput bold)"
  NO_COLOR="$(tput setaf 0; tput sgr0)"
else
  SPHINX_COLOR="-N"
  RED_BOLD=''
  RED=''
  BOLD=''
  NO_COLOR=''
fi
WARNING="${RED_BOLD}WARNING:${NO_COLOR}"
SPHINX_BUILD="sphinx-build ${SPHINX_COLOR} -b html -d ${TARGET}/doctrees"

# Hash of file with "Not Found"; returned by GitHub
NOT_FOUND_HASHES=("9d1ead73e678fa2f51a70a933b0bf017" "6cb875b80d51f9a26eb05db7f9779011")

ZIP_FILE_NAME=$HTML
ZIP="${ZIP_FILE_NAME}.zip"

# Set Google Analytics Codes

# Corporate Docs Code
GOOGLE_TAG_MANAGER_CODE_WEB="GTM-KWLFGH"
WEB="web"

# CDAP Project Code
GOOGLE_TAG_MANAGER_CODE_GITHUB="GTM-PBZ3JL"
GITHUB="github"


function usage() {
  echo "Build script for '${PROJECT_CAPS}' docs"
  echo "Usage: ${SCRIPT} <action> [source]"
  echo
  echo "  Action (select one)"
  echo "    build                Clean build of javadocs and HTML docs, copy javadocs and PDFs into place, zip results"
  echo "    build-github         Clean build and zip for placing on GitHub (no Javadocs)"
  echo "    build-web            Clean build and zip for placing on docs.cask.co webserver (no Javadocs)"
  echo "    build-docs           Clean build of docs (no Javadocs)"
  echo "    docs                 alias for 'build-docs'"
  echo "    docs-local           Clean build of docs (no Javadocs), using local copies of downloaded files"
  echo
  echo "    license-pdfs         Clean build of License Dependency PDFs"
  echo "    check-includes       Check if included files have changed from source"
  echo "    display-version      Print the version information"
  echo "  with"
  echo "    source               Path to ${PROJECT} source for javadocs, if not '${PROJECT_PATH}'"
  echo "                         Path is relative to '${SCRIPT_PATH}/../..'"
  echo
}

function echo_red_bold() {
  echo "${RED_BOLD}${1}${NO_COLOR}${2}"
}

function clean() {
  cd ${SCRIPT_PATH}
  rm -rf ${SCRIPT_PATH}/${TARGET}
  mkdir -p ${SCRIPT_PATH}/${TARGET}
  echo "Cleaned ${SCRIPT_PATH}/${TARGET} directory"
  echo
}

function build_docs() {
  clean
  cd ${SCRIPT_PATH}
  check_includes
  ${SPHINX_BUILD} -w ${TARGET}/${SPHINX_MESSAGES} ${SOURCE} ${TARGET}/html
  consolidate_messages
}

function build_docs_local() {
  BELL="${TRUE}"
  LOCAL_INCLUDES="${TRUE}"
  export LOCAL_INCLUDES
  build_docs
}

function build_docs_google() {
  clean
  cd ${SCRIPT_PATH}
  check_includes
  ${SPHINX_BUILD} -w ${TARGET}/${SPHINX_MESSAGES} -A html_google_tag_manager_code=$1 ${SOURCE} ${TARGET}/html
  consolidate_messages
}

function build_license_pdfs() {
  set_version
  cd ${SCRIPT_PATH}
  PROJECT_VERSION_TRIMMED=${PROJECT_VERSION%%-SNAPSHOT*}
  rm -rf ${SCRIPT_PATH}/${LICENSES_PDF}
  mkdir ${SCRIPT_PATH}/${LICENSES_PDF}
  # paths are relative to location of $DOC_GEN_PY script
  LIC_PDF="../../../${REFERENCE}/${LICENSES_PDF}"
  LIC_RST="../${REFERENCE}/source/${LICENSES}"
  PDFS="cdap-enterprise-dependencies cdap-level-1-dependencies cdap-standalone-dependencies cdap-ui-dependencies"
  for PDF in ${PDFS}; do
    echo
    echo "Building ${PDF}"
    python ${DOC_GEN_PY} -g pdf -o ${LIC_PDF}/${PDF}.pdf -b ${PROJECT_VERSION_TRIMMED} ${LIC_RST}/${PDF}.rst
  done
}

function copy_license_pdfs() {
  cp ${SCRIPT_PATH}/${LICENSES_PDF}/* ${TARGET_PATH}/${HTML}/${LICENSES}
}

function make_zip() {
  set_version
  if [ "x${1}" == "x" ]; then
    ZIP_DIR_NAME="${PROJECT}-docs-${PROJECT_VERSION}"
  else
    ZIP_DIR_NAME="${PROJECT}-docs-${PROJECT_VERSION}-$1"
  fi
  cd ${TARGET_PATH}
  mkdir ${PROJECT_VERSION}
  mv ${HTML} ${PROJECT_VERSION}/en
  # Add a redirect index.html file
  echo "${REDIRECT_EN_HTML}" > ${PROJECT_VERSION}/index.html
  # Zip everything
  zip -qr ${ZIP_DIR_NAME}.zip ${PROJECT_VERSION}/* --exclude *.DS_Store* *.buildinfo*
}

function build() {
  build_docs
  build_extras
}

function build_github() {
  build_docs_google ${GOOGLE_TAG_MANAGER_CODE_GITHUB}
  build_extras
}

function build_web() {
  build_docs_google ${GOOGLE_TAG_MANAGER_CODE_WEB}
  build_extras
}

function build_extras() {
  # Over-ride this function in guides where Javadocs or licenses are being built or copied.
  # Currently performed in reference-manual
  echo "No extras being built or copied."
}

function set_mvn_environment() {
  check_build_rst
  cd ${PROJECT_PATH}
  if [[ "${OSTYPE}" == "darwin"* ]]; then
    # TODO: hard-coded Java version 1.7
    export JAVA_HOME=$(/usr/libexec/java_home -v 1.7)
  else
#     export JAVA_HOME=/usr/lib/jvm/jdk1.7.0_75
    if [[ "x${JAVA_HOME}" == "x" ]]; then
      if [[ "x${JAVA_JDK_VERSION}" == "x" ]]; then
        JAVA_JDK_VERSION="jdk1.7.0_75"
      fi
      export JAVA_HOME=/usr/lib/jvm/${JAVA_JDK_VERSION}
    fi
  fi
  echo "Using JAVA_HOME: ${JAVA_HOME}"
}

function check_build_rst() {
  local current_directory=$(pwd)
  cd ${PROJECT_PATH}
  # check BUILD.rst for changes
  BUILD_RST_PATH="${PROJECT_PATH}/${BUILD_RST}"
  test_an_include "${BUILD_RST_HASH}" "${BUILD_RST_PATH}" "${BUILD_RST_HASH_LOCATION}"
  echo
  cd ${current_directory}
}

function check_includes() {
  if [ "x${DOCS_LOCAL}" == "x${TRUE}" ]; then
    LOCAL_INCLUDES="${TRUE}"
  fi
  if [ "${CHECK_INCLUDES}" == "${TRUE}" ]; then
    echo_red_bold "Downloading and checking files to be included."
    # Build includes
    local target_includes_dir=${SCRIPT_PATH}/${TARGET}/${INCLUDES}
    rm -rf ${target_includes_dir}
    mkdir ${target_includes_dir}
    download_includes ${target_includes_dir}
    test_includes ${target_includes_dir}
  else
    echo "No includes to be checked."
  fi
}

function download_includes() {
  # $1 is passed as the directory to which the downloaded files are to be written.
  # For an example of over-riding this function, see developer/build.sh
  echo "No includes to be downloaded."
}

function test_includes() {
  # $1 is passed as the directory to which the downloaded files are to be written.
  # For an example of over-riding this function, see developer/build.sh
  echo "No includes to be tested."
}

function test_an_include() {
  # Tests a file and checks that it hasn't changed.
  # Uses md5 hashes to monitor if any files have changed.
  local md5_hash=${1}
  local target=${2}
  local location=${3}
  local new_md5_hash
  local m
  local m_display  
  
  if [[ "x${target}" != "x" ]]; then
    local file_name=$(basename ${target})
  
    if [[ "${OSTYPE}" == "darwin"* ]]; then
      new_md5_hash=$(md5 -q ${target})
    else
      new_md5_hash=$(md5sum ${target} | awk '{print $1}')
    fi
    
    # If the new_md5_hash is in the NOT_FOUND_HASHES, it will set as the not_found_hash
    local not_found_hash=`echo ${NOT_FOUND_HASHES[@]} | grep -o "${new_md5_hash}"`
  
    if [[ "${new_md5_hash}" == "${not_found_hash}" ]]; then
      m="${WARNING} ${RED_BOLD}${file_name} not found!${NO_COLOR}"
      m="${m}\nfile: ${target}"
    elif [[ "${new_md5_hash}" != "${md5_hash}" ]]; then
      m="${WARNING} ${RED_BOLD}${file_name} has changed! Compare files and update hash!${NO_COLOR}"
      m="${m}\nfile: ${target}"
      m="${m}\nOld MD5 Hash: ${md5_hash} New MD5 Hash: ${new_md5_hash}"
      if [[ "x${location}" != "x" ]]; then
        m="${m}\nHash location: ${location}"
      fi
    fi
  else  
    m="No target is set for test_an_include"
  fi
  if [ "x${m}" != "x" ]; then
    set_message "${m}"
  else
    m="MD5 Hash for ${file_name} matches"
  fi
  printf "${m}\n"
}

function download_file() {
  # Downloads a file to the includes directory, and checks that it hasn't changed.
  # Uses md5 hashes to monitor if any files have changed.
  # Example:
  #               1:Target dir  2:Source dir  3:Filename                 4:MD5 hash of file               5: Target filename (optional)
  # download_file $includes     $project_main BounceCountsMapReduce.java 4474e5437a15d341572842613ba712bd BCMR.java

  local includes_dir=${1}
  local source_dir=${2}
  local file_name=${3}
  local md5_hash=${4}
  local target_filename=${5}
  if [[ "x${target_filename}" == "x" ]]; then
    local target=${includes_dir}/${file_name}
  else
    local target=${includes_dir}/${target_filename}
  fi
  
  if [ ! -d "${includes_dir}" ]; then
    mkdir ${includes_dir}
    echo "Creating Includes Directory: ${includes_dir}"
  fi

  echo "Downloading using curl ${file_name}"
  echo "from ${source_dir}"
  curl --silent ${source_dir}/${file_name} --output ${target}
  test_an_include ${md5_hash} ${target}
}

function set_version() {
  OIFS="${IFS}"
  local current_directory=$(pwd)
  cd ${PROJECT_PATH}
  source ${PROJECT_PATH}/${CDAP_DOCS}/vars
  PROJECT_VERSION=$(grep "<version>" pom.xml)
  PROJECT_VERSION=${PROJECT_VERSION#*<version>}
  PROJECT_VERSION=${PROJECT_VERSION%%</version>*}
  PROJECT_LONG_VERSION=$(expr "${PROJECT_VERSION}" : '\([0-9]*\.[0-9]*\.[0-9]*\)')
  PROJECT_SHORT_VERSION=$(expr "${PROJECT_VERSION}" : '\([0-9]*\.[0-9]*\)')
  local full_branch=$(git rev-parse --abbrev-ref HEAD)
  IFS=/ read -a branch <<< "${full_branch}"
  GIT_BRANCH="${branch[1]}"
  if [ "x${GIT_BRANCH_PARENT}" == "x" ]; then
    GIT_BRANCH_PARENT="develop"
  fi
  # Determine branch and branch type: one of develop, master, release, develop-feature, release-feature
  # If unable to determine type, uses develop-feature
  if [ "${full_branch}" == "develop" -o  "${full_branch}" == "master" ]; then
    GIT_BRANCH="${full_branch}"
    GIT_BRANCH_TYPE=${GIT_BRANCH}
  elif [ "${full_branch:0:4}" == "docs" ]; then
    GIT_BRANCH="${full_branch}"
    if [ "${GIT_BRANCH_PARENT:0:7}" == "develop" ]; then
      GIT_BRANCH_TYPE="develop-feature"
    elif [ "x${GIT_BRANCH_TYPE}" == "x" ]; then
      GIT_BRANCH_TYPE="release-feature"
    fi
  elif [ "${GIT_BRANCH:0:7}" == "release" ]; then
    GIT_BRANCH_TYPE="release"
  else
    # We are on a feature branch: but from develop or release?
    # This is not easy to determine. This can fail very easily.
    if [ "x${GIT_BRANCH_TYPE}" == "x" ]; then
      if [ "${GIT_BRANCH_PARENT:0:7}" == "release" ]; then
        GIT_BRANCH_TYPE="release-feature"
      else
        GIT_BRANCH_TYPE="develop-feature"
      fi
    fi
  fi
  cd ${current_directory}
  IFS="${OIFS}"
  
  if [ "x${GIT_BRANCH_TYPE:0:7}" == "xdevelop" ]; then
    GIT_BRANCH_CASK_HYDRATOR="develop"
    GIT_BRANCH_CASK_TRACKER="develop"
  fi
  get_cask_hydrator_version ${GIT_BRANCH_CASK_HYDRATOR}
  get_cask_tracker_version ${GIT_BRANCH_CASK_TRACKER}
}

function display_version() {
  echo "PROJECT_PATH: ${PROJECT_PATH}"
  echo "PROJECT_VERSION: ${PROJECT_VERSION}"
  echo "PROJECT_LONG_VERSION: ${PROJECT_LONG_VERSION}"
  echo "PROJECT_SHORT_VERSION: ${PROJECT_SHORT_VERSION}"
  echo "GIT_BRANCH: ${GIT_BRANCH}"
  echo "GIT_BRANCH_TYPE: ${GIT_BRANCH_TYPE}"
  echo "GIT_BRANCH_PARENT: ${GIT_BRANCH_PARENT}"
  echo "Hydrator:"
  echo "GIT_BRANCH_CASK_HYDRATOR: ${GIT_BRANCH_CASK_HYDRATOR}"
  echo "CASK_HYDRATOR_VERSION: ${CASK_HYDRATOR_VERSION}"
  echo "Tracker:"
  echo "GIT_BRANCH_CASK_TRACKER: ${GIT_BRANCH_CASK_TRACKER}"
  echo "CASK_TRACKER_VERSION: ${CASK_TRACKER_VERSION}"
}

function get_cask_hydrator_version() {
  # $1 Branch of Hydrator to use
  CASK_HYDRATOR_VERSION=$(curl --silent "https://raw.githubusercontent.com/caskdata/hydrator-plugins/${1}/pom.xml" | grep "<version>")
  CASK_HYDRATOR_VERSION=${CASK_HYDRATOR_VERSION#*<version>}
  CASK_HYDRATOR_VERSION=${CASK_HYDRATOR_VERSION%%</version>*}
  export CASK_HYDRATOR_VERSION
}

function get_cask_tracker_version() {
  # $1 Branch of Tracker to use
  CASK_TRACKER_VERSION=$(curl --silent "https://raw.githubusercontent.com/caskdata/cask-tracker/${1}/pom.xml" | grep "<version>")
  CASK_TRACKER_VERSION=${CASK_TRACKER_VERSION#*<version>}
  CASK_TRACKER_VERSION=${CASK_TRACKER_VERSION%%</version>*}
  if [ "x${CASK_TRACKER_VERSION}" == "x" ]; then
    CASK_TRACKER_VERSION="0.2.0-SNAPSHOT"
    echo "Using default CASK_TRACKER_VERSION ${CASK_TRACKER_VERSION}"
  fi
  export CASK_TRACKER_VERSION
}

function clear_messages_set_messages_file() {
  unset -v MESSAGES
  TMP_MESSAGES_FILE="${TARGET_PATH}/.$(basename $0).$$.messages"
  if [ ! -d ${TARGET_PATH} ]; then
    echo "Making directory ${TARGET_PATH}"
    mkdir ${TARGET_PATH}
  fi
  cat /dev/null > ${TMP_MESSAGES_FILE}
  export TMP_MESSAGES_FILE
  echo_red_bold "Cleared Messages and Messages file: " "$(basename ${TMP_MESSAGES_FILE})"
  echo
}

function cleanup_messages_file() {
  rm -f ${TMP_MESSAGES_FILE}
  unset -v TMP_MESSAGES_FILE
}

function set_message() {
  if [ "x${MESSAGES}" == "x" ]; then
    MESSAGES=${*}
  else
    MESSAGES="${MESSAGES}\n\n${*}"
  fi
}

function consolidate_messages() {
  if [[ "x${TMP_MESSAGES_FILE}" == "x" ]]; then
    return
  fi
  local m="Warning Messages for \"${MANUAL}\":"
  local l="--------------------------------------------------------"
  if [ "x${MESSAGES}" != "x" ]; then
    echo_red_bold "Consolidating messages" 
    echo >> ${TMP_MESSAGES_FILE}
    echo_red_bold "${m}" >> ${TMP_MESSAGES_FILE}
    echo ${l} >> ${TMP_MESSAGES_FILE}
    echo >> ${TMP_MESSAGES_FILE}
    printf "${MESSAGES}\n" >> ${TMP_MESSAGES_FILE}
    unset -v MESSAGES
  fi
  if [ -s ${TARGET}/${SPHINX_MESSAGES} ]; then
    echo_red_bold "Consolidating Sphinx messages" 
    m="Sphinx ${m}"
    echo >> ${TMP_MESSAGES_FILE}
    echo_red_bold "${m}" >> ${TMP_MESSAGES_FILE}
    echo ${l} >> ${TMP_MESSAGES_FILE}
    echo >> ${TMP_MESSAGES_FILE}
    cat ${TARGET}/${SPHINX_MESSAGES} | while read line
    do
      echo ${line} >> ${TMP_MESSAGES_FILE}
    done
  fi
}

function display_messages_file() {
  local warnings=0
  if [[ "x${TMP_MESSAGES_FILE}" != "x" && -s ${TMP_MESSAGES_FILE} ]]; then
    echo 
    echo "--------------------------------------------------------"
    echo_red_bold "Warning Messages: $(basename ${TMP_MESSAGES_FILE})"
    echo "--------------------------------------------------------"
    echo 
    cat ${TMP_MESSAGES_FILE} | while read line
    do
      echo "${line}"
    done
    echo 
    echo "--------------------------------------------------------"
    echo_red_bold "End Warning Messages"
    echo "--------------------------------------------------------"
    echo
    warnings=1 # Indicates warning messages present
  fi
  return ${warnings}
}

function rewrite() {
  # Substitutes text in file $1 and outputting to file $2, replacing text $3 with text $4
  # or if $4=='', substitutes text in-place in file $1, replacing text $2 with text $3
  # or if $3 & $4=='', substitutes text in-place in file $1, using sed command $2
  cd ${SCRIPT_PATH}
  local rewrite_source=${1}
  echo "Re-writing"
  echo "    $rewrite_source"
  if [ "x${3}" == "x" ]; then
    local sub_string=${2}
    echo "    $sub_string"
    if [ "$(uname)" == "Darwin" ]; then
      sed -i ".bak" "${sub_string}" ${rewrite_source}
      rm ${rewrite_source}.bak
    else
      sed -i "${sub_string}" ${rewrite_source}
    fi
  elif [ "x${4}" == "x" ]; then
    local sub_string=${2}
    local new_sub_string=${3}
    echo "    ${sub_string} -> ${new_sub_string} "
    if [ "$(uname)" == "Darwin" ]; then
      sed -i ".bak" "s|${sub_string}|${new_sub_string}|g" ${rewrite_source}
      rm ${rewrite_source}.bak
    else
      sed -i "s|${sub_string}|${new_sub_string}|g" ${rewrite_source}
    fi
  else
    local rewrite_target=${2}
    local sub_string=${3}
    local new_sub_string=${4}
    echo "  to"
    echo "    ${rewrite_target}"
    echo "    ${sub_string} -> ${new_sub_string} "
    sed -e "s|${sub_string}|${new_sub_string}|g" ${rewrite_source} > ${rewrite_target}
  fi
}

function run_command() {
  set_version
  case ${1} in
    build|build-github|build-web|build-docs)      "${1/-/_}";;
    check-includes|display-version)               "${1/-/_}";;
    license-pdfs)                                 "build_license_pdfs";;
    docs)                                         "build_docs";;
    docs-local)                                   "build_docs_local";;
    *)                                            usage;;
  esac
}

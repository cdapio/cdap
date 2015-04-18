#!/usr/bin/env bash

# Copyright © 2015 Cask Data, Inc.
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

################################################################################
### Deploy script for docs
# Deploys zip files created by build scripts
# First parameter is cdap (default)
# Second parameter is version
# third parameter is the remote directory to be created on the web/doc server
################################################################################
DEBUG=${DEBUG:-no}
PROJECT=${1:-cdap}
PROJECT_DOCS=${PROJECT}-docs
#PROJECT_PATH=
VERSION=${2:-2.8.1}
PROJECT_VERSION=${3:-2.8.1}
RSYNC_OPTS='-a --human-readable --progress --stats --rsync-path="sudo rsync"'
WEB_FILE=${PROJECT}-docs-${VERSION}-web.zip
GITHUB_FILE=${PROJECT}-docs-${VERSION}-github.zip
JOB_DIR=/var/bamboo/xml-data/build-dir/CDAP-DRBD-JOB1
FILE_PATH=${JOB_DIR}/${PROJECT}/${PROJECT_DOCS}/build
USER=bamboo
DOCS_SERVER1=docs1.cask.co
DOCS_SERVER2=docs2.cask.co
DOCS_SERVERS="${DOCS_SERVER1} ${DOCS_SERVER2}"
STG_SERVER=docs-staging.cask.co
REMOTE_DOCS_BASE=/var/www/docs/cdap
REMOTE_STG_BASE=/var/www/html/cdap
REMOTE_DOCS_DIR="${REMOTE_DOCS_BASE}/${PROJECT_VERSION}"
REMOTE_STG_DIR="${REMOTE_STG_BASE}/${PROJECT_VERSION}"

# output trimmer
decho () {
  if [[ "${DEBUG}" == 'yes' ]]; then
    echo ${*}
  else
    RSYNC_QUIET='--quiet'
  fi
}

die ( ) { echo ; echo "ERROR: ${*}" ; echo ; exit 1; }
################################################################################
function make_remote_dir () {
  decho "make sure remote directory exists"
  decho "ssh ${1}@${2} \"sudo mkdir -p ${3}\""
  ssh ${1}@${2} "sudo mkdir -p ${3}"
  decho ""
}

function rsync_zip_file () {
  decho "rsync archive"
  decho "rsync -a -e 'ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null' --human-readable --progress --rsync-path=\"sudo rsync\" ${5}/${4} \"${1}@${2}:${3}/.\""
  rsync -a -e "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null" --human-readable --progress --rsync-path="sudo rsync" ${5}/${4} "${1}@${2}:${3}/."
  decho ""
}

function unzip_archive () {
  decho "going to new staging server and unzipping the file"
  decho "ssh ${1}@${2} \"sudo unzip -o ${3}/${4} -d ${3}\""
  ssh ${1}@${2} "sudo unzip -o ${3}/${4} -d ${3}"
  decho ""
}

function deploy () {
  make_remote_dir ${1} ${2} ${3}
  rsync_zip_file ${1} ${2} ${3} ${4} ${5}
  unzip_archive ${1} ${2} ${3} ${4}
}
################################################################################
decho "DEPLOYING"

### DEVELOP => Staging
deploy ${USER} ${STG_SERVER} ${REMOTE_STG_DIR} ${WEB_FILE} ${FILE_PATH}

### RELEASE => Docs Servers
for i in ${DOCS_SERVERS}; do
  decho "not deploying to $i"
#  deploy ${USER} ${i} ${REMOTE_STG_DIR} ${WEB_FILE} ${FILE_PATH}  
done

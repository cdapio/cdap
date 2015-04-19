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
# Deployment script for docs
# Deploys zip files created by build scripts
#
# 'PROJECT' (e.g. cdap) environment variable must be set or the script will exit
#
# function variables reference: (these were kept consistent throughout)
# $1 user name (typically bamboo)
# $2 remote server (docs or docs staging servers)
# $3 remote web directory
# $4 zip archive file
# $5 local path to zip archive file
################################################################################

function get_version () {
  TMP_VERSION=`grep "<version>" ../pom.xml`
  TMP_VERSION=${TMP_VERSION#*<version>}
  VERSION=${TMP_VERSION%%</version>*}
}

get_version

# parameters that can be passed to this script (as environment variables)
DEBUG=${DEBUG:-no}
DEPLOY_TO_STG=${DEPLOY_TO_STG:-no}
DEPLOY_TO_DOCS=${DEPLOY_TO_DOCS:-no}
PROJECT=${PROJECT}   # mandatory
DOC_DIR=${DOC_DIR:-${VERSION}}
BUILD_WORKING_DIR=${BUILD_WORKING_DIR:-/var/bamboo/xml-data/build-dir/CDAP-DRBD-JOB1}
DOCS_SERVER1=${DOCS_SERVER1:-docs1.cask.co}
DOCS_SERVER2=${DOCS_SERVER2:-docs2.cask.co}
STG_SERVER=${STG_SERVER:-docs-staging.cask.co}
REMOTE_STG_BASE=${REMOTE_STG_BASE:-/var/www/html/cdap}
REMOTE_DOCS_BASE=${REMOTE_DOCS_BASE:-/var/www/docs/cdap}

#
USER=bamboo
PROJECT_DOCS=${PROJECT}-docs
WEB_FILE=${PROJECT}-docs-${VERSION}-web.zip
GITHUB_FILE=${PROJECT}-docs-${VERSION}-github.zip
FILE_PATH=${BUILD_WORKING_DIR}/${PROJECT}/${PROJECT_DOCS}/build
DOCS_SERVERS="${DOCS_SERVER1} ${DOCS_SERVER2}"
REMOTE_STG_DIR="${REMOTE_STG_BASE}/${DOC_DIR}"
REMOTE_DOCS_DIR="${REMOTE_DOCS_BASE}/${DOC_DIR}"

RSYNC_OPTS='-aPh'
SSH_OPTS='ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null'
RSYNC_PATH='sudo rsync'

# output trimmer
decho () {
  if [[ "${DEBUG}" == 'yes' ]]; then
    echo ${*}
  else
    RSYNC_QUIET='--quiet'
  fi
}

die ( ) { echo ; echo "ERROR: ${*}" ; echo ; exit 1; }

if [ "${PROJECT}" == '' ]; then
  echo "PROJECT not defined"
  exit 1
fi

################################################################################

function make_remote_dir () {
  decho "making sure remote directory ${3} exists on ${2}"
  decho "ssh ${1}@${2} \"sudo mkdir -p ${3}\""
  ssh ${1}@${2} "sudo mkdir -p ${3}" || die "could not create ${3} directory on ${2}"
  decho ""
}

function rsync_zip_file () {
  decho "rsyncing archive ${4} to ${2}"
  decho "rsync ${RSYNC_OPTS} -e \"${SSH_OPTS}\" --rsync-path=\"${RSYNC_PATH}\" ${5}/${4} \"${1}@${2}:${3}/.\"" || die "could not rsync ${4} to ${2}"
  rsync ${RSYNC_OPTS} -e "${SSH_OPTS}" --rsync-path="${RSYNC_PATH}" ${5}/${4} "${1}@${2}:${3}/." || die "could not rsync ${4} to ${2}"
  decho ""
}

function unzip_archive () {
  decho "unzipping ${4} on ${2}"
  decho "ssh ${1}@${2} \"sudo unzip -o ${3}/${4} -d ${3}\""
  ssh ${1}@${2} "sudo unzip -o ${3}/${4} -d ${3}" || die "unable to unzip ${4} in ${3} on ${2}, as ${1}"
  decho ""
}

function deploy () {
  decho "deploying to ${2}"
  make_remote_dir ${1} ${2} ${3}
  rsync_zip_file ${1} ${2} ${3} ${4} ${5}
  unzip_archive ${1} ${2} ${3} ${4}
}

################################################################################

decho "######################### DEPLOYING #########################"
decho "DEPLOY_TO_STG=${DEPLOY_TO_STG}"
decho "DEPLOY_TO_DOCS=${DEPLOY_TO_DOCS}"

### DEVELOP => Staging
if [[ "${DEPLOY_TO_STG}" == 'yes' ]]; then
  decho "Deploying to Staging server"
  deploy ${USER} ${STG_SERVER} ${REMOTE_STG_DIR} ${WEB_FILE} ${FILE_PATH}
fi

### RELEASE => Docs Servers
if [[ "${DEPLOY_TO_DOCS}" == 'yes' ]]; then
  decho "Deploying to Docs servers"
  for i in ${DOCS_SERVERS}; do
    deploy ${USER} ${i} ${REMOTE_DOCS_DIR} ${WEB_FILE} ${FILE_PATH}
  done
fi
decho "####################### DEPLOYING DONE #######################"


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
# BUILD_WORKING_DIR must be passed as an environment variable (can be set in Bamboo)
#   It will typically look like this:
#   Release: /var/bamboo/xml-data/build-dir/CDAP-DRBD-JOB1
#   Develop: /var/bamboo/xml-data/build-dir/CDAP-DDBD-JOB1
# 
# OPT_DIR is an optional directory to use instead of derived branch name. E.g.:
#   if branch name=feature/docs-build-testing, and we want to use testdir
#   we would set OPT_DIR=testdir and add it to the bamboo tasks' variables
#   and the remote directory would be 2.8.1/testdir (testdir treated as branch)
# If OPT_DIR is not set, bamboo.planRepository.<position>.branch will be used,
#   e.g. /var/www/html/cdap/2.8.1/feature-myfix, unless the branch name
#   is release/* or develop/* in which case, it will just be put into
#   the version directory, e.g. /var/www/html/cdap/2.8.1
#
################################################################################

# output trimmer
decho () {
  if [[ "${DEBUG}" == 'yes' ]]; then
    echo ${*}
  else
    RSYNC_QUIET='--quiet'
  fi
}

# get project version from main pom.xml
function get_version () {
  TMP_VERSION=`grep "<version>" ../pom.xml`
  TMP_VERSION=${TMP_VERSION#*<version>}
  VERSION=${TMP_VERSION%%</version>*}
}

# get project name (e.g. cdap) from main pom.xml
function get_project () {
  TMP_PROJECT=`grep "<artifactId>" ../pom.xml`
  TMP_PROJECT=${TMP_PROJECT#*<artifactId>}
  PROJECT=${TMP_PROJECT%%</artifactId>*}
}

# convert branch names that look like this: feature/my-branch to feature-my-branch
function convert_branch_name () {
  decho "converting '/' to '-' in branch name"
  DOC_DIR=`echo $DOC_DIR | tr '/' '-'`
}

# Determines remote directory based on type of build
#   3 scenarios (assuming remote base web directory=/var/www/html/cdap and VERSION=2.8.1)
#     OPT_DIR is set via environment variables
#         => remote directory=/var/www/html/cdap/${VERSION}-${OPT_DIR}
#     OPT_DIR is not set AND branch=release/* or branch=develop/*
#         => remote directory=/var/www/html/cdap/2.8.1
#     OPT_DIR is not set AND branch=anything else (e.g. feature/*, hotfix/*, etc..)
#         => remote directory=/var/www/html/cdap/${VERSION}-${BRANCH_NAME}
set_remote_dir () {
  DOC_DIR=${OPT_DIR:-${BRANCH_NAME}}
  convert_branch_name
  BRANCH=''
  if [[ "${DOC_DIR}" == release* || "${DOC_DIR}" == develop* ]]; then
    REMOTE_DIR=''
  else
    REMOTE_DIR=${VERSION}-${DOC_DIR}
    BRANCH=yes
  fi
  decho "SUBDIR=${REMOTE_DIR}"
}

################################
### parameters that can be passed to this script (as environment variables)

## bamboo plan variables
# OPT_DIR           (optional) 
DEBUG=${DEBUG:-no} #(optional)
DEPLOY_TO_STG=${DEPLOY_TO_STG:-no}
DEPLOY_TO_DOCS=${DEPLOY_TO_DOCS:-no}
REMOTE_STG_BASE=${REMOTE_STG_BASE:-/var/www/html/staging/}
REMOTE_DOCS_BASE=${REMOTE_DOCS_BASE:-/var/www/docs/}

## bamboo global variables
# DOCS_SERVER1
# DOCS_SERVER2
# STG_SERVER
################################

decho "OPT_DIR=${OPT_DIR}"
decho "BRANCH_NAME=${BRANCH_NAME}"

# set the mandatory variables first (from info in project's main pom.xml)
get_version
get_project

# Determine remote directory
set_remote_dir

#
USER=bamboo
PROJECT_DOCS=${PROJECT}-docs
ZIP_FILE=${PROJECT}-docs-${VERSION}-web.zip
FILE_PATH=${BUILD_WORKING_DIR}/${PROJECT}/${PROJECT_DOCS}/build
DOCS_SERVERS="${DOCS_SERVER1} ${DOCS_SERVER2}"
REMOTE_STG_DIR="${REMOTE_STG_BASE}/${PROJECT}/${REMOTE_DIR}"		# e.g. /var/www/html/staging/cdap/develop
REMOTE_DOCS_DIR="${REMOTE_DOCS_BASE}/${PROJECT}/${REMOTE_DIR}"		# e.g. /var/www/docs/cdap/release

SSH_OPTS='-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null'
RSYNC_OPTS='-aPh'
RSYNC_SSH_OPTS="ssh ${SSH_OPTS}"
RSYNC_PATH='sudo rsync'

die ( ) { echo ; echo "ERROR: ${*}" ; echo ; exit 1; }

# make sure PROJECT is set to something
if [ "${PROJECT}" == '' ]; then
  echo "PROJECT not defined"
  exit 1
fi

# make sure we have selected a location for deploying the docs
if [[ "${DOCS_SERVER1}" == '' || "${DOCS_SERVER2}" == '' ]] && [[ "${STG_SERVER}" == '' ]]; then
  echo "No servers defined for deployment!"
  exit 1
fi

################################################################################

# copy zip file to local_dir's unzipped directory (e.g. cdap-docs/build/3.0.0-SNAPSHOT/)
function copy_zip_file () {
  local _zip_file=${1}
  local _local_dir=${2}
  local _version=${3}
  decho "cp ${_local_dir}/${_zip_file} ${_local_dir}/${_version}/"
  cp ${_local_dir}/${_zip_file} ${_local_dir}/${_version}/
}

# delete and create remote directory prior to rsync
function make_remote_dir () {
  local _user=${1}
  local _host=${2}
  local _rdir=${3} # remote directory
  decho "ssh ${SSH_OPTS} ${_user}@${_host} \"sudo rm -rf ${_rdir} && sudo mkdir -p ${_rdir}\""
  ssh ${SSH_OPTS} ${_user}@${_host} "sudo rm -rf ${_rdir} && sudo mkdir -p ${_rdir}" || die "could not create ${_rdir} directory on ${_host}"
  decho ""
}

# rsync local dir to remote dir
function sync_local_dir_to_remote_dir () {
  local _user=${1}
  local _host=${2}
  local _rdir=${3} # remote directory
  local _zip_file=${4}
  local _local_dir=${5}
  local _version=${6}
  decho ""
  decho "rsync ${RSYNC_OPTS} -e \"${RSYNC_SSH_OPTS}\" --rsync-path=\"${RSYNC_PATH}\" ${_local_dir}/${_version} \"${_user}@${_host}:${_rdir}\"" 
  rsync ${RSYNC_OPTS} -e "${RSYNC_SSH_OPTS}" --rsync-path="${RSYNC_PATH}" ${_local_dir}/${_version}/.h* ${_local_dir}/${_version}/* "${_user}@${_host}:${_rdir}" || die "could not rsync ${_local_dir} to ${_rdir} on ${_host}" 
}

# main deploy function
function deploy () {
  local _user=${1}
  local _host=${2}
  local _remote_dir=${3}
  local _zip_file=${4}
  local _local_dir=${5}
  local _version=${6}
  local _branch=${7}
  decho "deploying to ${_host}"
  # if there is no branch name to append, append version to directory name, otherwise:
  #   cdap dir gets blown away in remote dir delete/create
  #   otherwise all files end up one level too high after rsync
  if [ "${BRANCH}" == '' ]; then
    _remote_dir=${_remote_dir}/${_version}
  fi
  copy_zip_file ${_zip_file} ${_local_dir} ${_version}
  make_remote_dir ${_user} ${_host} ${_remote_dir}
  sync_local_dir_to_remote_dir ${_user} ${_host} ${_remote_dir} ${_zip_file} ${_local_dir} ${_version}
  decho "branch=${_branch}"
}

################################################################################

decho "######################### DEPLOYING #########################"
decho "DEPLOY_TO_STG=${DEPLOY_TO_STG}"
decho "DEPLOY_TO_DOCS=${DEPLOY_TO_DOCS}"

### DEVELOP => Staging
if [[ "${DEPLOY_TO_STG}" == 'yes' ]]; then
  decho "Deploying artifacts to Staging server"
  deploy ${USER} ${STG_SERVER} ${REMOTE_STG_DIR} ${ZIP_FILE} ${FILE_PATH} ${VERSION} ${BRANCH}
fi

### RELEASE => Docs Servers
if [[ "${DEPLOY_TO_DOCS}" == 'yes' ]]; then
  if [ "${BRANCH}" == '' ]; then
    decho "Deploying artifacts to Docs servers"
    for i in ${DOCS_SERVERS}; do
      deploy ${USER} ${i} ${REMOTE_DOCS_DIR} ${ZIP_FILE} ${FILE_PATH} ${VERSION}
    done
  else
    decho "Do not deploy artifacts from feature branches to Docs servers"
  fi
fi
decho "####################### DONE DEPLOYING #######################"


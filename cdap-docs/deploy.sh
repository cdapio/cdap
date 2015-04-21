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
# OPT_DIR is an optional directory to use instead of derived branch name. E.g.:
#   if branch name=feature/docs-build-testing, and we want to use testdir
#   we would set OPT_DIR=testdir and add it to the bamboo tasks' variables
#   and the remote directory would be 2.8.1/testdir (testdir treated as branch)
# If OPT_DIR is not set, bamboo.planRepository.<position>.branch will be used,
#   e.g. /var/www/html/cdap/2.8.1/feature-myfix, unless the branch name
#   is release/* or develop/* in which case, it will just be put into
#   the version directory, e.g. /var/www/html/cdap/2.8.1
#
# function variables reference: (these were kept consistent throughout)
# $1 user name (typically bamboo)
# $2 remote server (docs or docs staging servers)
# $3 remote web directory
# $4 zip archive file
# $5 local path to zip archive file
################################################################################

# output trimmer
decho () {
  if [[ "${DEBUG}" == 'yes' ]]; then
    echo ${*}
  else
    RSYNC_QUIET='--quiet'
  fi
}

# set the mandatory variables first (from info in project's main pom.xml)

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

# create remote directory based on type of build
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
    #REMOTE_DIR=branches/${DOC_DIR}
    REMOTE_DIR=${VERSION}-${DOC_DIR}
    BRANCH=yes
  fi
  decho "SUBDIR=${REMOTE_DIR}"
}

# parameters that can be passed to this script (as environment variables)
DEBUG=${DEBUG:-no}
DEPLOY_TO_STG=${DEPLOY_TO_STG:-no}
DEPLOY_TO_DOCS=${DEPLOY_TO_DOCS:-no}
BUILD_WORKING_DIR=${BUILD_WORKING_DIR:-/var/bamboo/xml-data/build-dir/CDAP-DRBD-JOB1}
DOCS_SERVER1=${DOCS_SERVER1:-docs1.cask.co}
DOCS_SERVER2=${DOCS_SERVER2:-docs2.cask.co}
STG_SERVER=${STG_SERVER:-docs-staging.cask.co}
REMOTE_STG_BASE=${REMOTE_STG_BASE:-/var/www/html/${PROJECT}}
REMOTE_DOCS_BASE=${REMOTE_DOCS_BASE:-/var/www/docs/${PROJECT}}

decho "OPT_DIR=${OPT_DIR}"
decho "BRANCH_NAME=${BRANCH_NAME}"

get_version
get_project
set_remote_dir

#
USER=bamboo
PROJECT_DOCS=${PROJECT}-docs
ZIP_FILE=${PROJECT}-docs-${VERSION}-web.zip
FILE_PATH=${BUILD_WORKING_DIR}/${PROJECT}/${PROJECT_DOCS}/build
DOCS_SERVERS="${DOCS_SERVER1} ${DOCS_SERVER2}"
REMOTE_STG_DIR="${REMOTE_STG_BASE}/${REMOTE_DIR}"
REMOTE_DOCS_DIR="${REMOTE_DOCS_BASE}/${REMOTE_DIR}"

RSYNC_OPTS='-aPh'
SSH_OPTS='ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null'
RSYNC_PATH='sudo rsync'

die ( ) { echo ; echo "ERROR: ${*}" ; echo ; exit 1; }

if [ "${PROJECT}" == '' ]; then
  echo "PROJECT not defined"
  exit 1
fi

################################################################################

# create remote directory prior to rsync
function make_remote_dir () {
  local _user=${1}
  local _host=${2}
  local _remote_dir=${3}
  decho "ssh ${_user}@${_host} \"sudo mkdir -p ${_remote_dir}\""
  ssh ${_user}@${_host} "sudo mkdir -p ${_remote_dir}" || die "could not create ${_remote_dir} directory on ${_host}"
  decho ""
}

# rsync zip file to remote directory in directory we just created
function rsync_zip_file () {
  local _user=${1}
  local _host=${2}
  local _remote_dir=${3}
  local _zip_file=${4}
  local _local_dir=${5}
  decho "rsyncing archive ${_zip_file} to ${_host}"
  decho "rsync ${RSYNC_OPTS} -e \"${SSH_OPTS}\" --rsync-path=\"${RSYNC_PATH}\" ${_local_dir}/${_zip_file} \"${_user}@${_host}:${_remote_dir}/.\""
  rsync ${RSYNC_OPTS} -e "${SSH_OPTS}" --rsync-path="${RSYNC_PATH}" ${_local_dir}/${_zip_file} "${_user}@${_host}:${_remote_dir}/." || die "could not rsync ${_zip_file} to ${_host}"
  decho ""
}

# unzip file on remote server
function unzip_archive () {
  local _user=${1}
  local _host=${2}
  local _remote_dir=${3}
  local _zip_file=${4}
  decho "unzipping ${_zip_file} on ${_host}"
  decho "ssh ${_user}@${_host} \"sudo unzip -o ${_remote_dir}/${_zip_file} -d ${_remote_dir}\""
  ssh ${_user}@${_host} "sudo unzip -o ${_remote_dir}/${_zip_file} -d ${_remote_dir}" || die "unable to unzip ${_zip_file} in ${_remote_dir} on ${_host}, as ${_user}"
  decho ""
}

# after unzipping it, we move the zip file to it is unzipped directory
function move_zip_file () {
  local _user=${1}
  local _host=${2}
  local _remote_dir=${3}
  local _zip_file=${4}
  local _version=${5}
  decho "moving zip file"
  decho "ssh ${_user}@${_host} \"sudo mv ${_remote_dir}/${_zip_file} ${_remote_dir}/${_version}\""
  ssh ${_user}@${_host} "sudo mv ${_remote_dir}/${_zip_file} ${_remote_dir}/${_version}" || die "unable to move ${_zip_file} to ${_version} subdirectory on ${_host}"
  decho ""
}

# clean up remote subdirectory
function clean_remote_subdir () {
  local _user=${1}
  local _host=${2}
  local _remote_dir=${3}
  local _version=${4}
  # move content of subdirectory directly into _remote_dir
  decho "ssh ${_user}@${_host} \"sudo mv ${_remote_dir}/${_version}/* ${_remote_dir}/${_version}/.h* ${_remote_dir}/\""
  ssh ${_user}@${_host} "sudo mv ${_remote_dir}/${_version}/* ${_remote_dir}/${_version}/.h* ${_remote_dir}/" || die "unable to move subdirectory content on ${_host}"
  # clean up extraneous directory
  decho "ssh ${_user}@${_host} \"sudo rm -rf ${_remote_dir}/${_version}\""
  ssh ${_user}@${_host} "sudo rm -rf ${_remote_dir}/${_version}" || die "unable to remove extra subdirectory ${_remote_dir}/${_version} on ${_host}"
  decho ""
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
  make_remote_dir ${_user} ${_host} ${_remote_dir}
  rsync_zip_file ${_user} ${_host} ${_remote_dir} ${_zip_file} ${_local_dir}
  unzip_archive ${_user} ${_host} ${_remote_dir} ${_zip_file}
  decho "branch=${_branch}"
  if [ "${_branch}" == 'yes' ]; then
    move_zip_file ${_user} ${_host} ${_remote_dir} ${_zip_file} ${_version}
    clean_remote_subdir ${_user} ${_host} ${_remote_dir} ${_version}
  fi
}

################################################################################

decho "######################### DEPLOYING #########################"
decho "DEPLOY_TO_STG=${DEPLOY_TO_STG}"
decho "DEPLOY_TO_DOCS=${DEPLOY_TO_DOCS}"

### DEVELOP => Staging
if [[ "${DEPLOY_TO_STG}" == 'yes' ]]; then
  decho "Deploying to Staging server"
  deploy ${USER} ${STG_SERVER} ${REMOTE_STG_DIR} ${ZIP_FILE} ${FILE_PATH} ${VERSION} ${BRANCH}
fi

### RELEASE => Docs Servers
if [[ "${DEPLOY_TO_DOCS}" == 'yes' ]]; then
  decho "Deploying to Docs servers"
  for i in ${DOCS_SERVERS}; do
    deploy ${USER} ${i} ${REMOTE_DOCS_DIR} ${ZIP_FILE} ${FILE_PATH} ${VERSION} ${BRANCH}
  done
fi
decho "####################### DEPLOYING DONE #######################"


#!/usr/bin/env bash
#
# Copyright © 2012-2015 Cask Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
######################################################################################
# Receive artifacts and upload them directly to docs1 (or docs2 if there is an issue?)
# This script expects to receive 3 parameters in this order:
#   $1 user                     REMOTE_USER
#   $2 remote host              REMOTE_HOST
#   $3 target directory         REMOTE_INCOMING_DIR
######################################################################################
#DRY_RUN=--dry-run		### uncomment to only test what the rsync would do
DEBUG=${DEBUG:-no}              ### set to yes for debugging

# Vars
RUN_DATE=`date '+%Y%m%d_%R'`
SCRIPT=`basename ${BASH_SOURCE[0]}`                     ### Set Script Name variable
REMOTE_USER=${1}                                        ### remote user
REMOTE_HOST=${2:-127.0.0.1}                             ### remote host
REMOTE_INCOMING_DIR=${3}                                ### target directory on remote host
REMOTE_BASE_DIR="${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_INCOMING_DIR}"
BUILD_RELEASE_DIRS='*/target'                           ### Source directories
BUILD_PACKAGE=${BUILD_PACKAGE:-cdap}
#############################
# find top of repo
find_repo_root() {
  while test -z ${__repo_root} ; do
    t_root=${PWD}
    if [[ $(ls -a1 | grep -e '\.git$') ]] ; then
      __repo_root=${t_root}
    else
      cd ..
    fi
  done
}

# output trimmer
decho () {
  if [[ "${DEBUG}" == 'yes' ]]; then
    echo ${*}
  else
    RSYNC_QUIET='--quiet'
  fi
}

# Help function
function HELP {
  echo -e \\n"Help documentation for ${BOLD}${SCRIPT}.${NORM}"\\n
  echo -e "${REV}Basic usage:${NORM} ${BOLD}$SCRIPT${NORM} <remote_user> <remote_host> <remote_target_directory"\\n
  echo -e "${REV}-h${NORM}  --Displays this help message. No further functions are performed."\\n
  exit 1
}

die ( ) { echo ; echo "ERROR: ${*}" ; echo ; exit 1; }

###############################################################################
# sync any rpm/deb
function sync_build_artifacts_to_server () {
  _source=$1
  echo "source directories: ${_source}"

  decho "identify packages"
  PACKAGES=$(find ${_source} -type f \( -name '*.rpm' -o -name '*.deb' \) | sort -u)
  decho "${PACKAGES}"
  decho ""

  # copy packages
  decho "copy packages"
  for i in ${PACKAGES}
  do
    decho "PACKAGE=${i}"
    _package=`basename ${i}`
    _snapshot_time=''

    ##
    # most CDAP Debian packages will look like this: (typical, not just in CDAP)
    #      cdap-blah-blah-blah_2.8.0-1_all.deb
    #      cdap-blah-blah-blah_2.8.0.1427234000548-1_all.deb
    #    Pattern: <package name>_<version>-<build_revision>_all.deb
    #
    # most CDAP RPM packages will look like this:
    #      cdap-blah-blah-blah-2.8.0-1.noarch.rpm
    #      cdap-blah-blah-blah-2.8.0.1427234000548-1.noarch.rpm
    #    Pattern: <package name>-<version>-<build_revision>.noarch.rpm
    #
    # Where version = <Major>.<Minor>.<Patch>(.optional time stamp)(-optional build revision)
    ##

    # grab version stub
    if [[ "${_package}" == *_all.deb ]]
    then
      _version_stub=`echo ${_package} | sed 's/cdap.*_\(.*\)_all.deb/\1/'`
    elif [[ "${_package}" == *.noarch.rpm ]]
    then
      _version_stub=`echo ${_package} | sed 's/cdap.*-\(.*-.*\).noarch.rpm/\1/'`
    else # send failure notification because package file has an unexpected format/pattern
      decho "something is wrong this this package"
      die "package ${_package} has the wrong format: ${!}"
    fi

    # filter out version in 2 steps: 
    #   remove trailing build revision (e.g. -1)
    #   keep just the first 3 digits in case name includes timestamp
    decho "version stub=${_version_stub}"
    _version=`echo ${_version_stub} | awk -F - '{ print $1 }' | awk -F . '{ print $1"."$2"."$3 }'`
    decho "version = ${_version}"
    _snapshot_time=`echo ${_version_stub} | awk -F - '{ print $1 }' | sed 's/[0-9]\.[0-9]\.[0-9][\.]*\([0-9]*\)/\1/'`
    if [ "${_version}" == "${_snapshot_time}" ]; then
      _snapshot_time=''
    fi
    decho "snapshot time = ${_snapshot_time}"

    # identify and create remote incoming directory
    if [ "${_snapshot_time}" == '' ]; then
      OUTGOING_DIR=${BUILD_PACKAGE}/${_version}
    else
      OUTGOING_DIR=snapshot/cask/${BUILD_PACKAGE}/${_version}  ## send snapshots to a different directory
    fi
    echo "Create remote directory ${REMOTE_INCOMING_DIR}/${OUTGOING_DIR} if necessary"
    ssh -l ${REMOTE_USER} ${REMOTE_HOST} "mkdir -p ${REMOTE_INCOMING_DIR}/${OUTGOING_DIR}" || die "could not create remote directory"

    # sync package(s) to remote server
    decho "rsyncing with rsync -av -e \"ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null\" ${RSYNC_QUIET} ${i} ${REMOTE_BASE_DIR}/${OUTGOING_DIR}/ ${DRY_RUN} 2>&1"
    rsync -av -e "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null" ${RSYNC_QUIET} ${i} ${REMOTE_BASE_DIR}/${OUTGOING_DIR}/ ${DRY_RUN} 2>&1 || die "could not rsync ${_package} as ${REMOTE_USER} to ${REMOTE_HOST}: ${!}"
    decho ""
  done
}

######################################################################################
decho "STARTING"

find_repo_root && cd ${__repo_root}  ### this takes us to the right place (top of the repo)

# Check number of arguments. If <3 are passed, print help and exit.
NUMARGS=$#
decho -e \\n"Number of arguments: ${NUMARGS}"
if [ ${NUMARGS} -lt 3 ]; then
  HELP
fi

decho "#######################################################################################"
echo "Syncing build release src directory ${BUILD_RELEASE_DIRS} RPMs/DEBs to ${REMOTE_HOST}"
sync_build_artifacts_to_server "${BUILD_RELEASE_DIRS}"

exit 0

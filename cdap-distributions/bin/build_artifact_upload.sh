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
    _package=`echo ${i} | awk -F / '{ print $(NF) }'`

    case $_package in

      cdap.*_[0-9]\.[0-9]\.[0-9]\.[0-9]+.*deb | cdap.*-[0-9]\.[0-9]\.[0-9]\.[0-9]+.*rpm )  ## snapshots with timestamp
        _version_stub=`echo ${_package} | awk -F _ '{ print $2 }'`
        ;;

      *_all.deb )                             ## debian standard
        _version_stub=`echo ${_package} | awk -F _ '{ print $2 }' | awk -F - '{ print $1 }'`
        ;;

      *noarch.rpm )                           ## rpm standard
        _version_stub=`echo ${_package} | awk -F - '{ print $(NF-1) }'`
        ;;

      *_* )                                   ## default debian case
        _version_stub=`echo ${_package} | awk -F _ '{ print $2 }'`
        ;;

      *rpm | * )                              ## default RPM case and catchall
        _version_stub=`echo ${_package} | awk -F - '{ print $(NF) }'`
        ;;

    esac

    _version=`echo ${_version_stub} | awk -F . '{ print $1"."$2"."$3 }'`
    decho "version = ${_version}"
    OUTGOING_DIR=${BUILD_PACKAGE}/${_version}

    decho "rsyncing with rsync -av -e \"ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null\" ${RSYNC_QUIET} ${i} ${REMOTE_BASE_DIR}/${OUTGOING_DIR} ${DRY_RUN} 2>&1"
    rsync -av -e "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null" ${RSYNC_QUIET} ${i} ${REMOTE_BASE_DIR}/${OUTGOING_DIR} ${DRY_RUN} 2>&1 || die "could not rsync ${_package} as ${REMOTE_USER} to ${REMOTE_HOST}: ${!}"
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

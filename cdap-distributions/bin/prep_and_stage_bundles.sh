#!/usr/bin/env bash
#
# Copyright © 2014-2015 Cask Data, Inc.
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
# Create RPM and Debian bundles and upload them directly to docs1
# This script expects to receive 5 parameters in this order:
#   $1 BUILD_PACKAGE    e.g. cdap
#   $2 VERSION    e.g. 3.0.4
#   $3 USER                     REMOTE_USER
#   $4 remote host              REMOTE_HOST
#   $5 target directory         REMOTE_INCOMING_DIR
######################################################################################
DEBUG=${DEBUG:-no}              ### set to yes for debugging
RUN_DATE=`date '+%Y%m%d_%R'`
SCRIPT=`basename ${BASH_SOURCE[0]}`                     ### Set Script Name variable
BUILD_PACKAGE=${1}                                    ## e.g. cdap ## a.k.a. BUILD_PACKAGE
VERSION=${2} ## e.g. 3.0.4
REMOTE_USER=${3}                                        ### remote user
REMOTE_HOST=${4:-127.0.0.1}                             ### remote host
REMOTE_INCOMING_DIR=${5}                                ### target directory on remote host
BUILD_PACKAGE=${BUILD_PACKAGE:-cdap}
REMOTE_BASE_DIR="incoming"
USER=${USER:-bamboo}
TMP_DIR='/tmp/bundles'
S3STG_DIR=${S3STG_DIR:-/data/s3stg} # change it #####
DEB_BUNDLE="${BUILD_PACKAGE}-deb-bundle"
RPM_BUNDLE="${BUILD_PACKAGE}-rpm-bundle"
DEB_BUNDLE_DIR="${TMP_DIR}/${DEB_BUNDLE}"
RPM_BUNDLE_DIR="${TMP_DIR}/${RPM_BUNDLE}"
DISTRIBUTED_DEB_BUNDLE="${BUILD_PACKAGE}-distributed-deb-bundle"
DISTRIBUTED_RPM_BUNDLE="${BUILD_PACKAGE}-distributed-rpm-bundle"
DEB_BUNDLE_TGZ="${DISTRIBUTED_DEB_BUNDLE}-${VERSION}.tgz"
RPM_BUNDLE_TGZ="${DISTRIBUTED_RPM_BUNDLE}-${VERSION}.tgz"
DEB_BUNDLE_TGZ_PATH="${DEB_BUNDLE_DIR}/${DEB_BUNDLE_TGZ}"
RPM_BUNDLE_TGZ_PATH="${RPM_BUNDLE_DIR}/${RPM_BUNDLE_TGZ}"
S3STG_INCOMING="${S3STG_DIR}/incoming"
S3STG_INCOMING_PROJ="${S3STG_INCOMING}/${BUILD_PACKAGE}"
STG_DIR="${S3STG_INCOMING}/downloads/co/cask/${BUILD_PACKAGE}"
STG_DEB_DIR="${STG_DIR}/${DISTRIBUTED_DEB_BUNDLE}/${VERSION}"
STG_RPM_DIR="${STG_DIR}/${DISTRIBUTED_RPM_BUNDLE}/${VERSION}"
echo ${STG_RPM_DIR}

#############################
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

######################################################################################

clean() { test -d ${TMP_DIR} && rm -rf ${TMP_DIR}; };

if [ $# -lt 2 ]; then
  die "You need to pass project and version to ${_script}"$'\n\n'"Example: ./${_script} cdap 3.0.0"
fi

#########################################################################
create_bundles() {

   ### PREP
   # make sure bundle directories exist
   mkdir -p ${STG_DIR} || die "Unable to create STG_DIR"
   mkdir -p ${DEB_BUNDLE_DIR} ${RPM_BUNDLE_DIR} || die "Unable to create bundle processing directories"
   mkdir -p ${STG_DEB_DIR} ${STG_RPM_DIR} || die "Unable to create bundle staging directories"
   OUTGOING_DIR=${BUILD_PACKAGE}/${_version}
   echo ${OUTGOING_DIR}
   
   ### FIND ARTIFACTS
   mkdir -p ${S3STG_INCOMING_PROJ} || die "Unable to create project staging directory"
   cd ${S3STG_INCOMING_PROJ} || die "Unable to cd to ${S3STG_INCOMING_PROJ} directory"
   find . -name "${BUILD_PACKAGE}-*${VERSION}-*.deb" -exec cp '{}' ${DEB_BUNDLE_DIR}/. \; || die "Unable to copy Debian ${VERSION} packages to TMP directory"
   find . -name "${BUILD_PACKAGE}-*${VERSION}-*.rpm" -exec cp '{}' ${RPM_BUNDLE_DIR}/. \; || die "Unable to copy ${VERSION} RPMs to TMP directory"
   
   # create and verify debian tarball
   cd ${DEB_BUNDLE_DIR} || die "Unable to cd to ${DEB_BUNDLE_DIR} directory"
   tar czvf ${DEB_BUNDLE_TGZ} * || die "Unable to create Debian tar bundle"
   tar tzvf ${DEB_BUNDLE_TGZ} || die "Unable to show contents of Debian tar bundle"
   
   # create and verify RPM tarball
   cd ${RPM_BUNDLE_DIR} || die "Unable to cd to ${RPM_BUNDLE_DIR} directory"
   tar czvf ${RPM_BUNDLE_TGZ} * || die "Unable to create RPM tar bundle"
   tar tzvf ${RPM_BUNDLE_TGZ} || die "Unable to show contents of RPM tar bundle"
   
   # SYNC bundle(s) to remote server
   decho "rsyncing with rsync -av -e \"ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null\" ${RSYNC_QUIET} ${DEB_BUNDLE_TGZ_PATH} ${STG_DEB_DIR}/.  2>&1"
   decho "rsyncing with rsync -av -e \"ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null\" ${RSYNC_QUIET} ${RPM_BUNDLE_TGZ_PATH} ${STG_RPM_DIR}/.  2>&1"
   rsync -av -e "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null" ${RSYNC_QUIET} ${DEB_BUNDLE_TGZ_PATH} ${STG_DEB_DIR}/. 2>&1 || die "could not rsync ${DEB_BUNDLE_TGZ_PATH} as ${REMOTE_USER} to ${REMOTE_HOST}: ${!}"
   rsync -av -e "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null" ${RSYNC_QUIET} ${RPM_BUNDLE_TGZ_PATH} ${STG_RPM_DIR}/. 2>&1 || die "could not rsync ${RPM_BUNDLE_TGZ_PATH} as ${REMOTE_USER} to ${REMOTE_HOST}: ${!}"

   # clean up
   #clean
}

######################################################################################
decho "STARTING"

# Check number of arguments. If <5 are passed, print help and exit.
NUMARGS=$#
decho -e \\n"Number of arguments: ${NUMARGS}"
if [ ${NUMARGS} -lt 5 ]; then
  HELP
fi

decho "#######################################################################################"
echo "Creating RPM and Debian bundles"
create_bundles

exit 0


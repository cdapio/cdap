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
# This script expects to receive 3 parameters in this order:
#   $1 remote user              REMOTE_USER
#   $2 remote host              REMOTE_HOST
#   $3 target directory         REMOTE_INCOMING_DIR
######################################################################################
_script=`basename $0`
DEBUG=${DEBUG:-no}              ### set to yes for debugging
RUN_DATE=`date '+%Y%m%d_%R'`
SCRIPT=`basename ${BASH_SOURCE[0]}`                     ### Set Script Name variable
BUILD_PACKAGE='cdap'                                   ## e.g. cdap ## a.k.a. BUILD_PACKAGE
VERSION=$(<cdap-distributions/target/stage-packaging/opt/cdap/distributions/VERSION)
REMOTE_USER=${1}                                        ### remote user
REMOTE_HOST=${2:-127.0.0.1}                             ### remote host
REMOTE_INCOMING_DIR=${3}                                ### target directory on remote host
TMP_DIR='/tmp/bundles'
S3STG_DIR=${S3STG_DIR:-/data/s3stg}
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
S3STG_INCOMING="${S3STG_INCOMING:-/data/s3stg/incoming}"
STG_DIR="${S3STG_INCOMING}/downloads/co/cask/${BUILD_PACKAGE}"
STG_DEB_DIR="${STG_DIR}/${DISTRIBUTED_DEB_BUNDLE}/${VERSION}"
STG_RPM_DIR="${STG_DIR}/${DISTRIBUTED_RPM_BUNDLE}/${VERSION}"

#############################
# output trimmer
decho () {
  if [[ "${DEBUG}" == 'yes' ]]; then
    echo ${*}
  else
    RSYNC_QUIET='--quiet'
  fi
}

die ( ) { echo ; echo "ERROR: ${*}" ; echo ; exit 1; }

######################################################################################

clean() { test -d ${TMP_DIR} && rm -rf ${TMP_DIR}; };

if [ $# -lt 3 ]; then
  die "You need to pass project and version to ${_script}"$'\n\n'"Example: ./${_script} cdap 3.0.0"
fi

#########################################################################
create_bundles() {

   ### PREP
   # make sure bundle directories exist
   mkdir -p ${STG_DIR} || die "Unable to create STG_DIR"
   mkdir -p ${DEB_BUNDLE_DIR} ${RPM_BUNDLE_DIR} || die "Unable to create bundle processing directories"
   mkdir -p ${STG_DEB_DIR} ${STG_RPM_DIR} || die "Unable to create bundle staging directories"
   OUTGOING_DIR=${BUILD_PACKAGE}/${VERSION}
   decho ${OUTGOING_DIR}
   
   ### FIND ARTIFACTS
   find ${S3STG_INCOMING}/${BUILD_PACKAGE} -name "${BUILD_PACKAGE}*_${VERSION}-*.deb" -exec cp '{}' ${DEB_BUNDLE_DIR}/. \; || die "Unable to copy Debian ${VERSION} packages to TMP directory"
   find ${S3STG_INCOMING}/${BUILD_PACKAGE} -name "${BUILD_PACKAGE}*-${VERSION}-*.rpm" -exec cp '{}' ${RPM_BUNDLE_DIR}/. \; || die "Unable to copy ${VERSION} RPMs to TMP directory"
   
   # create and verify debian tarball
   cd ${DEB_BUNDLE_DIR} || die "Unable to cd to ${DEB_BUNDLE_DIR} directory"
   tar czvf ${DEB_BUNDLE_TGZ} * || die "Unable to create Debian tar bundle"
   if [[ "${DEBUG}" == 'yes' ]]; then
     tar tzf ${DEB_BUNDLE_TGZ} || die "Unable to show contents of Debian tar bundle"
   fi
   
   # create and verify RPM tarball
   cd ${RPM_BUNDLE_DIR} || die "Unable to cd to ${RPM_BUNDLE_DIR} directory"
   tar czvf ${RPM_BUNDLE_TGZ} * || die "Unable to create RPM tar bundle"
   if [[ "${DEBUG}" == 'yes' ]]; then
     tar tzf ${RPM_BUNDLE_TGZ} || die "Unable to show contents of RPM tar bundle"
   fi
}

sync_bundles() {
   # SYNC bundle(s) to remote server
   decho 'rsyncing with rsync -av -e "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null" ${RSYNC_QUIET} ${DEB_BUNDLE_TGZ_PATH} ${STG_DEB_DIR}/.  2>&1'
   decho 'rsyncing with rsync -av -e "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null" ${RSYNC_QUIET} ${RPM_BUNDLE_TGZ_PATH} ${STG_RPM_DIR}/.  2>&1'
   rsync -av -e "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null" ${RSYNC_QUIET} ${DEB_BUNDLE_TGZ_PATH} ${STG_DEB_DIR}/. 2>&1 || die "could not rsync ${DEB_BUNDLE_TGZ_PATH} as ${REMOTE_USER} to ${REMOTE_HOST}: ${!}"
   rsync -av -e "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null" ${RSYNC_QUIET} ${RPM_BUNDLE_TGZ_PATH} ${STG_RPM_DIR}/. 2>&1 || die "could not rsync ${RPM_BUNDLE_TGZ_PATH} as ${REMOTE_USER} to ${REMOTE_HOST}: ${!}"

   # clean up
   clean
}

######################################################################################
decho "STARTING"

# Check number of arguments. If <3 are passed, print help and exit.
NUMARGS=$#
decho -e \\n"Number of arguments: ${NUMARGS}"
if [ ${NUMARGS} -lt 3 ]; then
  echo "wrong number of arguments: ${NUMARGS} instead of 3"
  exit 1
fi

decho "#######################################################################################"
echo "Creating RPM and Debian bundles"
create_bundles

echo "Syncing bundles to staging"
sync_bundles


exit 0

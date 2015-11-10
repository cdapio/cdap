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
RUN_DATE=`date '+%Y%m%d_%R'`
SCRIPT=`basename ${BASH_SOURCE[0]}`                     ### Set Script Name variable
VERSION=$(<cdap-distributions/target/stage-packaging/opt/cdap/distributions/VERSION)
REMOTE_USER=${1}                                        ### remote user
REMOTE_HOST=${2:-127.0.0.1}                             ### remote host
REMOTE_INCOMING_DIR=${3}      # 'incoming'                  ### target directory on remote host
REMOTE_BASE_DIR="${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_INCOMING_DIR}"
BUILD_RELEASE_DIRS='*/target'                           ### Source directories
BUILD_PACKAGE=${BUILD_PACKAGE:-cdap}
TMP_DIR="/tmp/bundles"
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
REMOTE_DOWNLOADS_DIR="downloads/co/cask/${BUILD_PACKAGE}" 
STG_DEB_DIR="${REMOTE_DOWNLOADS_DIR}/${DISTRIBUTED_DEB_BUNDLE}/${VERSION}"
STG_RPM_DIR="${REMOTE_DOWNLOADS_DIR}/${DISTRIBUTED_RPM_BUNDLE}/${VERSION}"

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

clean() { test -d ${TMP_DIR} && rm -rf ${TMP_DIR}; }
die() { echo ; echo "ERROR: ${*}" ; echo ; exit 1; }

########################################################

bundle_prep() {
  ### PREP
  decho ''
  decho 'prep bundles'
  # create local processing directories
  mkdir -p ${DEB_BUNDLE_DIR} ${RPM_BUNDLE_DIR} || die "Unable to create bundle processing directories"
  # create remote bundle downloads directories
  decho "Create remote directories ${STG_DEB_DIR} ${STG_RPM_DIR} if necessary"
  ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -l ${REMOTE_USER} ${REMOTE_HOST} "mkdir -p ${STG_DEB_DIR} ${STG_RPM_DIR}" || die "could not create remote directories"
}

create_bundles() {
   decho 'Create Debian bundle'
   # create and verify debian tarball
   cd ${DEB_BUNDLE_DIR} || die "Unable to cd to ${DEB_BUNDLE_DIR} directory"
   decho "debbundledir=${DEB_BUNDLE_DIR}"
   tar czvf ${DEB_BUNDLE_TGZ} * || die "Unable to create Debian tar bundle"
   if [[ "${DEBUG}" == 'yes' ]]; then
     tar tzf ${DEB_BUNDLE_TGZ} || die "Unable to show contents of Debian tar bundle"
   fi

   decho ''
   decho 'Create RPM bundle'
   # create and verify RPM tarball
   decho "rpmbundledir=${RPM_BUNDLE_DIR}"
   cd ${RPM_BUNDLE_DIR} || die "Unable to cd to ${RPM_BUNDLE_DIR} directory"
   tar czvf ${RPM_BUNDLE_TGZ} * || die "Unable to create RPM tar bundle"
   if [[ "${DEBUG}" == 'yes' ]]; then
     tar tzf ${RPM_BUNDLE_TGZ} || die "Unable to show contents of RPM tar bundle"
   fi
}

sync_bundles() {
   ### COPY TO STAGING
   decho ''
   decho "Syncing bundles to staging"
   decho "rsync -av -e \"ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null\" ${RSYNC_QUIET} ${DEB_BUNDLE_TGZ_PATH} ${REMOTE_BASE_DIR}/${STG_DEB_DIR}/ ${DRY_RUN} 2>&1 || die could not rsync ${_package} as ${REMOTE_USER} to ${REMOTE_HOST}: ${!}"
   rsync -av -e "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null" ${RSYNC_QUIET} ${DEB_BUNDLE_TGZ_PATH} ${REMOTE_BASE_DIR}/${STG_DEB_DIR}/ ${DRY_RUN} 2>&1 || die "could not rsync ${DEB_BUNDLE_TGZ_PATH} as ${REMOTE_USER} to ${REMOTE_HOST}: ${!}"

   rsync -av -e "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null" ${RSYNC_QUIET} ${RPM_BUNDLE_TGZ_PATH} ${REMOTE_BASE_DIR}/${STG_RPM_DIR}/ ${DRY_RUN} 2>&1 || die "could not rsync ${RPM_BUNDLE_TGZ_PATH} as ${REMOTE_USER} to ${REMOTE_HOST}: ${!}"

   # clean up
   #clean
}

###############################################################################
# identify and sync any rpm/deb
function sync_build_artifacts_to_server () {
  _source=$1
  decho "source directories: ${_source}"

  clean

  bundle_prep

  decho "identify packages"
  PACKAGES=$(find ${_source} -type f \( -name '*.rpm' -o -name '*.deb' \) | sort -u)
  DEB_PACKAGES=$(find ${_source} -type f \( -name '*.deb' \) | sort -u)
  RPM_PACKAGES=$(find ${_source} -type f \( -name '*.rpm' \) | sort -u)
  decho "${PACKAGES}"
  decho ""

  cp ${DEB_PACKAGES} ${DEB_BUNDLE_DIR}
  cp ${RPM_PACKAGES} ${RPM_BUNDLE_DIR}

  create_bundles

  sync_bundles

  # copy packages
  decho "copy packages"
  for i in ${PACKAGES}
  do
    _package=`basename ${i}`
    decho "PACKAGE=${i}    package=${_package}"
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

    # store the packages in a temporary directory

    # grab version stub
    if [[ "${_package}" == *_all.deb ]]
    then
      _version_stub=`echo ${_package} | sed 's/cdap.*_\(.*\)_all.deb/\1/'`
      LOCAL_DIR=${DEB_BUNDLE_DIR}
    elif [[ "${_package}" == *.noarch.rpm ]]
    then
      _version_stub=`echo ${_package} | sed 's/cdap.*-\(.*-.*\).noarch.rpm/\1/'`
      LOCAL_DIR=${RPM_BUNDLE_DIR}
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
    decho "Create remote directory ${REMOTE_INCOMING_DIR}/${OUTGOING_DIR} if necessary"
    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -l ${REMOTE_USER} ${REMOTE_HOST} "mkdir -p ${REMOTE_INCOMING_DIR}/${OUTGOING_DIR}" || die "could not create remote directory"

    # sync package(s) to remote server
    decho "rsyncing with rsync -av -e \"ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null\" ${RSYNC_QUIET} ${LOCAL_DIR}/${_package} ${REMOTE_BASE_DIR}/${OUTGOING_DIR}/ ${DRY_RUN} 2>&1"
    rsync -av -e "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null" ${RSYNC_QUIET} ${LOCAL_DIR}/${_package} ${REMOTE_BASE_DIR}/${OUTGOING_DIR}/ ${DRY_RUN} 2>&1 || die "could not rsync ${_package} as ${REMOTE_USER} to ${REMOTE_HOST}: ${!}"
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
  echo "wrong number of arguments: ${NUMARGS} instead of 3"
  exit 1
fi

decho "#######################################################################################"
echo "Syncing build release src directory ${BUILD_RELEASE_DIRS} RPMs/DEBs to ${REMOTE_HOST}"
sync_build_artifacts_to_server "${BUILD_RELEASE_DIRS}"

exit 0

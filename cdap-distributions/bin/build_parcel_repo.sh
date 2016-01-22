#!/usr/bin/env bash
#
# Copyright © 2016 Cask Data, Inc.
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

# Logic:
# Get version
# s3cmd ls to see if repo exists
# - yes, sync repo local
# - no, create repo dir
# copy new parcel into repo dir
# symlink parcels
# create manifest

# Find our location and base repo directory
# Resolve links: $0 may be a link
PRG=${0}
# Need this for relative symlinks.
while [ -h ${PRG} ]; do
    ls=`ls -ld ${PRG}`
    link=`expr ${ls} : '.*-> \(.*\)$'`
    if expr ${link} : '/.*' > /dev/null; then
        PRG=${link}
    else
        PRG=`dirname ${PRG}`/${link}
    fi
done
cd `dirname ${PRG}`/.. >&-
DISTRIBUTIONS_HOME=`pwd -P`
cd `dirname ${DISTRIBUTIONS_HOME}` >&-
REPO_HOME=`pwd -P`

TARGET_DIR=${DISTRIBUTIONS_HOME}/target
STAGE_DIR=${TARGET_DIR}/parcelrepo

S3_BUCKET=${S3_BUCKET:-repository.cask.co}
S3_REPO_PATH=${S3_REPO_PATH:-parcels/cdap} # No leading or trailing slashes
VERSION=${VERSION:-$(basename ${TARGET_DIR}/*.parcel | head -n 1 | cut -d- -f2)} # Assumes NAME-${VERSION}*
__version=${VERSION/-SNAPSHOT/}
__maj_min=$(echo ${__version} | cut -d. -f1,2)

function die() { echo "ERROR: ${1}" 1>&2; exit 1; }

function repo_exists() {
  if [[ $(s3cmd ls s3://${S3_BUCKET}/${S3_REPO_PATH}/ | grep ${__maj_min}) ]]; then
    return 0
  else
    return 1
  fi
}

function setup_repo_staging() {
  mkdir -p ${STAGE_DIR}/${__maj_min} || return 1
  if repo_exists; then
    echo "Found existing repository at s3://${S3_BUCKET}/${S3_REPO_PATH}/${__maj_min}... copying to staging directory"
    s3cmd sync --no-preserve s3://${S3_BUCKET}/${S3_REPO_PATH}/${__maj_min} ${STAGE_DIR}
    return $?
  fi
  return 0
}

function add_parcels_to_repo_staging() {
  echo "Copying parcels to ${STAGE_DIR}/${__maj_min}"
  cd ${STAGE_DIR}/${__maj_min}
  for __parcel in $(ls -1 ${TARGET_DIR}/CDAP-*.parcel 2>/dev/null); do
    echo "- ${__parcel}"
    cp -f ${__parcel} .
  done
  # We only build el6, so use that as key
  for p in $(ls -1 CDAP-*-el6.parcel) ; do
    for d in precise trusty wheezy ; do
      ln -sf ${p} ${p/el6/${d}} || (echo "Failed to symlink ${p/el6/${d}} to ${p}" && return 1)
    done
  done
}

function make_manifest() {
  if [ -x ${DISTRIBUTIONS_HOME}/bin/make_manifest.rb ]; then
    echo "Using Ruby port of make_manifest.py"
    ${DISTRIBUTIONS_HOME}/bin/make_manifest.rb ${*} || return 1
  else
    echo "Using Python make_manifest.py and assuming it's in PATH"
    make_manifest.py ${*} || return 1
  fi
}

function make_manifest_in_repo_staging() {
  cd ${STAGE_DIR}
  echo "Creating repository ${__maj_min} in staging directory"
  make_manifest ${__maj_min}
}

# Here we go!
setup_repo_staging || die "Something went wrong setting up the staging directory"
add_parcels_to_repo_staging || die "Failed copying parcels to staging directory"
make_manifest_in_repo_staging || die "Failed to create repository from staging directory"

cp -f ${STAGE_DIR}/${__maj_min}/manifest.json ${TARGET_DIR}

echo "Complete: ${STAGE_DIR}/${__maj_min} repository created"
exit 0 # We made it!

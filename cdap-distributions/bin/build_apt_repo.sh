#!/usr/bin/env bash
#
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

# Logic:
# Get version
# s3cmd ls to see if repo exists
# - yes, sync repo local
# - no, create repo dir
# copy new packages into repo dir
# create repo
# sign repo
# create tarball

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
STAGE_DIR=${TARGET_DIR}/aptrepo

S3_BUCKET=${S3_BUCKET:-repository.cask.co}
S3_REPO_PATH=${S3_REPO_PATH:-ubuntu/precise/amd64/cdap} # No leading or trailing slashes
VERSION=${VERSION:-$(basename ${TARGET_DIR}/cdap_*.deb | cut -d_ -f2 | cut -d- -f1)}
GPG_KEY_NAME=${GPG_KEY_NAME:-${1}}
GPG_PASSPHRASE=${GPG_PASSPHRASE:-${2}}
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
  mkdir -p ${STAGE_DIR} || return 1
  # Create initial Freight repository
  freight-init \
    --gpg=${GPG_KEY_NAME} \
    --conf=${STAGE_DIR}/freight.conf \
    --libdir=${STAGE_DIR}/lib \
    --cachedir=${STAGE_DIR}/${__maj_min} \
    --archs=amd64 \
    --origin=Cask \
    --label=Cask \
    ${STAGE_DIR} || return $?
  # Sync old repository
  if repo_exists; then
    echo "Found existing repository at s3://${S3_BUCKET}/${S3_REPO_PATH}/${__maj_min}... copying to staging directory"
    mkdir -p ${STAGE_DIR}/__tmpdir
    s3cmd sync --no-preserve s3://${S3_BUCKET}/${S3_REPO_PATH}/${__maj_min} ${STAGE_DIR}/__tmpdir
    return $?
  fi
  return 0
}

function add_packages_to_repo_staging() {
  echo "Adding packages to freight library"
  mkdir -p ${STAGE_DIR}/lib/apt/precise/cdap || return 1
  # __old_repo = ${STAGE_DIR}/__tmpdir/${__maj_min}/pool/${UBUNTU_RELEASE_NAME}/${APT_COMPONENT}/[:alpha:]/${PACKAGE_NAME}/*.deb
  local __old_repo=$(ls -1 ${STAGE_DIR}/__tmpdir/*/*/*/*/*/*/*.deb 2>/dev/null)
  if [ -n "${__old_repo}" ]; then
    for __deb in ${__old_repo}; do
      cp -f ${__deb} ${STAGE_DIR}/lib/apt/precise/cdap
    done
  fi
  for __deb in $(ls -1 ${REPO_HOME}/*/target/*.deb ${REPO_HOME}/target/*.deb 2>/dev/null); do
    echo "- ${__deb}"
    cp -f ${__deb} ${STAGE_DIR}/lib/apt/precise/cdap
  done
}

function write_gpg_passphrase() {
  echo ${GPG_PASSPHRASE} > ${STAGE_DIR}/gpgpass.tmp
}

function createrepo_in_repo_staging() {
  write_gpg_passphrase || return 1
  echo "Creating repository ${__maj_min} in staging directory"
  freight-cache --conf=${STAGE_DIR}/freight.conf --gpg=${GPG_KEY_NAME} --passphrase-file=${STAGE_DIR}/gpgpass.tmp apt/precise || return 1
  # Replace symlink
  rm -rf ${STAGE_DIR}/${__maj_min}/dists/precise{/.refs,} && mv ${STAGE_DIR}/${__maj_min}/dists/precise{-*,} || return 1
}

function create_repo_tarball() {
  cd ${STAGE_DIR}
  echo "Create APT repository tarball"
  # Delete any previous tarballs
  rm -f ${TARGET_DIR}/cdap-aptrepo-${__maj_min}.tar.gz
  tar zcf ${TARGET_DIR}/cdap-aptrepo-${__maj_min}.tar.gz ${__maj_min}
}

# Here we go!
setup_repo_staging || die "Something went wrong setting up the staging directory"
add_packages_to_repo_staging || die "Failed copying packages to staging directory"
createrepo_in_repo_staging || die "Failed to create repository from staging directory"
create_repo_tarball || die "Failed to create APT repository tarball"

echo "Complete: cdap-aptrepo-${__maj_min}.tar.gz created"
exit 0 # We made it!

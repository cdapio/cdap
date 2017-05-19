#!/usr/bin/env bash
#
# Copyright © 2015-2017 Cask Data, Inc.
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
# sign packages
# create repo
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
STAGE_DIR=${TARGET_DIR}/yumrepo

S3_BUCKET=${S3_BUCKET:-repository.cask.co}
S3_REPO_PATH=${S3_REPO_PATH:-centos/6/x86_64/cdap} # No leading or trailing slashes
VERSION=${VERSION:-$(ls -1 ${TARGET_DIR}/*.rpm | head -n 1 | xargs rpm --queryformat "%{VERSION}" -qp)} # query package file
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
  mkdir -p ${STAGE_DIR}/${__maj_min} || return 1
  if repo_exists; then
    echo "Found existing repository at s3://${S3_BUCKET}/${S3_REPO_PATH}/${__maj_min}... copying to staging directory"
    s3cmd sync --no-preserve s3://${S3_BUCKET}/${S3_REPO_PATH}/${__maj_min} ${STAGE_DIR}
    return $?
  fi
  return 0
}

function add_packages_to_repo_staging() {
  mkdir -p ${STAGE_DIR}/${__maj_min}/rpms
  echo "Copying packages to ${STAGE_DIR}/${__maj_min}/rpms"
  for __rpm in $(ls -1 ${REPO_HOME}/*/target/*.rpm ${REPO_HOME}/target/*.rpm 2>/dev/null); do
    echo "- ${__rpm}"
    cp -f ${__rpm} ${STAGE_DIR}/${__maj_min}/rpms
  done
}

function verify_signature_on_package() {
  local __rpm=${1}
  rpm --checksig ${__rpm} | grep -e pgp -e gpg # We want grep's return code, not rpm's
}

function sign_rpm_package() {
  local __rpm=${1}
  ${DISTRIBUTIONS_HOME}/bin/rpmsign.expect ${GPG_KEY_NAME} ${GPG_PASSPHRASE} ${__rpm}
}

function sign_packages_in_repo_staging() {
  [ -z "${GPG_KEY_NAME}" -o -z "${GPG_PASSPHRASE}" ] && return
  cd ${STAGE_DIR}/${__maj_min}/rpms
  local __rpm __ret
  for __rpm in $(ls -1 *.rpm); do
    verify_signature_on_package ${__rpm}
    __ret=$?
    if [ ${__ret} -eq 0 ]; then
      # Package is already signed, hooray!
      echo "Signature verified for ${__rpm}"
      continue
    else
      # Sign away
      sign_rpm_package ${__rpm}
    fi
  done
}

function export_public_key() {
  [ -z "${GPG_KEY_NAME}" -o -z "${GPG_PASSPHRASE}" ] && return
  cd ${STAGE_DIR}/${__maj_min}
  echo "Exporting GPG key to staging directory"
  gpg --armor --export ${GPG_KEY_NAME} > pubkey.gpg
}

function createrepo_in_repo_staging() {
  cd ${STAGE_DIR}
  echo "Creating repository ${__maj_min} in staging directory"
  createrepo ${__maj_min}
}

function create_definition_file() {
  [ -f "${STAGE_DIR}/${__maj_min}/cask.repo" ] && return
  echo "Create YUM repository definition file"
  cd ${STAGE_DIR}/${__maj_min}
  cat <<EOF > cask.repo
[cask]
name=Cask Packages
baseurl=https://${S3_BUCKET}/${S3_REPO_PATH}/${__maj_min}
enabled=1
gpgcheck=1
EOF
}

function create_repo_tarball() {
  cd ${STAGE_DIR}
  echo "Create YUM repository tarball"
  # Delete any previous tarballs
  rm -f ${TARGET_DIR}/cdap-yumrepo-${__maj_min}.tar.gz
  tar zcf ${TARGET_DIR}/cdap-yumrepo-${__maj_min}.tar.gz ${__maj_min}
}

# Here we go!
setup_repo_staging || die "Something went wrong setting up the staging directory"
add_packages_to_repo_staging || die "Failed copying packages to staging directory"
sign_packages_in_repo_staging
export_public_key
createrepo_in_repo_staging || die "Failed to create repository from staging directory"
create_definition_file || die "Failed to create repository definition file"
create_repo_tarball || die "Failed to create YUM repository tarball"

echo "Complete: cdap-yumrepo-${__maj_min}.tar.gz created"
exit 0 # We made it!

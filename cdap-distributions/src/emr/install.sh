#!/bin/bash
#
# Copyright © 2015-2016 Cask Data, Inc.
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

#
# Install CDAP for Amazon Elastic MapReduce (EMR) AMI 4.6.0+ using Amazon Hadoop
#

# CDAP config
# The git branch to clone
CDAP_BRANCH=${CDAP_BRANCH:-develop}
# Optional tag to checkout - All released versions of this script should set this
CDAP_TAG=''
# The CDAP package version passed to Chef
CDAP_VERSION=${CDAP_VERSION:-4.0.0-1}
# The version of Chef to install
CHEF_VERSION=${CHEF_VERSION:-12.10.24}
# cdap-site.xml configuration parameters
EXPLORE_ENABLED='true'

__tmpdir="/tmp/cdap_install.$$.$(date +%s)"
__gitdir="${__tmpdir}/cdap"

__packerdir="${__gitdir}/cdap-distributions/src/packer/scripts"
__cdap_site_template="${__gitdir}/cdap-distributions/src/emr/cdap-conf.json"

__repo_url=${CDAP_YUM_REPO_URL:-http://repository.cask.co/centos/6/x86_64/cdap/MAJ_MIN}

die() { echo "ERROR: ${*}"; exit 1; };

# Parse any command line options
while [[ ${#} -gt 0 ]]; do
  case ${1} in
    --cdap-version*)
      __tmp=${1#*=} # only keep after = if given
      shift
      if [[ ${__tmp} =~ ^- ]]; then # maybe version is next argument?
        __arg=${1}
        if [[ ${__arg} =~ ^- ]]; then
          echo "WARNING: --cdap-version should specify a version afterwards, using default ${CDAP_VERSION}"
          continue
        else
          CDAP_VERSION=${__arg}
          echo "INFO: Setting CDAP version to ${CDAP_VERSION}"
          shift
        fi
      else
        CDAP_VERSION=${__tmp}
        echo "INFO: Setting CDAP version to ${CDAP_VERSION}"
      fi
      ;;
    --cdap-repo-url*)
      __tmp=${1#*=}
      shift
      if [[ ${__tmp} =~ ^- ]]; then # maybe url is next argument?
        __arg=${1}
        if [[ ${__arg} =~ ^- ]]; then
          __maj_min=${CDAP_VERSION%.*-*}
          echo "WARNING: --cdap-repo-url should specify a URL afterwards, using default ${__repo_url/MAJ_MIN/${__maj_min}}"
          continue
        else
          CDAP_YUM_REPO_URL=${__arg}
          echo "INFO: Setting CDAP repository URL to ${CDAP_YUM_REPO_URL}"
          shift
        fi
      else
        CDAP_YUM_REPO_URL=${__tmp}
        echo "INFO: Setting CDAP repository URL to ${CDAP_YUM_REPO_URL}"
      fi
      ;;
    *) break ;;
  esac
done

__maj_min=${CDAP_VERSION%.*-*}

# One last sed-fu, if we're using the default CDAP_YUM_REPO_URL, in case version's been updated
CDAP_YUM_REPO_URL=${CDAP_YUM_REPO_URL:-${__repo_url/MAJ_MIN/${__maj_min}}}

__cleanup_tmpdir() { test -d ${__tmpdir} && rm -rf ${__tmpdir}; };
__create_tmpdir() { mkdir -p ${__tmpdir}; };

# Begin CDAP Prep/Install

# Install git
yum install --yes git || die "Failed to install git"

# Install chef
curl -L https://www.chef.io/chef/install.sh | sudo bash -s -- -v ${CHEF_VERSION} || die "Failed to install chef"

# Clone CDAP repo
__create_tmpdir
git clone --depth 1 --branch ${CDAP_BRANCH} https://github.com/caskdata/cdap.git ${__gitdir}

# Check out to specific tag if specified
if [ -n "${CDAP_TAG}" ]; then
  git -C ${__gitdir} checkout tags/${CDAP_TAG}
fi

# Setup cookbook repo
test -d /var/chef/cookbooks && rm -rf /var/chef/cookbooks
${__packerdir}/cookbook-dir.sh || die "Failed to setup cookbook dir"

# Install cookbooks via knife
${__packerdir}/cookbook-setup.sh || die "Failed to install cookbooks"

# CDAP cli install, ensures package dependencies are present
# We must specify the cdap version
echo "{\"cdap\": {\"version\": \"${CDAP_VERSION}\"}}" > ${__tmpdir}/cli-conf.json
chef-solo -o 'recipe[cdap::cli]' -j ${__tmpdir}/cli-conf.json

source ${__gitdir}/cdap-common/bin/functions.sh || die "Cannot source CDAP functions script"

# Read zookeeper quorum from hbase-site.xml, using sourced init script function
__zk_quorum=$(cdap_get_conf 'hbase.zookeeper.quorum' '/etc/hbase/conf/hbase-site.xml') || die "Cannot determine zookeeper quorum"

# Derive hive classpath using same method as Hive
__hive_classpath=/etc/hive/conf:
__hive_classpath+=$(ls -1 ${HIVE_HOME}/lib/*.jar | tr '\n' ':')
__hive_classpath+=$(if [[ -d ${HIVE_AUX_JARS_PATH} ]]; then ls -1 ${HIVE_AUX_JARS_PATH}/*.jar | tr '\n' ':'; else echo ${HIVE_AUX_JARS_PATH} | tr ',' ':'; fi)
__hive_classpath+=$(ls -1 ${HIVE_HOME}/auxlib/*.jar 2>/dev/null | tr '\n' ':')

# Read hive exec engine
__hive_exec_engine=$(cdap_get_conf 'hive.execution.engine' '/etc/hive/conf/hive-site.xml' 'mr') || die "Cannot get Hive Exec Engine"

# Get IP
__ipaddr=$(ifconfig eth0 | grep addr: | cut -d: -f2 | head -n 1 | awk '{print $1}')

# Create chef json configuration
sed \
  -e "s/{{ZK_QUORUM}}/${__zk_quorum}/" \
  -e "s/{{CDAP_VERSION}}/${CDAP_VERSION}/" \
  -e "s/{{CDAP_YUM_REPO_URL}}/${CDAP_YUM_REPO_URL}/" \
  -e "s/{{EXPLORE_ENABLED}}/${EXPLORE_ENABLED}/" \
  -e "s/{{HIVE_CLASSPATH}}/${__hive_classpath}/" \
  -e "s/{{HIVE_EXEC_ENGINE}}/${__hive_exec_engine}/" \
  -e "s/{{ROUTER_IP_ADDRESS}}/${__ipaddr}/" \
  ${__cdap_site_template} > ${__tmpdir}/generated-conf.json

# Install/Configure CDAP
chef-solo -o 'recipe[cdap::fullstack]' -j ${__tmpdir}/generated-conf.json

# Temporary Hack to workaround CDAP-4089
rm -f /opt/cdap/kafka/lib/log4j.log4j-1.2.14.jar

# Start CDAP Services
for i in /etc/init.d/cdap-*; do
  __svc=$(basename ${i})
  chkconfig ${__svc}) on || die "Failed to enable ${__svc}"
done

__cleanup_tmpdir
exit 0

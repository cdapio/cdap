#!/bin/bash
#
# Copyright © 2017 Cask Data, Inc.
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
# Install CDAP for Azure HDInsight v2 marketplace
#

# CDAP config
# The git branch to clone
CDAP_BRANCH='release/4.1'
# Optional tag to checkout - All released versions of this script should set this
CDAP_TAG='v4.1.1'
# The CDAP package version passed to Chef
CDAP_VERSION='4.1.1-1'
# The version of Chef to install
CHEF_VERSION='12.10.24'
# cdap-site.xml configuration parameters
EXPLORE_ENABLED='true'

__tmpdir="/tmp/cdap_install.$$.$(date +%s)"
__gitdir="${__tmpdir}/cdap"

__packerdir="${__gitdir}/cdap-distributions/src/packer/scripts"
__cdap_site_template="${__gitdir}/cdap-distributions/src/hdinsight/cdap-conf.json"


# Function definitions

die() { echo "ERROR: ${*}"; exit 1; };

__cleanup_tmpdir() { test -d ${__tmpdir} && rm -rf ${__tmpdir}; };
__create_tmpdir() { mkdir -p ${__tmpdir}; };

# Begin CDAP Prep/Install

# Install git
apt-get install --yes git || die "Failed to install git"

# Install chef
curl -L https://www.chef.io/chef/install.sh | sudo bash -s -- -v ${CHEF_VERSION} || die "Failed to install chef"

# Clone CDAP repo
__create_tmpdir
git clone --depth 1 --branch ${CDAP_BRANCH} https://github.com/caskdata/cdap.git ${__gitdir}

# Check out to specific tag if specified
if [ -n "${CDAP_TAG}" ]; then
  # Ensure tags are fetched
  git -C ${__gitdir} fetch --all --tags --prune
  # Checks out tag to a detached head state
  git -C ${__gitdir} checkout tags/${CDAP_TAG}
fi

# Setup cookbook repo
test -d /var/chef/cookbooks && rm -rf /var/chef/cookbooks
${__packerdir}/cookbook-dir.sh || die "Failed to setup cookbook dir"

# Install cookbooks via knife
${__packerdir}/cookbook-setup.sh || die "Failed to install cookbooks"

# CDAP cli install, ensures package dependencies are present
# We must specify the cdap version
echo "{\"cdap\": {\"version\": \"${CDAP_VERSION}\", \"skip_prerequisites\": \"true\"}}" > ${__tmpdir}/cli-conf.json
chef-solo -o 'recipe[cdap::cli]' -j ${__tmpdir}/cli-conf.json || die 'Failed during Chef CDAP CLI install'

# Read zookeeper quorum from hbase-site.xml, using sourced init script function
source ${__gitdir}/cdap-common/bin/functions.sh || die "Cannot source CDAP common script"
__zk_quorum=$(cdap_get_conf 'hbase.zookeeper.quorum' '/etc/hbase/conf/hbase-site.xml') || die "Cannot determine zookeeper quorum"

# Get HDP version, catch any errors from hdp-select
__hdp_version_str=$(hdp-select status hadoop-client) || die "Cannot run hdp-select to determine HDP version"
__hdp_version=$(echo ${__hdp_version_str} | cut -d' ' -f3) || die "Cannot determine HDP version from string ${__hdp_version_str}"

# Create chef json configuration
sed \
  -e "s/{{ZK_QUORUM}}/${__zk_quorum}/" \
  -e "s/{{HDP_VERSION}}/${__hdp_version}/" \
  -e "s/{{CDAP_VERSION}}/${CDAP_VERSION}/" \
  -e "s/{{EXPLORE_ENABLED}}/${EXPLORE_ENABLED}/" \
  ${__cdap_site_template} > ${__tmpdir}/generated-conf.json

# Install/Configure CDAP
chef-solo -o 'recipe[ulimit::default],recipe[cdap::fullstack],recipe[cdap::init]' -j ${__tmpdir}/generated-conf.json || die 'Failed during Chef run'

# Temporary Hack to workaround CDAP-4089
rm -f /opt/cdap/kafka/lib/log4j.log4j-1.2.14.jar

# Start CDAP Services
for i in /etc/init.d/cdap-*
do
  ${i} start || die "Failed to start ${i}"
done

__cleanup_tmpdir
exit 0

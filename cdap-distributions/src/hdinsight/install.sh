#!/bin/bash
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

#
# Install CDAP for Azure HDInsight cluster
#

# Arguments supplied by https://raw.githubusercontent.com/hdinsight/Edge-Node-Scripts/release-12-1-2015/edgeNodeSetup.sh
CLUSTERNAME=${1}
USERID=${2}
PASSWD=${3}
CUSTOMPARAMETER=${4} # Not used
SSHUSER=${5}

# Ambari constants
AMBARICONFIGS_SH=/var/lib/ambari-server/resources/scripts/configs.sh
AMBARIPORT=8080


# CDAP config
# The git branch to clone
CDAP_BRANCH='release/3.3'
# Optional tag to checkout - All released versions of this script should set this
CDAP_TAG=''
# The CDAP package version passed to Chef
CDAP_VERSION='3.3.1-1'
# The version of Chef to install
CHEF_VERSION='12.7.2'
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


# HDInsight cluster maintains an /etc/hosts entry 'headnodehost' to designate active master.
# Here on the edgenode we only have 'headnode0' and 'headnode1', so we can only guess and check
# Sets ${ACTIVEAMBARIHOST} if successful
setHeadNodeOrDie() {
  __validateAmbariConnectivity 'headnode0' || __validateAmbariConnectivity 'headnode1' || die 'Could not determine active headnode'
}

# Given a candidate Ambari host, try to perform an API GET
# Dies if credentials are bad
__validateAmbariConnectivity() {
    ACTIVEAMBARIHOST=${1}
    coreSiteContent=$(bash ${AMBARICONFIGS_SH} -u ${USERID} -p ${PASSWD} get ${ACTIVEAMBARIHOST} ${CLUSTERNAME} core-site)
    local __ret=$?
    if [[ ${coreSiteContent} == *"[ERROR]"* && ${coreSiteContent} == *"Bad credentials"* ]]; then
        die '[ERROR] Username and password are invalid. Exiting!'
    fi
    return ${__ret}
}

# Given a config type, name, and value, set the configuration via the Ambari API
# Dies if unsuccessful
setAmbariConfig() {
    local __configtype=${1} # core-site, etc
    local __name=${2}
    local __value=${3}
    updateResult=$(bash ${AMBARICONFIGS_SH} -u ${USERID} -p ${PASSWD} set ${ACTIVEAMBARIHOST} ${CLUSTERNAME} ${__configtype} ${__name} ${__value})

    if [[ ${updateResult} != *"Tag:version"* ]] && [[ ${updateResult} == *"[ERROR]"* ]]; then
        die "[ERROR] Failed to update ${__configtype}: ${updateResult}"
    fi
}

# DANGEROUS! unless you know your input is unique
# Runs sed -i to update local Hadoop/HBase client configuration
substituteLocalConfigValue() {
    local __oldVal=${1}
    local __newVal=${2}

    for f in $(ls -1 /etc/hadoop/conf/*.xml /etc/hbase/conf/*.xml) ; do
      sed -i.old -e "s#${__oldVal}#${__newVal}#g" ${f}
    done
}

# Fetches value of fs.azure.page.blob.dir, appends '/cdap' to it, updates it via Ambari API, updates the local client configurations, and restarts cluster services
updateFsAzurePageBlobDirForCDAP() {
    currentValue=$(bash ${AMBARICONFIGS_SH} -u ${USERID} -p ${PASSWD} get ${ACTIVEAMBARIHOST} ${CLUSTERNAME} core-site | grep 'fs.azure.page.blob.dir' | cut -d' ' -f3 | sed -e 's/"\(.*\)"[,]*/\1/')
    if [ -n ${currentValue} ]; then
      if echo ${currentValue} | grep -q '/cdap'; then
        echo "fs.azure.page.blob.dir already contains /cdap"
        return 0
      else
        newValue="${currentValue},/cdap"
      fi
    else
      newValue="/cdap"
    fi
    echo "Updating fs.azure.page.blob.dir to ${newValue}"
    setAmbariConfig 'core-site' 'fs.azure.page.blob.dir' ${newValue} || die "Could not update Ambari config"
    substituteLocalConfigValue ${currentValue} ${newValue} || die "Could not update Local Hadoop Client config"
    restartCdapDependentClusterServices
}

__waitForServiceState() {
    local __svc=${1}
    local __state=${2}

    for i in {1..60} ; do
      __currState=$(curl -s -u ${USERID}:${PASSWD} -i -H 'X-Requested-By: ambari' -X GET http://${ACTIVEAMBARIHOST}:${AMBARIPORT}/api/v1/clusters/${CLUSTERNAME}/services/${__svc} | grep \"state\" | awk '{ print $3}' | sed -e 's/"\(.*\)"[,]*/\1/')
      if [ "${__state}" == "${__currState}" ]; then
        return 0
      else
        sleep 5
      fi
    done
    echo "ERROR: giving up waiting for ${__svc} state to become ${__state}. Current state: ${__currState}"
    return 1
}

# Stop an Ambari cluster service
stopServiceViaRest() {
    if [ -z "${1}" ]; then
        echo "Need service name to stop service"
        exit 136
    fi
    SERVICENAME=${1}
    echo "Stopping ${SERVICENAME}"
    curl -u ${USERID}:${PASSWD} -i -H 'X-Requested-By: ambari' -X PUT -d '{"RequestInfo": {"context" :"Stop Service for CDAP installation"}, "Body": {"ServiceInfo": {"state": "INSTALLED"}}}' http://${ACTIVEAMBARIHOST}:${AMBARIPORT}/api/v1/clusters/${CLUSTERNAME}/services/${SERVICENAME}
}

# Start an Ambari cluster service
startServiceViaRest() {
    if [ -z "${1}" ]; then
        echo "Need service name to start service"
        exit 136
    fi
    sleep 2
    SERVICENAME=${1}
    echo "Starting ${SERVICENAME}"
    startResult=$(curl -u ${USERID}:${PASSWD} -i -H 'X-Requested-By: ambari' -X PUT -d '{"RequestInfo": {"context" :"Start Service for CDAP installation"}, "Body": {"ServiceInfo": {"state": "STARTED"}}}' http://${ACTIVEAMBARIHOST}:${AMBARIPORT}/api/v1/clusters/${CLUSTERNAME}/services/${SERVICENAME})
    if [[ ${startResult} == *"500 Server Error"* || ${startResult} == *"internal system exception occurred"* ]]; then
        sleep 60
        echo "Retry starting ${SERVICENAME}"
        startResult=$(curl -u ${USERID}:${PASSWD} -i -H 'X-Requested-By: ambari' -X PUT -d '{"RequestInfo": {"context" :"Start Service for CDAP installation"}, "Body": {"ServiceInfo": {"state": "STARTED"}}}' http://${ACTIVEAMBARIHOST}:${AMBARIPORT}/api/v1/clusters/${CLUSTERNAME}/services/${SERVICENAME})
    fi
    echo ${startResult}
}

# Restart Ambari cluster services
# Ambari API is asynchronous, so this implements polling to make it somewhat synchronous
restartCdapDependentClusterServices() {
    stopServiceViaRest HBASE
    stopServiceViaRest YARN
    stopServiceViaRest MAPREDUCE2
    stopServiceViaRest HDFS

    __waitForServiceState HBASE INSTALLED || die "Could not stop service HBASE"
    __waitForServiceState YARN INSTALLED || die "Could not stop service YARN"
    __waitForServiceState MAPREDUCE2 INSTALLED || die "Could not stop service MAPREDUCE2"
    __waitForServiceState HDFS INSTALLED || die "Could not stop service HDFS"

    startServiceViaRest HDFS
    startServiceViaRest MAPREDUCE2
    startServiceViaRest YARN
    startServiceViaRest HBASE

    __waitForServiceState HDFS STARTED || die "Could not start service HDFS"
    __waitForServiceState MAPREDUCE2 STARTED || die "Could not start service MAPREDUCE2"
    __waitForServiceState YARN STARTED || die "Could not start service YARN"
    __waitForServiceState HBASE STARTED || die "Could not start service HBASE"
}


# Begin Ambari Cluster Prep

# Find/Validate Ambari connectivity
setHeadNodeOrDie && echo "Found active Ambari host: ${ACTIVEAMBARIHOST}"

# Update necessary hadoop configuration
updateFsAzurePageBlobDirForCDAP


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
  git -C ${__gitdir} checkout tags/${CDAP_TAG}
fi

# Setup cookbook repo
test -d /var/chef/cookbooks && rm -rf /var/chef/cookbooks
${__packerdir}/cookbook-dir.sh || die "Failed to setup cookbook dir"

# Install cookbooks via knife
${__packerdir}/cookbook-setup.sh || die "Failed to install cookbooks"

# CDAP cli install, ensures package dependencies are present
chef-solo -o 'recipe[cdap::cli]'

# Read zookeeper quorum from hbase-site.xml, using sourced init script function
source ${__gitdir}/cdap-common/bin/common.sh || die "Cannot source CDAP common script"
__zk_quorum=$(cdap_get_conf 'hbase.zookeeper.quorum' '/etc/hbase/conf/hbase-site.xml') || die "Cannot determine zookeeper quorum"

# Get HDP version, allow for the future addition hdp-select "current" directory
__hdp_version=$(ls /usr/hdp | grep "^[0-9]*\.") || die "Cannot determine HDP version"

# Create chef json configuration
sed \
  -e "s/{{ZK_QUORUM}}/${__zk_quorum}/" \
  -e "s/{{HDP_VERSION}}/${__hdp_version}/" \
  -e "s/{{CDAP_VERSION}}/${CDAP_VERSION}/" \
  -e "s/{{EXPLORE_ENABLED}}/${EXPLORE_ENABLED}/" \
  ${__cdap_site_template} > ${__tmpdir}/generated-conf.json

# Install/Configure ntp, CDAP
chef-solo -o 'recipe[ntp::default],recipe[ulimit::default],recipe[cdap::fullstack],recipe[cdap::init]' -j ${__tmpdir}/generated-conf.json

# Temporary Hack to workaround CDAP-4089
rm -f /opt/cdap/kafka/lib/log4j.log4j-1.2.14.jar

# Start CDAP Services
for i in /etc/init.d/cdap-*
do
  ${i} start || die "Failed to start ${i}"
done

__cleanup_tmpdir
exit 0

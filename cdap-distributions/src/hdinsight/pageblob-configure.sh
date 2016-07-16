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
# Configure HDFS Pageblob support for CDAP HDInsight cluster
#

# Ambari constants
AMBARICONFIGS_SH=/var/lib/ambari-server/resources/scripts/configs.sh
AMBARIPORT=8080
ACTIVEAMBARIHOST=headnodehost
USERID=$(python -c 'import hdinsight_common.Constants as Constants; print Constants.AMBARI_WATCHDOG_USERNAME')
PASSWD=$(python -c 'import hdinsight_common.ClusterManifestParser as ClusterManifestParser; \
                    import hdinsight_common.Constants as Constants; \
                    import base64; \
                    base64pwd = ClusterManifestParser.parse_local_manifest().ambari_users.usersmap[Constants.AMBARI_WATCHDOG_USERNAME].password; \
                    print base64.b64decode(base64pwd)')

AMBARICREDS="-u ${USERID} -p ${PASSWD}"
AMBARICURLCREDS="-u \"${USERID}\":\"${PASSWD}\""


# Function definitions

die() { __code=${2:-1}; echo "[ERROR] ${1}"; exit ${__code}; };

checkHostNameAndSetClusterName() {
  CLUSTERNAME=${CLUSTERNAME:-$(python -c 'import hdinsight_common.ClusterManifestParser as ClusterManifestParser; \
    print ClusterManifestParser.parse_local_manifest().deployment.cluster_name')}
  [[ ${CLUSTERNAME} ]] || die "Cannot determine cluster name. Exiting!" 133
}

# Try to perform an Ambari API GET
# Dies if credentials are bad
validateAmbariConnectivity() {
  local __coreSiteContent
  local __ret
  __coreSiteContent=$(${AMBARICONFIGS_SH} ${AMBARICREDS} get ${ACTIVEAMBARIHOST} ${CLUSTERNAME} core-site)
  __ret=$?
  if [[ ${__coreSiteContent} =~ "[ERROR]" && ${__coreSiteContent} =~ "Bad credentials" ]]; then
    die 'Username and password are invalid. Exiting!' 134
  elif [ ${__ret} -ne 0 ]; then
    die "Could not connect to Ambari: ${__coreSiteContent}" 134
  fi
}

# Given a config type, name, and value, set the configuration via the Ambari API
# Dies if unsuccessful
setAmbariConfig() {
  local __configtype=${1} # core-site, etc
  local __name=${2}
  local __value=${3}
  local __updateResult
  __updateResult=$(${AMBARICONFIGS_SH} ${AMBARICREDS} set ${ACTIVEAMBARIHOST} ${CLUSTERNAME} ${__configtype} ${__name} ${__value})

  if [[ ${__updateResult} =~ "[ERROR]" ]] && [[ ! ${__updateResult} =~ "Tag:version" ]]; then
    die "Failed to update ${__configtype}: ${__updateResult}" 135
  fi
}

# Fetches value of fs.azure.page.blob.dir, appends '/cdap' to it, updates it via Ambari API, updates the local client configurations, and restarts cluster services
updateFsAzurePageBlobDirForCDAP() {
  local __currentValue
  local __newValue
  __currentValue=$(${AMBARICONFIGS_SH} ${AMBARICREDS} get ${ACTIVEAMBARIHOST} ${CLUSTERNAME} core-site | grep 'fs.azure.page.blob.dir' | cut -d' ' -f3 | sed -e 's/"\(.*\)"[,]*/\1/')
  if [ -n ${__currentValue} ]; then
    if [[ ${__currentValue} =~ "/cdap" ]]; then
      echo "fs.azure.page.blob.dir already contains /cdap"
      return 0
    else
      __newValue="${__currentValue},/cdap"
    fi
  else
    __newValue="/cdap"
  fi
  echo "Updating fs.azure.page.blob.dir to ${__newValue}"
  setAmbariConfig 'core-site' 'fs.azure.page.blob.dir' ${__newValue} || die "Could not update Ambari config" 1
  restartCdapDependentClusterServices
}

# Stop an Ambari cluster service
stopServiceViaRest() {
  local SERVICENAME=${1}
  [[ ${SERVICENAME} ]] || die "Need service name to stop service" 136
  echo "Stopping ${SERVICENAME}"
  cmd="curl -s ${AMBARICURLCREDS} -i -H 'X-Requested-By: ambari' -X PUT -d '{\"RequestInfo\": {\"context\" :\"Configure Azure page blob support for CDAP installation. Stopping ${SERVICENAME}\"}, \"Body\": {\"ServiceInfo\": {\"state\": \"INSTALLED\"}}}' http://${ACTIVEAMBARIHOST}:${AMBARIPORT}/api/v1/clusters/${CLUSTERNAME}/services/${SERVICENAME}"
  eval $cmd
}

# Start an Ambari cluster service
startServiceViaRest() {
  local SERVICENAME=${1}
  local __startResult
  [[ ${SERVICENAME} ]] || die "Need service name to start service" 136
  sleep 2
  echo "Starting ${SERVICENAME}"
  cmd="curl -s ${AMBARICURLCREDS} -i -H 'X-Requested-By: ambari' -X PUT -d '{\"RequestInfo\": {\"context\" :\"Configure Azure page blob support for CDAP installation. Starting ${SERVICENAME}\"}, \"Body\": {\"ServiceInfo\": {\"state\": \"STARTED\"}}}' http://${ACTIVEAMBARIHOST}:${AMBARIPORT}/api/v1/clusters/${CLUSTERNAME}/services/${SERVICENAME}"
  __startResult=$(eval $cmd)
  if [[ ${__startResult} =~ "500 Server Error" || ${__startResult} =~ "internal system exception occurred" ]]; then
    sleep 60
    echo "Retry starting ${SERVICENAME}"
    __startResult=$(eval $cmd)
  fi
  echo ${__startResult}
}

# Given a service and component, wait for "started_count" to equal 0
waitForComponentStop() {
  local __svc=${1}
  local __component=${2}
  local __hc_url

  # Get a list of host_component hrefs
  cmd="curl -s ${AMBARICURLCREDS} -H 'X-Requested-By: ambari' -X GET http://${ACTIVEAMBARIHOST}:${AMBARIPORT}/api/v1/clusters/${CLUSTERNAME}/services/${__svc}/components/${__component} | grep href | grep host_components | awk '{ print \$3 }' | sed -e 's/\"\(.*\)\"[,]*/\1/'"
  for __hc_url in $(eval $cmd) ; do
    echo "querying host_component at: ${__hc_url}"
    waitForHostComponentStop ${__hc_url} || die "Giving up waiting for ${__svc}, component ${__component} to stop"
    echo "${__component} stopped"
  done
}

# Given a "host_component" href, wait for "state" to be "INSTALLED" (stopped)
waitForHostComponentStop() {
  local __hc_url=${1}
  local __currState

  cmd="curl -s ${AMBARICURLCREDS} -H 'X-Requested-By: ambari' -X GET ${__hc_url} | grep \\\"state\\\" | awk '{ print \$3 }' | sed -e 's/\"\(.*\)\"[,]*/\1/'"
  for i in {1..60}; do
    __currState=$(eval $cmd)
    if [ "${__currState}" == "INSTALLED" ]; then
      return 0
    else
      sleep 5
    fi
  done
  return 1
}

# Restart Ambari cluster services
restartCdapDependentClusterServices() {
  stopServiceViaRest HBASE
  stopServiceViaRest YARN
  stopServiceViaRest MAPREDUCE2
  stopServiceViaRest HDFS

  # Ensure all critical components are completely stopped before issuing any starts.  Otherwise, components may be left not running.
  echo "Confirming HBASE STOP"
  for __component in HBASE_MASTER HBASE_REGIONSERVER PHOENIX_QUERY_SERVER; do
    waitForComponentStop HBASE ${__component} || die "Could not stop component ${__component} of service HBASE" 1
  done
  echo "Confirming YARN STOP"
  for __component in RESOURCEMANAGER NODEMANAGER; do
    waitForComponentStop YARN ${__component} || die "Could not stop component ${__component} of service YARN" 1
  done
  echo "Confirming HDFS STOP"
  for __component in DATANODE NAMENODE JOURNALNODE ZKFC; do
    waitForComponentStop HDFS ${__component} || die "Could not stop component ${__component} of service HDFS" 1
  done

  startServiceViaRest HDFS
  startServiceViaRest MAPREDUCE2
  startServiceViaRest YARN
  startServiceViaRest HBASE
}


# Begin Ambari Cluster Prep

checkHostNameAndSetClusterName

validateAmbariConnectivity

# Update necessary hadoop configuration
updateFsAzurePageBlobDirForCDAP

exit 0

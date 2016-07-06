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

# Function definitions

die() { echo "ERROR: ${*}"; exit 1; };

checkHostNameAndSetClusterName() {
    fullHostName=$(hostname -f)
    echo "fullHostName=$fullHostName"
    CLUSTERNAME=$(sed -n -e 's/.*\.\(.*\)-ssh.*/\1/p' <<< $fullHostName)
    if [ -z "$CLUSTERNAME" ]; then
        CLUSTERNAME=$(echo -e "import hdinsight_common.ClusterManifestParser as ClusterManifestParser\nprint ClusterManifestParser.parse_local_manifest().deployment.cluster_name" | python)
        if [ $? -ne 0 ]; then
            echo "[ERROR] Cannot determine cluster name. Exiting!"
            exit 133
        fi
    fi
    echo "Cluster Name=$CLUSTERNAME"
}

# Try to perform an Ambari API GET
# Dies if credentials are bad
__validateAmbariConnectivity() {
    coreSiteContent=$(bash ${AMBARICONFIGS_SH} -u ${USERID} -p ${PASSWD} get ${ACTIVEAMBARIHOST} ${CLUSTERNAME} core-site)
    if [[ ${coreSiteContent} == *"[ERROR]"* && ${coreSiteContent} == *"Bad credentials"* ]]; then
        echo '[ERROR] Username and password are invalid. Exiting!'
        exit 134
    fi
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
    restartCdapDependentClusterServices
}

# Stop an Ambari cluster service
stopServiceViaRest() {
    if [ -z "${1}" ]; then
        echo "Need service name to stop service"
        exit 136
    fi
    SERVICENAME=${1}
    echo "Stopping ${SERVICENAME}"
    curl -u ${USERID}:${PASSWD} -i -H 'X-Requested-By: ambari' -X PUT -d '{"RequestInfo": {"context" :"Configure Azure page blob support for CDAP installation"}, "Body": {"ServiceInfo": {"state": "INSTALLED"}}}' http://${ACTIVEAMBARIHOST}:${AMBARIPORT}/api/v1/clusters/${CLUSTERNAME}/services/${SERVICENAME}
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
    startResult=$(curl -u ${USERID}:${PASSWD} -i -H 'X-Requested-By: ambari' -X PUT -d '{"RequestInfo": {"context" :"Configure Azure page blob support for CDAP installation"}, "Body": {"ServiceInfo": {"state": "STARTED"}}}' http://${ACTIVEAMBARIHOST}:${AMBARIPORT}/api/v1/clusters/${CLUSTERNAME}/services/${SERVICENAME})
    if [[ ${startResult} == *"500 Server Error"* || ${startResult} == *"internal system exception occurred"* ]]; then
        sleep 60
        echo "Retry starting ${SERVICENAME}"
        startResult=$(curl -u ${USERID}:${PASSWD} -i -H 'X-Requested-By: ambari' -X PUT -d '{"RequestInfo": {"context" :"Configure Azure page blob support for CDAP installation"}, "Body": {"ServiceInfo": {"state": "STARTED"}}}' http://${ACTIVEAMBARIHOST}:${AMBARIPORT}/api/v1/clusters/${CLUSTERNAME}/services/${SERVICENAME})
    fi
    echo ${startResult}
}

# Restart Ambari cluster services
restartCdapDependentClusterServices() {
    stopServiceViaRest HBASE
    stopServiceViaRest YARN
    stopServiceViaRest MAPREDUCE2
    stopServiceViaRest HDFS

    startServiceViaRest HDFS
    startServiceViaRest MAPREDUCE2
    startServiceViaRest YARN
    startServiceViaRest HBASE
}


# Begin Ambari Cluster Prep

USERID=$(echo -e "import hdinsight_common.Constants as Constants\nprint Constants.AMBARI_WATCHDOG_USERNAME" | python)
echo "USERID=$USERID"

PASSWD=$(echo -e "import hdinsight_common.ClusterManifestParser as ClusterManifestParser\nimport hdinsight_common.Constants as Constants\nimport base64\nbase64pwd = ClusterManifestParser.parse_local_manifest().ambari_users.usersmap[Constants.AMBARI_WATCHDOG_USERNAME].password\nprint base64.b64decode(base64pwd)" | python)

checkHostNameAndSetClusterName

__validateAmbariConnectivity

# Update necessary hadoop configuration
updateFsAzurePageBlobDirForCDAP

exit 0

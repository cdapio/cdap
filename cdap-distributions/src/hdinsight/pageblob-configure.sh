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
    cmd="curl -u ${USERID}:${PASSWD} -i -H 'X-Requested-By: ambari' -X PUT -d '{\"RequestInfo\": {\"context\" :\"Configure Azure page blob support for CDAP installation. Stopping ${SERVICENAME}\"}, \"Body\": {\"ServiceInfo\": {\"state\": \"INSTALLED\"}}}' http://${ACTIVEAMBARIHOST}:${AMBARIPORT}/api/v1/clusters/${CLUSTERNAME}/services/${SERVICENAME}"
    eval $cmd
}

# Start an Ambari cluster service
startServiceViaRest() {
    local SERVICENAME=${1}
    local __startResult
    [[ ${SERVICENAME} ]] || die "Need service name to start service" 136
    sleep 2
    echo "Starting ${SERVICENAME}"
    cmd="curl -u ${USERID}:${PASSWD} -i -H 'X-Requested-By: ambari' -X PUT -d '{\"RequestInfo\": {\"context\" :\"Configure Azure page blob support for CDAP installation. Starting ${SERVICENAME}\"}, \"Body\": {\"ServiceInfo\": {\"state\": \"STARTED\"}}}' http://${ACTIVEAMBARIHOST}:${AMBARIPORT}/api/v1/clusters/${CLUSTERNAME}/services/${SERVICENAME}"
    __startResult=$(eval $cmd)
    if [[ ${__startResult} =~ "500 Server Error" || ${__startResult} =~ "internal system exception occurred" ]]; then
        sleep 60
        echo "Retry starting ${SERVICENAME}"
        __startResult=$(eval $cmd)
    fi
    echo ${__startResult}
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

checkHostNameAndSetClusterName

validateAmbariConnectivity

# Update necessary hadoop configuration
updateFsAzurePageBlobDirForCDAP

exit 0

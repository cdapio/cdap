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
# Install CDAP for EMR
#

die() { echo "ERROR: ${*}"; exit 1; };

# This should return the latest released CDAP version... do not put -SNAPSHOT here
export CDAP_VERSION=${CDAP_VERSION:-4.0.0-1}

__repo_url=${CDAP_REPO_URL:-http://repository.cask.co/centos/6/x86_64/cdap/MAJ_MIN}

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
          __maj_min=$(echo ${CDAP_VERSION} | cut -d. -f1,2)
          echo "WARNING: --cdap-repo-url should specify a URL afterwards, using default ${__repo_url/MAJ_MIN/${__maj_min}}"
          continue
        else
          CDAP_REPO_URL=${__arg}
          echo "INFO: Setting CDAP repository URL to ${CDAP_REPO_URL}"
          shift
        fi
      else
        CDAP_REPO_URL=${__tmp}
        echo "INFO: Setting CDAP repository URL to ${CDAP_REPO_URL}"
      fi
      ;;
    *) break ;;
  esac
done

# Get major/minor from CDAP version
__maj_min=$(echo ${CDAP_VERSION} | cut -d. -f1,2)

# One last sed-fu, if we're using the default CDAP_REPO_URL, in case version's been updated
CDAP_REPO_URL=${CDAP_REPO_URL:-${__repo_url/MAJ_MIN/${__maj_min}}}

# echo "CDAP_VERSION:  $CDAP_VERSION"
# echo "CDAP_REPO_URL: $CDAP_REPO_URL"

# Create YUM repo file
__repo_file=/etc/yum.repos.d/cdap-${__maj_min}.repo
echo "[CDAP-${__maj_min}]" > ${__repo_file}
echo "name=CDAP ${__maj_min}" >> ${__repo_file}
echo "baseurl=${CDAP_REPO_URL}" >> ${__repo_file}
echo "enabled=1" >> ${__repo_file}

# Do GPG check on official repos
if [[ ${CDAP_REPO_URL} == ${__repo_url/MAJ_MIN/${__maj_min}} ]]; then
  echo "gpgcheck=1" >> ${__repo_file}
  rpm --import ${CDAP_REPO_URL}/pubkey.gpg || die "Cannot import GPG key from repo"
fi

yum makecache
yum install cdap-{cli,gateway,kafka,master,security,ui} -y || die "Failed installing packages"

mkdir -p /mnt/cdap/kafka-logs
chown -R cdap:cdap /mnt/cdap/kafka-logs

# Configure CDAP
__ipaddr=`ifconfig eth0 | grep addr: | cut -d: -f2 | head -n 1 | awk '{print $1}'`
sed \
  -e "s/FQDN1:2181,FQDN2/${__ipaddr}/" \
  -e 's:/data/cdap/kafka-logs:/mnt/cdap/kafka-logs:' \
  -e "s/FQDN1:9092,FQDN2:9092/${__ipaddr}:9092/" \
  -e "s/LOCAL-ROUTER-IP/${__ipaddr}/" \
  -e 's/LOCAL-APP-FABRIC-IP//' \
  -e 's/LOCAL-DATA-FABRIC-IP//' \
  -e 's/LOCAL-WATCHDOG-IP//' \
  -e "s/ROUTER-HOST-IP/${__ipaddr}/" \
  /etc/cdap/conf/cdap-site.xml.example > /etc/cdap/conf/cdap-site.xml

# Temporary Hack to workaround CDAP-4089
rm -f /opt/cdap/kafka/lib/log4j.log4j-1.2.14.jar

# Setup HDFS directories
su - hdfs -c 'for d in /cdap /user/cdap; do hdfs dfs -mkdir -p ${d} ; hdfs dfs -chown cdap ${d}; done'

# Start CDAP Services
for i in /etc/init.d/cdap-*
do
  ${i} start || die "Failed to start ${i}"
done

exit 0

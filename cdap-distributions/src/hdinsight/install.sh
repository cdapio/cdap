#!/bin/bash
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

#
# Install CDAP for Azure HDInsight cluster
#

die() { echo "ERROR: ${*}"; exit 1; };

# Fetch repo file for Apt
curl http://repository.cask.co/ubuntu/precise/amd64/cdap/3.2/cask.list > /etc/apt/sources.list.d/cask.list || die "Cannot fetch repo"
curl -s http://repository.cask.co/ubuntu/precise/amd64/cdap/3.2/pubkey.gpg | sudo apt-key add - || die "Cannot import GPG key from repo"
# install node.js
curl --silent --location https://deb.nodesource.com/setup | sudo bash - || die "Failed to configure nodejs repo"
sudo apt-get install --yes nodejs || die "Failed to install nodejs"

# Redundant - nodejs install does an apt-get update
# apt-get update

# Install CDAP packages
apt-get install cdap-{gateway,kafka,master,ui} --yes || die "Failed installing CDAP packages"

# Setup kafka storage dir
mkdir -p /var/cdap/kafka-logs
chown -R cdap:cdap /var/cdap/kafka-logs

# We need to get the zookeeper quorum
# Read it from hbase-site.xml

. /opt/cdap/master/bin/common.sh || die "Cannot source cdap-master common script"
__zk_quorum=$(get_conf 'hbase.zookeeper.quorum' '/usr/hdp/current/hbase-client/conf/hbase-site.xml')

# Configure CDAP
__ipaddr=`ifconfig eth0 | grep addr: | cut -d: -f2 | head -n 1 | awk '{print $1}'`
sed \
  -e "s/FQDN1:2181,FQDN2/${__zk_quorum}/" \
  -e 's:/data/cdap/kafka-logs:/var/cdap/kafka-logs:' \
  -e "s/FQDN1:9092,FQDN2:9092/${__ipaddr}:9092/" \
  -e "s/LOCAL-ROUTER-IP/${__ipaddr}/" \
  -e 's/LOCAL-APP-FABRIC-IP//' \
  -e 's/LOCAL-DATA-FABRIC-IP//' \
  -e 's/LOCAL-WATCHDOG-IP//' \
  -e "s/ROUTER-HOST-IP/${__ipaddr}/" \
  -e 's/router.server.port/router.bind.port/' \
  /etc/cdap/conf/cdap-site.xml.example > /etc/cdap/conf/cdap-site.xml

__hdp_version=$(basename $(dirname $(readlink /usr/hdp/current/hadoop-client)))

sed -i \
  -e "s/# export OPTS=\"\${OPTS} -Dhdp.version=2.2.6.0-2800\"/export \"OPTS=\${OPTS} -Dhdp.version=${__hdp_version}\"/" \
  /etc/cdap/conf/cdap-env.sh

# Start services
for i in /etc/init.d/cdap-*
do
  $i start || die "Failed to start $i"
done
exit 0

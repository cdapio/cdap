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
# Install CDAP under hadoop user for EMR
#

die() { echo "ERROR: ${*}"; exit 1; };

# Fetch repo file for YUM
curl http://repository.cask.co/centos/6/x86_64/cdap/3.3/cask.repo > /etc/yum.repos.d/cdap-3.3.repo || die "Cannot fetch repo"
rpm --import http://repository.cask.co/centos/6/x86_64/cdap/3.0/pubkey.gpg || die "Cannot import GPG key from repo"
# Enable EPEL packages (req'd for nodejs)
sed -i -e '0,/enabled=0/s//enabled=1/' /etc/yum.repos.d/epel.repo || die "Unable to enable EPEL"
yum makecache
# Install dependencies
yum install --downloadonly chkconfig -y || die "Cannot download chkconfig package"
rpm --install --force /var/cache/yum/x86_64/latest/amzn-main/packages/chkconfig-* || die "Unable to install chkconfig"
yum install gyp http-parser-devel redhat-rpm-config icu nodejs nodejs-devel npm cdap-{gateway,kafka,master,ui} -y || die "Failed installing packages"
# Update permissions and symlink logs
mkdir -p /mnt/var/log/cdap /mnt/cdap/kafka-logs
chown -R hadoop:hadoop /var/cdap/run /var/run/cdap /var/tmp/cdap /mnt/var/log/cdap /mnt/cdap/kafka-logs
rm -rf /var/log/cdap
ln -sf /mnt/var/log/cdap /var/log/cdap
# Configure CDAP
__ipaddr=`ifconfig eth0 | grep addr: | cut -d: -f2 | head -n 1 | awk '{print $1}'`
sed \
  -e 's/yarn/hadoop/' \
  -e "s/FQDN1:2181,FQDN2/${__ipaddr}/" \
  -e 's:/data/cdap/kafka-logs:/mnt/cdap/kafka-logs:' \
  -e "s/FQDN1:9092,FQDN2:9092/${__ipaddr}:9092/" \
  -e "s/LOCAL-ROUTER-IP/${__ipaddr}/" \
  -e 's/LOCAL-APP-FABRIC-IP//' \
  -e 's/LOCAL-DATA-FABRIC-IP//' \
  -e 's/LOCAL-WATCHDOG-IP//' \
  -e "s/ROUTER-HOST-IP/${__ipaddr}/" \
  -e 's/router.server.port/router.bind.port/' \
  -e 's/10000/11015/' \
  /etc/cdap/conf/cdap-site.xml.example > /etc/cdap/conf/cdap-site.xml
# Start services
for i in \
  /opt/cdap/gateway/bin/svc-router \
  /opt/cdap/kafka/bin/svc-kafka-server \
  /opt/cdap/master/bin/svc-master \
  /opt/cdap/ui/bin/svc-ui
do
  su - hadoop -c "$i start" || die "Failed to start $i"
done
exit 0

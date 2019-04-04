#!/bin/bash
#
# Copyright © 2018 Cask Data, Inc.
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
# Update Sandbox configuration to use CDAP Basic Authentication
#

# Strip closing </configuration> tag
sed -e '/<\/configuration>/d' /opt/cdap/sandbox/conf/cdap-site.xml > /opt/cdap/sandbox/conf/cdap-site.xml.new

# Append our security configuration
echo "  <property>
    <name>security.enabled</name>
    <value>true</value>
  </property>

  <property>
    <name>security.authentication.basic.realmfile</name>
    <value>/opt/cdap/sandbox/conf/realmfile</value>
  </property>

  <property>
    <name>security.authentication.handlerClassName</name>
    <value>io.cdap.cdap.security.server.BasicAuthenticationHandler</value>
  </property>

</configuration>" >> /opt/cdap/sandbox/conf/cdap-site.xml.new

unalias mv # in case root has a "mv -i" alias
mv -f /opt/cdap/sandbox/conf/cdap-site.xml{.new,}

# Create init script to populate realmfile
echo '#!/usr/bin/env bash

#
# chkconfig: 2345 95 15
# description: Creates /opt/cdap/sandbox/conf/realmfile using GCP metadata field cdap_user_password
#
### BEGIN INIT INFO
# Provides:          cdap-realmfile
# Short-Description: CDAP realmfile creator
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Required-Start:    $syslog $remote_fs
# Required-Stop:     $syslog $remote_fs
# Should-Start:
# Should-Stop:
### END INIT INFO

if [[ ${1} == start ]]; then
  if [[ -e /opt/cdap/sandbox/conf/realmfile ]]; then
    echo "CDAP Sandbox Realmfile already exists... skipping generation"
  else
    __user_password=$(curl -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/attributes/cdap_user_password 2>/dev/null)
    echo "Creating CDAP Sandbox Realmfile with cdap_user_password metadata as password"
    echo "cdap: ${__user_password}" > /opt/cdap/sandbox/conf/realmfile
    chown cdap:cdap /opt/cdap/sandbox/conf/realmfile
    chmod 0400 /opt/cdap/sandbox/conf/realmfile
  fi
fi
exit 0' > /etc/init.d/cdap-realmfile
chmod 755 /etc/init.d/cdap-realmfile

# Add to default run-levels
if [[ $(which update-rc.d 2>/dev/null) ]]; then
  update-rc.d cdap-realmfile defaults
else
  chkconfig --add cdap-realmfile
fi

# Make cdap own /opt/cdap
chown -R cdap:cdap /opt/cdap

exit 0

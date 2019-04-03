#!/bin/bash
#
# Copyright © 2015-2017 Cask Data, Inc.
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

# Disable automatic udev rules for network interfaces in Ubuntu,
# source: http://6.ptmc.org/164/
rm -f /etc/udev/rules.d/70-persistent-net.rules
rm -f /lib/udev/rules.d/75-persistent-net-generator.rules
rm -rf /dev/.udev/ /var/lib/dhcp/*

# Remove HWADDR/UUID from ifcfg-* files (RHEL-compatable)
for ndev in $(ls -1 /etc/sysconfig/network-scripts/ifcfg-* 2>/dev/null); do
  [[ "${ndev##*/}" != "ifcfg-lo" ]] && sed -i '/^HWADDR/d;/^UUID/d' ${ndev}
done

# Adding a 2 sec delay to the interface up, to make the dhclient happy
if [[ -d /etc/network/interfaces.d ]]; then
  if [[ -e /etc/network/interfaces.d/eth0.cfg ]]; then
    echo "pre-up sleep 2" >> /etc/network/interfaces.d/eth0.cfg
  fi
else
  echo "pre-up sleep 2" >> /etc/network/interfaces
fi

exit 0

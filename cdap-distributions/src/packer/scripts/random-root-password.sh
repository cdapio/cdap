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
# Set a random root password
#

# Install apg
apt-get install -y apg

# Setup cdap for sudo
usermod -G sudo,adm,cdrom,dip,plugdev,lpadmin,sambashare cdap

# Use apg to generate a 14-character password and assign to root
echo "root:`apg -a 1 -m 14 -n 1`" | chpasswd

# Remove apg
apt-get purge -y apg

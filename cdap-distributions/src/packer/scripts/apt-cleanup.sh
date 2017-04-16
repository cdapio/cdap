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

#
# Removes stuff from APT and clears caches
#

# Remove old kernels/headers
apt-get remove --purge -y $(dpkg -l 'linux-image-*' | grep -v linux-image-extra | grep -v linux-image-generic | sed '/^ii/!d;s/^[^ ]* [^ ]* \([^ ]*\).*/\1/;/[0-9]/!d' | sort | tail -n +2)
apt-get remove --purge -y $(dpkg -l 'linux-headers-*' | sed '/^ii/!d;s/^[^ ]* [^ ]* \([^ ]*\).*/\1/;/[0-9]/!d' | sort | tail -n +2)

# Remove build packages
apt-get purge -y build-essential autoconf autogen automake autotools-dev

# Remove fuse
apt-get purge -y fuse

# Autoremove and clean
apt-get autoremove -y
apt-get autoclean

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
# Install and configure fluxbox
#

# Install
apt-get install -y fluxbox

# Create .fluxbox
mkdir -p ~cdap/.fluxbox
cd ~cdap/.fluxbox

# Symlink idea
ln -sf /opt/idea* /opt/idea

# Populate startup file
echo 'xterm -e "cat /etc/motd; bash -l" &' > startup
# echo '/opt/idea/bin/idea.sh &' >> startup
# echo 'eclipse &' >> startup
# echo 'firefox http://localhost:9999 http://docs.cask.co/cdap &' >> startup
echo 'exec fluxbox' >> startup

# Create our fluxbox menu
echo '[begin] (fluxbox)' > menu
echo '[exec] (CDAP UI) {firefox http://localhost:9999}' >> menu
echo '[exec] (CDAP Docs) {firefox http://docs.cask.co/cdap}' >> menu
echo '[exec] (Firefox) {firefox}' >> menu
echo '[exec] (Eclipse IDE) {eclipse}' >> menu
echo '[exec] (IntelliJ IDE) {/opt/idea/bin/idea.sh}' >> menu
echo '[exec] (Terminal) {xterm}' >> menu
echo '[include] (/etc/X11/fluxbox/fluxbox-menu)' >> menu
echo '[end]' >> menu

# Fix permissions
chown -R cdap:cdap ~cdap

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
# Sets up Chef cookbook directory for "knife cookbook site install"
#

# Create directory
mkdir -p /var/chef/cookbooks
cd /var/chef/cookbooks

# Initialize Git repository
touch .gitignore

if [[ $(which apt-get 2>/dev/null) ]]; then
  apt-get update
  apt-get install -y --no-install-recommends git || exit 1
else
  yum install -y git || exit 1
fi
git config --global user.email "ops@cask.co"
git config --global user.name "Cask Ops"
git init || exit 1
git add .gitignore || exit 1
git commit -m 'Initial commit' || exit 1

exit 0

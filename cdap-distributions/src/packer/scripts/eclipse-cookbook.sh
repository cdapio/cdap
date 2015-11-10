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
# Download Eclipse cookbook for Chef from https://github.com/geocent-cookbooks/eclipse
#

cd /var/chef/cookbooks || (echo "Cannot change to cookbook directory" && exit 1)

# Check out the cookbook from GitHub
which git || (echo "Cannot locate git" && exit 1)
git clone https://github.com/geocent-cookbooks/eclipse.git || (echo "Cannot checkout eclipse cookbook" && exit 1)

# Remove .git, so it's no longer tracked by git
rm -rf eclipse/.git

exit 0

#
# Cookbook Name:: cdap
# Recipe:: security_realm_file
#
# Copyright © 2013-2015 Cask Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Manage Authentication realmfile
realmfile = node['cdap']['cdap_site']['security.authentication.basic.realmfile']
realmdir = ::File.dirname(realmfile)

# Ensure parent directory exists
directory realmdir do
  action :create
  recursive true
end

# Create the realmfile
template realmfile do
  source 'generic-kv-colon.erb'
  mode 0o644
  owner 'cdap'
  group 'cdap'
  variables options: node['cdap']['security']['realmfile']
  action :create
end

#
# Cookbook Name:: cdap
# Recipe:: security_realm_file
#
# Copyright © 2013-2016 Cask Data, Inc.
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
if node['cdap']['cdap_site'].key?('security.authentication.basic.realmfile') && !node['cdap']['security']['realmfile'].empty?
  realmfile = node['cdap']['cdap_site']['security.authentication.basic.realmfile']
  realmdir = ::File.dirname(realmfile)

  # Ensure parent directory exists
  directory realmdir do
    mode '0700'
    owner 'cdap'
    group 'cdap'
    action :create
    recursive true
    not_if { ::Dir.exist? realmdir }
  end

  # Create the realmfile
  template realmfile do
    source 'generic-kv-colon.erb'
    mode '0600'
    owner 'cdap'
    group 'cdap'
    variables options: node['cdap']['security']['realmfile']
    action :create
  end
end

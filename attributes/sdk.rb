#
# Cookbook Name:: cdap
# Attribute:: sdk
#
# Copyright © 2015 Cask Data, Inc.
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

# URL to repository
ver = node['cdap']['version'].gsub(/-.*/, '')
default['cdap']['sdk']['url'] = "http://repository.cask.co/downloads/co/cask/cdap/cdap-sdk/#{ver}/cdap-sdk-#{ver}.zip"
default['cdap']['sdk']['checksum'] =
  case ver
  when '2.8.0'
    '1f5824a67fcbb5b2fcec02524d59b7befd1c315ed4046d02221fe8f54bbf233a'
  when '3.0.0'
    '1f5824a67fcbb5b2fcec02524d59b7befd1c315ed4046d02221fe8f54bbf233a'
  end
default['cdap']['sdk']['install_path'] = '/opt/cdap'
default['cdap']['sdk']['user'] = 'cdap'
default['cdap']['sdk']['manage_user'] = true
default['cdap']['sdk']['init_name'] = 'SDK'
default['cdap']['sdk']['init_krb5'] = false
default['cdap']['sdk']['init_cmd'] = "#{node['cdap']['sdk']['install_path']}/sdk/bin/cdap.sh"
default['cdap']['sdk']['init_actions'] = [:enable, :start]

#
# Cookbook Name:: cdap
# Recipe:: sdk
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

# Dependencies
%w(ark java nodejs).each do |recipe|
  include_recipe recipe
end
link '/usr/bin/node' do
  to '/usr/local/bin/node'
  action :create
  not_if 'test -e /usr/bin/node'
end

ver = node['cdap']['version'].gsub(/-.*/, '')
ark_prefix_path = ::File.dirname(node['cdap']['sdk']['install_path']) if ::File.basename(node['cdap']['sdk']['install_path']) == "sdk-#{ver}"
ark_prefix_path ||= node['cdap']['sdk']['install_path']

directory ark_prefix_path do
  action :create
  recursive true
end

user node['cdap']['sdk']['user'] do
  comment 'CDAP SDK Service Account'
  home node['cdap']['sdk']['install_path']
  shell '/bin/bash'
  supports :manage_home => true
  system true
  action :create
end

ark 'sdk' do
  url node['cdap']['sdk']['url']
  prefix_root ark_prefix_path
  prefix_home ark_prefix_path
  checksum node['cdap']['sdk']['checksum']
  version ver
  owner node['cdap']['sdk']['user']
  group node['cdap']['sdk']['user']
end

link '/etc/init.d/cdap-sdk' do
  to "#{ark_prefix_path}/sdk/bin/cdap.sh"
end

service 'cdap-sdk' do
  # init_command "#{ark_prefix_path}/sdk/bin/cdap.sh"
  # start_command "#{ark_prefix_path}/sdk/bin/cdap.sh start"
  # stop_command "#{ark_prefix_path}/sdk/bin/cdap.sh stop"
  # status_command "#{ark_prefix_path}/sdk/bin/cdap.sh status"
  action :start
end

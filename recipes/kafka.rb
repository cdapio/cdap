#
# Cookbook Name:: cdap
# Recipe:: kafka
#
# Copyright © 2013-2014 Cask Data, Inc.
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

include_recipe 'cdap::default'

package 'cdap-kafka' do
  action :install
  version node['cdap']['version']
end

kafka_log_dir =
  if node['cdap']['cdap_site'].key?('kafka.log.dir')
    node['cdap']['cdap_site']['kafka.log.dir']
  else
    '/tmp/kafka-logs'
  end

node.default['cdap']['cdap_site']['kafka.log.dir'] = kafka_log_dir

directory kafka_log_dir do
  mode 0755
  owner 'cdap'
  group 'cdap'
  action :create
  recursive true
end

service 'cdap-kafka-server' do
  status_command 'service cdap-kafka-server status'
  action :nothing
end

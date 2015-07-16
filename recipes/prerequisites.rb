#
# Cookbook Name:: cdap
# Recipe:: prerequisites
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

# We require Java, so include it and add it to Hadoop env
include_recipe 'java'
log 'java-home' do
  message "JAVA_HOME = #{node['java']['java_home']}"
end
ENV['JAVA_HOME'] = node['java']['java_home']
%w(hadoop hbase hive).each do |envfile|
  node.default[envfile]["#{envfile}_env"]['java_home'] = node['java']['java_home']
end

# We need Hadoop/HBase installed
%w(default hbase).each do |recipe|
  include_recipe "hadoop::#{recipe}"
end

# Hive is optional
if node['cdap'].key?('cdap_site') && node['cdap']['cdap_site'].key?('explore.enabled') &&
   node['cdap']['cdap_site']['explore.enabled'].to_s == 'true'

  log 'hive-explore-enabled' do
    message 'Explore module enabled, installing Hive libraries'
  end
  include_recipe 'hadoop::hive'
end

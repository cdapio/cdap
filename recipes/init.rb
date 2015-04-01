#
# Cookbook Name:: cdap
# Recipe:: init
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

# We also need the configuration, so we can run HDFS commands
execute 'initaction-create-hdfs-cdap-dir' do
  not_if  "hdfs dfs -test -d #{node['cdap']['cdap_site']['hdfs.namespace']}", :user => node['cdap']['cdap_site']['hdfs.user']
  command "hdfs dfs -mkdir -p #{node['cdap']['cdap_site']['hdfs.namespace']} && hdfs dfs -chown #{node['cdap']['cdap_site']['hdfs.user']} #{node['cdap']['cdap_site']['hdfs.namespace']}"
  timeout 300
  user 'hdfs'
  group 'hdfs'
end

execute 'initaction-create-hdfs-cdap-user-dir' do
  not_if  "hdfs dfs -test -d /user/#{node['cdap']['cdap_site']['hdfs.user']}", :user => node['cdap']['cdap_site']['hdfs.user']
  command "hdfs dfs -mkdir -p /user/#{node['cdap']['cdap_site']['hdfs.user']} && hdfs dfs -chown #{node['cdap']['cdap_site']['hdfs.user']} /user/#{node['cdap']['cdap_site']['hdfs.user']}"
  timeout 300
  user 'hdfs'
  group 'hdfs'
end

%w(cdap yarn).each do |u|
  execute "initaction-create-hdfs-mr-jhs-staging-intermediate-done-dir-#{u}" do
    not_if "hdfs dfs -test -d /tmp/hadoop-yarn/staging/history/done_intermediate/#{u}", :user => u
    command "hdfs dfs -mkdir -p /tmp/hadoop-yarn/staging/history/done_intermediate/#{u} && hdfs dfs -chown #{u} /tmp/hadoop-yarn/staging/history/done_intermediate/#{u} && hdfs dfs -chmod ugo+rx /tmp/hadoop-yarn/staging/history/done_intermediate/#{u}"
    timeout 300
    user 'hdfs'
    group 'hdfs'
  end
  execute "initaction-create-hdfs-mr-jhs-staging-done-dir-#{u}" do
    not_if "hdfs dfs -test -d /tmp/hadoop-yarn/staging/history/done/#{u}", :user => u
    command "hdfs dfs -mkdir -p /tmp/hadoop-yarn/staging/history/done/#{u} && hdfs dfs -chown #{u} /tmp/hadoop-yarn/staging/history/done/#{u} && hdfs dfs -chmod ugo+rx /tmp/hadoop-yarn/staging/history/done/#{u}"
    timeout 300
    user 'hdfs'
    group 'hdfs'
  end
end

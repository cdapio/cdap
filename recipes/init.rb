#
# Cookbook Name:: cdap
# Recipe:: init
#
# Copyright © 2013-2017 Cask Data, Inc.
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
# Retries allow for orchestration scenarios where HDFS is starting up
ns_path = node['cdap']['cdap_site']['hdfs.namespace']
hdfs_user = node['cdap']['cdap_site']['hdfs.user']
# Workaround for CDAP-3817, by pre-creating the transaction service snapshot directory
tx_snapshot_dir =
  if node['cdap']['cdap_site'].key?('data.tx.snapshot.dir')
    node['cdap']['cdap_site']['data.tx.snapshot.dir']
  else
    "#{ns_path}/tx.snapshot"
  end
user_path = "/user/#{hdfs_user}"

%W(#{ns_path} #{tx_snapshot_dir} #{user_path}).each do |path|
  execute "initaction-create-hdfs-path#{path.tr('/', '-')}" do
    command "hadoop fs -mkdir -p #{path} && hadoop fs -chown #{hdfs_user} #{path}"
    not_if "hadoop fs -test -d #{path}", :user => hdfs_user
    timeout 300
    user node['cdap']['fs_superuser']
    retries 3
    retry_delay 10
  end
end

%w(cdap yarn mapr).each do |u|
  %w(done done_intermediate).each do |dir|
    execute "initaction-create-hdfs-mr-jhs-staging-#{dir.tr('_', '-')}-#{u}" do
      only_if "getent passwd #{u}"
      not_if "hadoop fs -test -d /tmp/hadoop-yarn/staging/history/#{dir}/#{u}", :user => u
      command "hadoop fs -mkdir -p /tmp/hadoop-yarn/staging/history/#{dir}/#{u} && hadoop fs -chown #{u} /tmp/hadoop-yarn/staging/history/#{dir}/#{u} && hadoop fs -chmod 770 /tmp/hadoop-yarn/staging/history/#{dir}/#{u}"
      timeout 300
      user node['cdap']['fs_superuser']
      retries 3
      retry_delay 10
    end
  end
end

if hadoop_kerberos?
  include_recipe 'krb5'
  include_recipe 'krb5::rkerberos_gem'
end

princ =
  if node['cdap']['cdap_site'].key?('cdap.master.kerberos.principal')
    node['cdap']['cdap_site']['cdap.master.kerberos.principal']
  else
    "#{hdfs_user}/_HOST"
  end

# Set principal with _HOST in config, so configs match across cluster
node.default['cdap']['cdap_site']['cdap.master.kerberos.principal'] = princ
# Substitite _HOST with FQDN, for creating actual principals
princ.gsub!('_HOST', node['fqdn'])

krb5_principal princ do
  randkey true
  action :create
  only_if { hadoop_kerberos? }
end

keytab =
  if node['cdap']['cdap_site'].key?('cdap.master.kerberos.keytab')
    node['cdap']['cdap_site']['cdap.master.kerberos.keytab']
  else
    "#{node['krb5']['keytabs_dir']}/cdap.service.keytab"
  end

node.default['cdap']['cdap_site']['cdap.master.kerberos.keytab'] = keytab

krb5_keytab keytab do
  principals [princ]
  owner 'cdap'
  group 'cdap'
  mode '0640'
  action :create
  only_if { hadoop_kerberos? }
end

# Template for HBase grant - this is done here to ensure it's present
template "#{Chef::Config[:file_cache_path]}/hbase-grant.hbase" do
  source 'hbase-shell.erb'
  owner 'hbase'
  group 'hadoop'
  action :create
  only_if { hadoop_kerberos? }
end

hbkt = "#{node['krb5']['keytabs_dir']}/hbase.service.keytab"
execute 'kinit-as-hbase-and-grant' do
  command "kinit -kt #{hbkt} hbase/#{node['fqdn']} && hbase shell #{Chef::Config[:file_cache_path]}/hbase-grant.hbase"
  user 'hbase'
  only_if { hadoop_kerberos? }
end

#
# Cookbook Name:: cdap
# Recipe:: master
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

pkgs = %w(cdap-hbase-compat-0.94 cdap-hbase-compat-0.96)
unless node['cdap']['version'].to_f >= 2.6
  unless node['cdap']['version'].split('.')[2].to_i >= 9000
    pkgs += ['cdap-hbase-compat-0.98']
  end
end

pkgs.each do |pkg|
  package pkg do
    version node['cdap']['version']
    action :install
  end
end

package 'cdap-master' do
  action :install
  version node['cdap']['version']
end

# Include kerberos support
if node['hadoop'].key?('core_site') && node['hadoop']['core_site'].key?('hadoop.security.authentication') &&
   node['hadoop']['core_site']['hadoop.security.authentication'] == 'kerberos'

  if node['cdap'].key?('security') && node['cdap']['security'].key?('cdap_keytab') &&
     node['cdap']['security'].key?('cdap_principal') &&
     node['cdap'].key?('cdap_site') && node['cdap']['cdap_site'].key?('kerberos.auth.enabled') &&
     node['cdap']['cdap_site']['kerberos.auth.enabled'].to_s == 'true'

    my_vars = { :options => node['cdap']['security'] }

    directory '/etc/default' do
      owner 'root'
      group 'root'
      mode '0755'
      action :create
    end

    template '/etc/default/cdap-master' do
      source 'generic-env.sh.erb'
      mode '0755'
      owner 'root'
      group 'root'
      action :create
      variables my_vars
    end # End /etc/default/cdap-master

    include_recipe 'yum-epel' if node['platform_family'] == 'rhel'

    package 'kstart'

    group 'hadoop' do
      append true
      members ['cdap']
      action :modify
    end

    include_recipe 'krb5_utils'
    # We need to be hbase to run our shell
    execute 'kinit-as-hbase-user' do
      command "kinit -kt #{node['krb5_utils']['keytabs_dir']}/hbase.service.keytab hbase/#{node['fqdn']}@#{node['krb5']['krb5_conf']['realms']['default_realm'].upcase}"
      user 'hbase'
      only_if "test -e #{node['krb5_utils']['keytabs_dir']}/hbase.service.keytab"
    end
    # Template for HBase GRANT
    template "#{Chef::Config[:file_cache_path]}/hbase-grant.hbase" do
      source 'hbase-shell.erb'
      owner 'hbase'
      group 'hadoop'
      action :create
    end
    execute 'hbase-grant' do
      command "hbase shell #{Chef::Config[:file_cache_path]}/hbase-grant.hbase"
      user 'hbase'
    end
  else
    # Hadoop is secure, but we're not configured for Kerberos
    log 'bad-security-configuration' do
      message "Invalid security configuration: You must specify node['cdap']['cdap_site']['kerberos.auth.enabled']"
      level :error
    end
    Chef::Application.fatal!('Invalid Hadoop/CDAP security configuration')
  end
end

service 'cdap-master' do
  status_command 'service cdap-master status'
  action :nothing
end

#
# Cookbook Name:: cdap
# Recipe:: security
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

include_recipe 'java::default'
include_recipe 'cdap::repo'

package 'cdap-security' do
  action :install
  version node['cdap']['version']
end

template '/etc/init.d/cdap-auth-server' do
  source 'cdap-service.erb'
  mode 0755
  owner 'root'
  group 'root'
  action :create
  variables node['cdap']['security']
end

# Create a new keystore if SSL is enabled
execute 'create-security-server-ssl-keystore' do
  ssl_enabled =
    if node['cdap']['version'].to_f < 2.5 && node['cdap'].key?('cdap_site') &&
       node['cdap']['cdap_site'].key?('security.server.ssl.enabled')
      node['cdap']['cdap_site']['security.server.ssl.enabled']
    elsif node['cdap'].key?('cdap_site') && node['cdap']['cdap_site'].key?('ssl.enabled')
      node['cdap']['cdap_site']['ssl.enabled']
    # This one is here for compatibility, but ssl.enabled takes precedence, if set
    elsif node['cdap'].key?('cdap_site') && node['cdap']['cdap_site'].key?('security.server.ssl.enabled')
      node['cdap']['cdap_site']['security.server.ssl.enabled']
    else
      false
    end

  password = node['cdap']['cdap_site']['security.server.ssl.keystore.password']
  keypass = node['cdap']['cdap_site']['security.server.ssl.keystore.keypassword']
  path = node['cdap']['cdap_site']['security.server.ssl.keystore.path']
  common_name = node['cdap']['security']['ssl_common_name']
  jks =
    if node['cdap']['cdap_site'].key?('security.server.ssl.keystore.type') &&
       node['cdap']['cdap_site']['security.server.ssl.keystore.type'] != 'JKS'
      false
    else
      true
    end

  command "keytool -genkey -noprompt -alias ext-auth -keysize 2048 -keyalg RSA -keystore #{path} -storepass #{password} -keypass #{keypass} -dname 'CN=#{common_name}, OU=cdap, O=cdap, L=Palo Alto, ST=CA, C=US'"
  not_if { File.exist?(path) }
  only_if { ssl_enabled && jks }
end

# Manage Authentication realmfile
if node['cdap']['security']['manage_realmfile'].to_s == 'true' &&
   node.key?('cdap') && node['cdap'].key?('cdap_site') && node['cdap']['cdap_site'].key?('security.authentication.handlerClassName') &&
   node['cdap']['cdap_site']['security.authentication.handlerClassName'] == 'co.cask.cdap.security.server.BasicAuthenticationHandler' &&
   node['cdap']['cdap_site'].key?('security.authentication.basic.realmfile')
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
    mode 0644
    owner 'cdap'
    group 'cdap'
    variables options: node['cdap']['security']['realmfile']
    action :create
  end
end

service 'cdap-auth-server' do
  status_command 'service cdap-auth-server status'
  action node['cdap']['security']['init_actions']
end

#
# Cookbook Name:: cdap
# Recipe:: security
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

include_recipe 'java::default'
include_recipe 'cdap::repo'

package 'cdap-security' do
  action :install
  version node['cdap']['version']
end

include_recipe 'cdap::ssl_keystore_certificates'

template '/etc/init.d/cdap-auth-server' do
  source 'cdap-service.erb'
  mode '0755'
  owner 'root'
  group 'root'
  action :create
  variables node['cdap']['security']
end

# Manage Authentication realmfile
if node['cdap']['security']['manage_realmfile'].to_s == 'true' &&
   node.key?('cdap') && node['cdap'].key?('cdap_site') && node['cdap']['cdap_site'].key?('security.authentication.handlerClassName') &&
   node['cdap']['cdap_site']['security.authentication.handlerClassName'] == 'co.cask.cdap.security.server.BasicAuthenticationHandler' &&
   node['cdap']['cdap_site'].key?('security.authentication.basic.realmfile')
  include_recipe 'cdap::security_realm_file'
end

service 'cdap-auth-server' do
  status_command 'service cdap-auth-server status'
  action node['cdap']['security']['init_actions']
end

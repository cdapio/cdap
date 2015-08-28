#
# Cookbook Name:: cdap
# Recipe:: gateway
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

include_recipe 'cdap::default'

package 'cdap-gateway' do
  action :install
  version node['cdap']['version']
end

# Create a new keystore if SSL is enabled (and we don't already have one in place)
execute 'create-router-ssl-keystore' do
  ssl_enabled =
    if node['cdap']['version'].to_f < 2.5 && node['cdap'].key?('cdap_site') &&
       node['cdap']['cdap_site'].key?('security.server.ssl.enabled')
      node['cdap']['cdap_site']['security.server.ssl.enabled']
    elsif node['cdap'].key?('cdap_site') && node['cdap']['cdap_site'].key?('ssl.enabled')
      node['cdap']['cdap_site']['ssl.enabled']
    # This one is here for compatibility, but ssl.enabled takes precidence, if set
    elsif node['cdap'].key?('cdap_site') && node['cdap']['cdap_site'].key?('security.server.ssl.enabled')
      node['cdap']['cdap_site']['security.server.ssl.enabled']
    else
      false
    end

  password = node['cdap']['cdap_site']['router.ssl.keystore.password']
  keypass = node['cdap']['cdap_site']['router.ssl.keystore.keypassword']
  path = node['cdap']['cdap_site']['router.ssl.keystore.path']
  common_name = node['cdap']['security']['ssl_common_name']
  jks =
    if node['cdap']['cdap_site'].key?('router.ssl.keystore.type') && node['cdap']['cdap_site']['router.ssl.keystore.type'] != 'JKS'
      false
    else
      true
    end

  command "keytool -genkey -noprompt -alias ext-auth -keysize 2048 -keyalg RSA -keystore #{path} -storepass #{password} -keypass #{keypass} -dname 'CN=#{common_name}, OU=cdap, O=cdap, L=Palo Alto, ST=CA, C=US'"
  not_if { File.exist?(path) }
  only_if { ssl_enabled && jks }
end

svcs = ['cdap-router']
unless node['cdap']['version'].to_f >= 2.6
  unless node['cdap']['version'].split('.')[2].to_i >= 9000
    svcs += ['cdap-gateway']
  end
end

svcs.each do |svc|
  attrib = svc.gsub('cdap-', '').tr('-', '_')
  template "/etc/init.d/#{svc}" do
    source 'cdap-service.erb'
    mode 0755
    owner 'root'
    group 'root'
    action :create
    variables node['cdap'][attrib]
  end

  service svc do
    status_command "service #{svc} status"
    action node['cdap'][attrib]['init_actions']
  end
end

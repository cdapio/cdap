#
# Cookbook Name:: cdap
# Recipe:: security_init
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

include_recipe 'cdap::security'

# Create a new keystore if SSL is enabled
execute 'create-security-server-ssl-keystore' do
  ssl_enabled =
    if node['cdap']['version'].to_f < 2.5 && node['cdap'].key?('cdap_site') &&
       node['cdap']['cdap_site'].key?('security.server.ssl.enabled')
      node['cdap']['cdap_site']['security.server.ssl.enabled']
    elsif node['cdap'].key?('cdap_site') && node['cdap']['cdap_site'].key?('ssl.enabled')
      node['cdap']['cdap_site']['ssl.enabled']
    else
      false
    end

  password = node['cdap']['cdap_site']['security.server.ssl.keystore.password']
  path = node['cdap']['cdap_site']['security.server.ssl.keystore.path']
  common_name = node['cdap']['security']['ssl_common_name']

  command "keytool -genkey -noprompt -alias ext-auth -keysize 2048 -keyalg RSA -keystore #{path} -storepass #{password} -keypass #{password} -dname 'CN=#{common_name}, OU=cdap, O=cdap, L=Palo Alto, ST=CA, C=US'"
  not_if { File.exist?(path) }
  only_if { ssl_enabled }
end

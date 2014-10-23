#
# Cookbook Name:: cdap
# Recipe:: web_app_init
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
# Dependencies
# Need to make sure certpath and keypath attributes are set

include_recipe 'cdap::web_app'

### Generate a certificate if SSL is enabled
execute 'generate-webapp-ssl-cert' do
  ssl_enabled = node['cdap']['cdap_site']['ssl.enabled'] || false
  common_name = node['cdap']['security']['ssl_common_name']
  keypath = node['cdap']['cdap_site']['dashboard.ssl.key']
  certpath = node['cdap']['cdap_site']['dashboard.ssl.cert']
  command "openssl req -x509 -nodes -days 3650 -newkey rsa:2048 -keyout #{keypath} -out #{certpath} -subj '/C=US/ST=CA/L=Palo Alto/OU=cdap/O=cdap/CN=#{common_name}'"
  not_if { File.exist?(certpath) && File.exist?(keypath) }
  only_if { ssl_enabled }
end

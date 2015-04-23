#
# Cookbook Name:: cdap
# Recipe:: web_app
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

include_recipe 'nodejs::default'
link '/usr/bin/node' do
  to '/usr/local/bin/node'
  action :create
  not_if 'test -e /usr/bin/node'
end

include_recipe 'cdap::repo'

package 'cdap-web-app' do
  action :install
  version node['cdap']['version']
end

if node['cdap'].key?('web_app')
  my_vars = { :options => node['cdap']['web_app'] }

  directory '/etc/default' do
    owner 'root'
    group 'root'
    mode '0755'
    action :create
  end

  template '/etc/default/cdap-web-app' do
    source 'generic-env.sh.erb'
    mode '0755'
    owner 'root'
    group 'root'
    action :create
    variables my_vars
  end # End /etc/default/cdap-web-app
end

service 'cdap-web-app' do
  status_command 'service cdap-web-app status'
  action :nothing
end

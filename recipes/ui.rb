#
# Cookbook Name:: cdap
# Recipe:: ui
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

# Starting with CDAP 3.4, we ship Node.js with the cdap-ui package
if node['cdap']['version'].to_f < 3.4
  include_recipe 'nodejs::default'
  link '/usr/bin/node' do
    to '/usr/local/bin/node'
    action :create
    not_if 'test -e /usr/bin/node'
  end
end

include_recipe 'cdap::repo'

package 'cdap-ui' do
  action :install
  version node['cdap']['version']
end

include_recipe 'cdap::ssl_keystore_certificates'

template '/etc/init.d/cdap-ui' do
  source 'cdap-service.erb'
  mode '0755'
  owner 'root'
  group 'root'
  action :create
  variables node['cdap']['ui']
end

service 'cdap-ui' do
  status_command 'service cdap-ui status'
  action node['cdap']['ui']['init_actions']
end

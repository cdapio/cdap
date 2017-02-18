#
# Cookbook Name:: cdap
# Recipe:: base
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

# Install Java and Hadoop Clients
unless node['cdap'].key?('skip_prerequisites') && node['cdap']['skip_prerequisites'].to_s == 'true'
  include_recipe 'cdap::prerequisites'
end

include_recipe 'cdap::repo'

# add global CDAP_HOME environment variable
file '/etc/profile.d/cdap_home.sh' do
  content <<-EOS
    export CDAP_HOME=/opt/cdap
    export PATH=$PATH:$CDAP_HOME/bin
  EOS
  mode '0755'
end

# Install cdap base package
package 'cdap' do
  action :install
  version node['cdap']['version']
end

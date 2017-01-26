#
# Cookbook Name:: cdap
# Recipe:: cli
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

# Install Java, unless skip_prerequisites
unless node['cdap'].key?('skip_prerequisites') && node['cdap']['skip_prerequisites'].to_s == 'true'
  include_recipe 'java'
end

include_recipe 'cdap::repo'

# Install base and cli packages. Order matters.
%w(cdap cdap-cli).each do |pkg|
  package pkg do
    action :install
    version node['cdap']['version']
  end
end

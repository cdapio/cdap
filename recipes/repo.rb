#
# Cookbook Name:: cdap
# Recipe:: repo
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

case node['platform_family']
when 'debian'
  include_recipe 'apt'
  apt_repository 'cask' do
    uri node['cdap']['repo']['apt_repo_url']
    distribution node['lsb']['codename']
    components node['cdap']['repo']['apt_components']
    action :add
    arch 'amd64'
    key "#{node['cdap']['repo']['apt_repo_url']}/#{node['cdap']['repo']['repo_version']}/pubkey.gpg"
  end
when 'rhel'
  include_recipe 'yum'
  yum_repository 'cask' do
    description 'Cask YUM repository'
    url node['cdap']['repo']['yum_repo_url']
    gpgkey "#{node['cdap']['repo']['yum_repo_url']}/#{node['cdap']['repo']['repo_version']}/pubkey.gpg"
    gpgcheck true
    action :add
  end
end

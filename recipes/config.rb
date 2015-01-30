#
# Cookbook Name:: cdap
# Recipe:: config
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

cdap_conf_dir = "/etc/cdap/#{node['cdap']['conf_dir']}"

directory cdap_conf_dir do
  mode '0755'
  owner 'root'
  group 'root'
  action :create
  recursive true
end

# Setup cdap-site.xml
if node['cdap'].key?('cdap_site')
  my_vars = { :options => node['cdap']['cdap_site'] }

  template "#{cdap_conf_dir}/cdap-site.xml" do
    source 'generic-site.xml.erb'
    mode 0644
    owner 'cdap'
    group 'cdap'
    variables my_vars
    action :create
  end
end # End cdap-site.xml

# Setup cdap-security.xml
if node['cdap'].key?('cdap_security')
  my_vars = { :options => node['cdap']['cdap_security'] }

  template "#{cdap_conf_dir}/cdap-security.xml" do
    source 'generic-site.xml.erb'
    mode 0600
    owner 'cdap'
    group 'cdap'
    variables my_vars
    action :create
  end
end # End cdap-security.xml

# Setup cdap-env.sh
if node['cdap'].key?('cdap_env') && node['cdap']['version'].to_f >= 2.7
  my_vars = { :options => node['cdap']['cdap_env'] }

  template "#{cdap_conf_dir}/cdap-env.sh" do
    source 'generic-env.sh.erb'
    mode 0644
    owner 'cdap'
    group 'cdap'
    variables my_vars
    action :create
  end
end # End cdap-env.sh

execute 'copy logback.xml from conf.dist' do
  command "cp /etc/cdap/conf.dist/logback.xml /etc/cdap/#{node['cdap']['conf_dir']}"
  not_if { ::File.exist?("/etc/cdap/#{node['cdap']['conf_dir']}/logback.xml") }
end

# Update alternatives to point to our configuration
execute 'update cdap-conf alternatives' do
  command "update-alternatives --install /etc/cdap/conf cdap-conf /etc/cdap/#{node['cdap']['conf_dir']} 50"
  not_if "update-alternatives --display cdap-conf | grep best | awk '{print $5}' | grep /etc/cdap/#{node['cdap']['conf_dir']}"
end

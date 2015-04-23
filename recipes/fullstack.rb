#
# Cookbook Name:: cdap
# Recipe:: fullstack
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

ui_recipe = node['cdap']['version'].to_i < 3 ? 'web_app' : 'ui'

%W(cli gateway kafka master #{ui_recipe}).each do |recipe|
  include_recipe "cdap::#{recipe}"
end

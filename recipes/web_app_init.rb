#
# Cookbook Name:: cdap
# Recipe:: web_app_init
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
# Dependencies
# Need to make sure certpath and keypath attributes are set

# web_app is deprecated in favor of ui in CDAP 3.0
if node['cdap']['version'].to_i > 2
  include_recipe 'cdap::ui'
else
  include_recipe 'cdap::web_app'
end
Chef::Log.warn('The cdap::web_app_init recipe is deprecated. Please, remove it from your run_list.')

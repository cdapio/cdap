#
# Cookbook Name:: cdap
# Recipe:: upgrade
#
# Copyright © 2015 Cask Data, Inc.
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

master_init_actions = node['cdap']['master']['init_actions']

# Ensure master service is stopped
node.override['cdap']['master']['init_actions'] = [:stop]

include_recipe 'cdap::master'

# Run the CDAP Upgrade Tool
ruby_block 'run-cdap-upgrade-tool' do # ~FC037
  block do
    resources('execute[cdap-upgrade-tool]').run_action(:run)
  end
  master_init_actions.each do |action|
    notifies action.to_sym, 'service[cdap-master]', :immediately unless action == :nothing
  end
end

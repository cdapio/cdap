#
# Cookbook Name:: cdap
# Attribute:: ambari
#
# Copyright © 2016-2017 Cask Data, Inc.
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

# Version of CDAP Ambari Service
default['cdap']['ambari']['version'] = '4.0.2-1'
# Should cdap::ambari recipe install/manage Ambari
default['cdap']['ambari']['install_ambari'] = node['cdap']['ambari']['install'] || false

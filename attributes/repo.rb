#
# Cookbook Name:: cdap
# Attribute:: repo
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

# URL to repository
default['cdap']['repo']['apt_repo_url'] = 'http://repository.cask.co/ubuntu/precise/amd64/cdap/2.7'
default['cdap']['repo']['apt_components'] = ['cdap']
default['cdap']['repo']['yum_repo_url'] = 'http://repository.cask.co/centos/6/x86_64/cdap/2.7'

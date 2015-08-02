#
# Cookbook Name:: cdap
# Attribute:: sdk
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

# URL to repository
ver = node['cdap']['version'].gsub(/-.*/, '')
default['cdap']['sdk']['url'] = "http://repository.cask.co/downloads/co/cask/cdap/cdap-sdk/#{ver}/cdap-sdk-#{ver}.zip"
# shasum -a 256 filename
default['cdap']['sdk']['checksum'] =
  case ver
  when '2.8.0'
    '1f5824a67fcbb5b2fcec02524d59b7befd1c315ed4046d02221fe8f54bbf233a'
  when '2.8.2'
    'add767a5e5a0828e13178cc78854c17b5d6b9b2f43b669622274ddc53dd5703b'
  when '3.0.0'
    'a45125208015c8ade3ac3a01002a9989ccc8432bcb5785e2625d17a406cabcff'
  when '3.0.1'
    'c6881a84c3189679e98145a2eeb8ef5d976ddcf42698ddc64d702c2b7b995776'
  when '3.0.2'
    '67f6e93b47591ea7bdbc7e66416f9858ab0c75eb3c9c338bf109b81089310e52'
  when '3.0.3'
    '54a4cc4ef0a0148a97b571de5284660b393f2277eea86c70a94d8a83208187b1'
  when '3.1.0'
    '0b868aafcd0801e35ab52ead70537a63ec0eb5c458c6e7c3ace363a8fd4d48d2'
  end
default['cdap']['sdk']['install_path'] = '/opt/cdap'
default['cdap']['sdk']['user'] = 'cdap'
default['cdap']['sdk']['manage_user'] = true
default['cdap']['sdk']['init_name'] = 'SDK'
default['cdap']['sdk']['init_krb5'] = false
default['cdap']['sdk']['init_cmd'] = "#{node['cdap']['sdk']['install_path']}/sdk/bin/cdap.sh"
default['cdap']['sdk']['init_actions'] = [:enable, :start]

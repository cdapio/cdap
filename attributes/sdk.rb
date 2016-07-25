#
# Cookbook Name:: cdap
# Attribute:: sdk
#
# Copyright © 2015-2016 Cask Data, Inc.
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
  when '3.0.4'
    '4681cdf5a638f877a6b12a6bb03f0feb78c506c1b98b0514dbd98b3eb9ab13e8'
  when '3.0.5'
    'ccbd4b7f74544a0e849c9e2eed7189ece7c3bb5e1af2eb6ed8b11e28ff28224e'
  when '3.1.0'
    '0b868aafcd0801e35ab52ead70537a63ec0eb5c458c6e7c3ace363a8fd4d48d2'
  when '3.1.1'
    '5b70b537bbf64b2d3db9cec7c44d9c2b7697314bcc7b52ef6bdf22b1a40b6ca5'
  when '3.1.2'
    'f68310e84c821ae149842909a70dd4999fbc91404285409b6c719ea749057683'
  when '3.2.0'
    '45981e5add79d83fa957e3d67c3941c676d323ebe61ab6a6ccf07465e0d574ae'
  when '3.2.1'
    '565dd06caf3201e964dba5ccde1148842d159d297b87ecab51c40249715ebcf5'
  when '3.2.2'
    '0ee53307bd6bbd126e19848f28ca45f1cfe7ef155e48ba36b30fddbb8b8730d3'
  when '3.3.0'
    '19ff27493bb79fa377fc10dc57c08e66ce7836fa3696b09a13dd416150ad40c4'
  when '3.3.1'
    '11ccfc43934e1259f322436d68fbd30e85e35c30f578ce024ad0ec2253db7c16'
  when '3.3.2'
    'ec03e7a2d699d83c35d3206277a9b5109bed717483f86b0a24cecff65639d407'
  when '3.3.3'
    'a1ac15cff362a8db7b31217d3e015a66cd2e09cca6a4ea3d1971c0eddabd4fc6'
  when '3.3.4'
    'b98822c58dd7230d1f7246e89586d798c34547591daf7881c97f88ad98d77562'
  when '3.3.5'
    '953b9b7244e8075c78881695d9efb3dbd43f30e98adeafb0e28b857c98e142fd'
  when '3.3.6'
    '2ae37dc2cebc4c0f99b80a0437e1af52053180e82ad66b6951bf02d16e952345'
  when '3.4.0'
    '1876673b010ff6e795418272ed3ecf171fae340b6f489fea8f6b2d3fe0d42e3f'
  when '3.4.1'
    '82c31e1e6d5b371ea7dedf79f2cd9be9cd4ba8a3cfe3ef36ff2cd7b38fc91a4d'
  when '3.4.2'
    '6103e5d7f3fa2f91057511703f12fa513235e6f5ff52d4e10ead4bbc8727954c'
  when '3.4.3'
    'b834eda1137f8423aec2e45dd917f14b86009cfa061c6487f3d20ae29017db8d'
  end
default['cdap']['sdk']['install_path'] = '/opt/cdap'
default['cdap']['sdk']['user'] = 'cdap'
default['cdap']['sdk']['manage_user'] = true
default['cdap']['sdk']['init_name'] = 'SDK'
default['cdap']['sdk']['init_krb5'] = false
default['cdap']['sdk']['init_cmd'] = "#{node['cdap']['sdk']['install_path']}/sdk/bin/cdap.sh"
default['cdap']['sdk']['init_actions'] = [:enable, :start]

#
# Cookbook Name:: cdap
# Attribute:: config
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

# Default: conf.chef
default['cdap']['conf_dir'] = 'conf.chef'
# Default: 2.6.0-1
default['cdap']['version'] = '2.6.0-1'
# cdap-site.xml
default['cdap']['cdap_site']['root.namespace'] = 'cdap'
# ideally we could put the macro '/${cdap.namespace}' here but this attribute is used elsewhere in the cookbook
default['cdap']['cdap_site']['hdfs.namespace'] = "/#{node['cdap']['cdap_site']['root.namespace']}"
default['cdap']['cdap_site']['hdfs.user'] = 'yarn'
default['cdap']['cdap_site']['kafka.log.dir'] = '/data/cdap/kafka-logs'
default['cdap']['cdap_site']['kafka.broker.quorum'] = "#{node['fqdn']}:9092"
default['cdap']['cdap_site']['kafka.seed.brokers'] = "#{node['fqdn']}:9092"
default['cdap']['cdap_site']['kafka.default.replication.factor'] = '1'
default['cdap']['cdap_site']['log.retention.duration.days'] = '7'
default['cdap']['cdap_site']['zookeeper.quorum'] = "#{node['fqdn']}:2181/#{node['cdap']['cdap_site']['root.namespace']}"
default['cdap']['cdap_site']['router.bind.address'] = node['fqdn']
default['cdap']['cdap_site']['router.server.address'] = node['fqdn']
default['cdap']['cdap_site']['app.bind.address'] = node['fqdn']
default['cdap']['cdap_site']['data.tx.bind.address'] = node['fqdn']
default['cdap']['cdap_site']['metrics.query.bind.address'] = node['fqdn']
default['cdap']['cdap_site']['dashboard.bind.port'] = '9999'
default['cdap']['cdap_site']['log.saver.run.memory.megs'] = '512'
# These are only used with CDAP < 2.6
if node['cdap']['version'].to_f < 2.6
  default['cdap']['cdap_site']['gateway.server.address'] = node['fqdn']
  default['cdap']['cdap_site']['gateway.server.port'] = '10000'
  default['cdap']['cdap_site']['gateway.memory.mb'] = '512'
end

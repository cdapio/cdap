#
# Cookbook Name:: cdap
# Attribute:: services
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

name = 'gateway'
default['cdap'][name]['user'] = 'cdap'
default['cdap'][name]['init_name'] = name.split.map(&:capitalize).join(' ')
default['cdap'][name]['init_krb5'] = false
default['cdap'][name]['init_cmd'] = "/opt/cdap/#{name}/bin/svc-#{name}"
default['cdap'][name]['init_actions'] = [:nothing]

name = 'kafka'
default['cdap'][name]['user'] = 'cdap'
default['cdap'][name]['init_name'] = 'Kafka Server'
default['cdap'][name]['init_krb5'] = false
default['cdap'][name]['init_cmd'] = "/opt/cdap/#{name}/bin/svc-#{name}-server"
default['cdap'][name]['init_actions'] = [:nothing]

name = 'master'
default['cdap'][name]['user'] = 'cdap'
default['cdap'][name]['init_name'] = name.split.map(&:capitalize).join(' ')
default['cdap'][name]['init_krb5'] = true
default['cdap'][name]['init_cmd'] = "/opt/cdap/#{name}/bin/svc-#{name}"
default['cdap'][name]['init_actions'] = [:nothing]

name = 'router'
default['cdap'][name]['user'] = 'cdap'
default['cdap'][name]['init_name'] = name.split.map(&:capitalize).join(' ')
default['cdap'][name]['init_krb5'] = false
default['cdap'][name]['init_cmd'] = "/opt/cdap/gateway/bin/svc-#{name}"
default['cdap'][name]['init_actions'] = [:nothing]

name = 'security'
default['cdap'][name]['user'] = 'cdap'
default['cdap'][name]['init_name'] = 'Auth Server'
default['cdap'][name]['init_krb5'] = false
default['cdap'][name]['init_cmd'] = "/opt/cdap/#{name}/bin/svc-auth-server"
default['cdap'][name]['init_actions'] = [:nothing]

name = 'ui'
default['cdap'][name]['user'] = 'cdap'
default['cdap'][name]['init_name'] = name.upcase
default['cdap'][name]['init_krb5'] = false
default['cdap'][name]['init_cmd'] = "/opt/cdap/#{name}/bin/svc-#{name}"
default['cdap'][name]['init_actions'] = [:nothing]

name = 'web_app'
default['cdap'][name]['user'] = 'cdap'
default['cdap'][name]['init_name'] = name.split('_').map(&:capitalize).join(' ')
default['cdap'][name]['init_krb5'] = false
default['cdap'][name]['init_cmd'] = "/opt/cdap/#{name}/bin/svc-#{name.gsub('_', '-')}"
default['cdap'][name]['init_actions'] = [:nothing]

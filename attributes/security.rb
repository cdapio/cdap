#
# Cookbook Name:: cdap
# Attribute:: security
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

# auth
default['cdap']['cdap_site']['security.server.ssl.keystore.password'] = 'defaultpassword'
default['cdap']['cdap_site']['security.server.ssl.keystore.path'] = '/opt/cdap/security/conf/keystore.jks'
default['cdap']['cdap_site']['security.auth.server.address'] = node['fqdn']

# web ui
default['cdap']['cdap_site']['dashboard.ssl.key'] = "/etc/cdap/#{node['cdap']['conf_dir']}/webapp.key"
default['cdap']['cdap_site']['dashboard.ssl.cert'] = "/etc/cdap/#{node['cdap']['conf_dir']}/webapp.crt"

default['cdap']['security']['ssl_common_name'] = node['fqdn']

if node['cdap']['cdap_site'].key?('kerberos.auth.enabled') && node['cdap']['cdap_site']['kerberos.auth.enabled'].to_s == 'true'
  include_attribute 'krb5_utils'

  # For keytab creation
  default['krb5_utils']['krb5_service_keytabs']['cdap'] = { 'owner' => 'cdap', 'group' => 'cdap', 'mode' => '0640' }

  default_realm = node['krb5']['krb5_conf']['realms']['default_realm'].upcase

  # For cdap-master init script
  default['cdap']['security']['cdap_keytab'] = "#{node['krb5_utils']['keytabs_dir']}/cdap.service.keytab"
  default['cdap']['security']['cdap_principal'] = "cdap/#{node['fqdn']}@#{default_realm}"

  # Add cdap user to YARN container-executor.cfg's allowed.system.users
  if node['hadoop'].key?('container_executor') && node['hadoop']['container_executor'].key?('allowed.system.users')
    arr = node['hadoop']['container_executor']['allowed.system.users'].split(',')
    user = node['cdap']['security']['cdap_principal'].split(/[@\/]/).first
    unless arr.include?(user)
      arr += [user]
      default['hadoop']['container_executor']['allowed.system.users'] = arr.join(',')
    end
  end

  # For cdap-auth-server and cdap-router
  default['cdap']['cdap_site']['cdap.master.kerberos.keytab'] = node['cdap']['security']['cdap_keytab']
  default['cdap']['cdap_site']['cdap.master.kerberos.principal'] = node['cdap']['security']['cdap_principal']

end

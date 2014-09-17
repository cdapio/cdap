#
# Cookbook Name:: cdap
# Attribute:: security
#
# Copyright (C) 2013-2014 Continuuity, Inc.
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

default['cdap']['cdap_site']['security.server.ssl.keystore.password'] = 'defaultpassword'
default['cdap']['cdap_site']['security.server.ssl.keystore.path'] = '/opt/cdap/security/conf/keystore.jks'
default['cdap']['cdap_site']['security.auth.server.address'] = node['fqdn']

default['cdap']['security']['ssl_common_name'] = node['fqdn']

if node['cdap']['cdap_site'].key?('kerberos.auth.enabled') && node['cdap']['cdap_site']['kerberos.auth.enabled'].to_s == 'true'
  include_attribute 'krb5_utils'

  # For keytab creation
  default['krb5_utils']['krb5_service_keytabs']['cdap'] = { 'owner' => 'cdap', 'group' => 'cdap', 'mode' => '0640' }

  default_realm = node['krb5']['krb5_conf']['realms']['default_realm'].upcase

  # For cdap-master init script
  default['cdap']['security']['cdap_keytab'] = "#{node['krb5_utils']['keytabs_dir']}/yarn.service.keytab"
  default['cdap']['security']['cdap_principal'] = "yarn/`hostname -f`@#{default_realm}"

  # For cdap-auth-server and cdap-router
  default['cdap']['cdap_site']['cdap.master.kerberos.keytab'] = "#{node['krb5_utils']['keytabs_dir']}/cdap.service.keytab"
  default['cdap']['cdap_site']['cdap.master.kerberos.principal'] = "cdap/_HOST@#{default_realm}"

end

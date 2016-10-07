#
# Cookbook Name:: cdap
# Recipe:: ssl_keystore_certificates
#
# Copyright © 2016 Cask Data, Inc.
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

# Create Auth Server/Router Java keystores
%w(security.server router).each do |svc|
  execute "create-#{svc}.keystore.path-keystore" do
    ssl = jks_opts(svc) if node['cdap'].key?('cdap_security')
    command <<-EOS
    keytool -genkey -noprompt -alias ext-auth -keysize 2048 -keyalg RSA \
      -keystore #{ssl['path']} -storepass #{ssl['password']} -keypass #{ssl['keypass']} \
      -dname 'CN=#{ssl['common_name']}, OU=cdap, O=cdap, L=Palo Alto, ST=CA, C=US'
    EOS
    not_if { ::File.exist?(ssl['path'].to_s) }
    only_if { ssl_enabled? && jks?("#{svc}.ssl.keystore.type") }
  end
end

### Generate a certificate if SSL is enabled
execute 'generate-ui-ssl-cert' do
  ssl = ssl_opts if node['cdap'].key?('cdap_security')
  command <<-EOS
  openssl req -x509 -nodes -days 3650 -newkey rsa:2048 \
    -keyout #{ssl['keypath']} -out #{ssl['certpath']} \
    -subj '/C=US/ST=CA/L=Palo Alto/OU=cdap/O=cdap/CN=#{ssl['common_name']}'
  EOS
  not_if { ::File.exist?(ssl['certpath']) && ::File.exist?(ssl['keypath']) }
  only_if { ssl_enabled? }
end

#
# Cookbook Name:: cdap
# Library:: helpers
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

module CDAP
  module Helpers
    #
    # Return true if SSL is enabled
    #
    def ssl_enabled?
      ssl_enabled =
        if node['cdap']['version'].to_f < 2.5 && node['cdap'].key?('cdap_site') &&
           node['cdap']['cdap_site'].key?('security.server.ssl.enabled')
          node['cdap']['cdap_site']['security.server.ssl.enabled']
        elsif node['cdap'].key?('cdap_site') && node['cdap']['cdap_site'].key?('ssl.enabled')
          node['cdap']['cdap_site']['ssl.enabled']
        # This one is here for compatibility, but ssl.enabled takes precedence, if set
        elsif node['cdap'].key?('cdap_site') && node['cdap']['cdap_site'].key?('security.server.ssl.enabled')
          node['cdap']['cdap_site']['security.server.ssl.enabled']
        else
          false
        end
      ssl_enabled.to_s == 'true' ? true : false
    end

    def jks?(property)
      jks =
        if node['cdap'].key?('cdap_security') && node['cdap']['cdap_security'].key?(property)
          node['cdap']['cdap_security'][property]
        else
          'JKS'
        end
      jks == 'JKS' ? true : false
    end

    # Return hash with SSL options for JKS
    def jks_opts(prefix)
      ssl = {}
      ssl['password'] = node['cdap']['cdap_security']["#{prefix}.ssl.keystore.password"]
      ssl['keypass'] =
        if node['cdap']['cdap_security'].key?("#{prefix}.ssl.keystore.keypassword")
          node['cdap']['cdap_security']["#{prefix}.ssl.keystore.keypassword"]
        else
          ssl['password']
        end
      ssl['path'] = node['cdap']['cdap_security']["#{prefix}.ssl.keystore.path"]
      ssl['common_name'] = node['cdap']['security']['ssl_common_name']
      ssl
    end

    # Return hash with SSL options for OpenSSL
    def ssl_opts
      ssl = {}
      ssl['keypath'] = node['cdap']['cdap_security']['dashboard.ssl.key']
      ssl['certpath'] = node['cdap']['cdap_security']['dashboard.ssl.cert']
      ssl['common_name'] = node['cdap']['security']['ssl_common_name']
      ssl
    end
  end
end

# Load helpers
Chef::Recipe.send(:include, CDAP::Helpers)
Chef::Resource.send(:include, CDAP::Helpers)

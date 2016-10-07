require 'spec_helper'

describe 'cdap::ssl_keystore_certificates' do
  context 'with SSL disabled' do
    let(:chef_run) do
      ChefSpec::SoloRunner.new(platform: 'centos', version: 6.6) do |node|
        node.automatic['domain'] = 'example.com'
      end.converge(described_recipe)
    end

    %w(security.server router).each do |svc|
      it "does not run execute[create-#{svc}.keystore.path-keystore]" do
        expect(chef_run).not_to run_execute("create-#{svc}.keystore.path-keystore")
      end
    end

    it 'does not run execute[generate-ui-ssl-cert]' do
      expect(chef_run).not_to run_execute('generate-ui-ssl-cert')
    end
  end

  context 'with SSL enabled' do
    let(:chef_run) do
      ChefSpec::SoloRunner.new(platform: 'centos', version: 6.6) do |node|
        node.automatic['domain'] = 'example.com'
        node.default['cdap']['cdap_site']['ssl.enabled'] = true
        node.default['cdap']['cdap_security']['security.server.ssl.keystore.password'] = 'password'
        node.default['cdap']['cdap_security']['security.server.ssl.keystore.path'] = '/tmp/security.jks'
        node.default['cdap']['cdap_security']['router.ssl.keystore.password'] = 'password'
        node.default['cdap']['cdap_security']['router.ssl.keystore.path'] = '/tmp/router.jks'
        node.default['cdap']['cdap_security']['dashboard.ssl.cert'] = '/tmp/dashboard.cert'
        node.default['cdap']['cdap_security']['dashboard.ssl.key'] = '/tmp/dashboard.key'
      end.converge(described_recipe)
    end

    %w(security.server router).each do |svc|
      it "runs execute[create-#{svc}.keystore.path-keystore]" do
        expect(chef_run).to run_execute("create-#{svc}.keystore.path-keystore")
      end
    end

    it 'runs execute[generate-ui-ssl-cert]' do
      expect(chef_run).to run_execute('generate-ui-ssl-cert')
    end
  end
end

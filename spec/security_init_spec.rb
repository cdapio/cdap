require 'spec_helper'

describe 'cdap::security_init' do
  context 'using default cdap version' do
    let(:chef_run) do
      ChefSpec::SoloRunner.new(platform: 'centos', version: 6.6) do |node|
        node.automatic['domain'] = 'example.com'
        node.override['cdap']['cdap_site']['ssl.enabled'] = true
      end.converge(described_recipe)
    end

    it 'executes create-security-server-ssl-keystore' do
      expect(chef_run).to run_execute('create-security-server-ssl-keystore')
    end
  end

  context 'with realmfile creation enabled' do
    let(:chef_run) do
      ChefSpec::SoloRunner.new(platform: 'centos', version: 6.6) do |node|
        node.automatic['domain'] = 'example.com'
        node.override['cdap']['security']['manage_realmfile'] = true
        node.override['cdap']['cdap_site']['security.authentication.handlerClassName'] = 'co.cask.cdap.security.server.BasicAuthenticationHandler'
        node.override['cdap']['cdap_site']['security.authentication.basic.realmfile'] = '/test/tmp/testrealm'
        node.override['cdap']['security']['realmfile']['username'] = 'testuser'
        node.override['cdap']['security']['realmfile']['password'] = 'testpass'
      end.converge(described_recipe)
    end

    it 'creates /test/tmp directory' do
      expect(chef_run).to create_directory('/test/tmp')
    end

    it 'creates /test/tmp/testrealm file' do
      expect(chef_run).to render_file('/test/tmp/testrealm').with_content('testuser: testpass')
    end
  end
end

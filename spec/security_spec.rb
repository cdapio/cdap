require 'spec_helper'

describe 'cdap::security' do
  context 'using default cdap version' do
    let(:chef_run) do
      ChefSpec::SoloRunner.new(platform: 'centos', version: 6.6) do |node|
        node.automatic['domain'] = 'example.com'
        stub_command(/update-alternatives --display /).and_return(false)
      end.converge(described_recipe)
    end
    pkg = 'cdap-auth-server'

    %W(
      /etc/init.d/#{pkg}
    ).each do |file|
      it "creates #{file} from template" do
        expect(chef_run).to create_template(file)
      end
    end

    it 'installs cdap-security package' do
      expect(chef_run).to install_package('cdap-security')
    end

    it 'does not execute create-security-server-ssl-keystore' do
      expect(chef_run).not_to run_execute('create-security-server-ssl-keystore')
    end

    it "creates #{pkg} service, but does not run it" do
      expect(chef_run).not_to start_service(pkg)
    end
  end

  context 'with ssl.enabled' do
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
        node.override['cdap']['security']['realmfile']['testuser'] = 'testpass'
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

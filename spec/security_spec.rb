require 'spec_helper'

describe 'cdap::security' do
  context 'using default cdap version' do
    let(:chef_run) do
      ChefSpec::SoloRunner.new(platform: 'centos', version: 6.6) do |node|
        node.automatic['domain'] = 'example.com'
        stub_command(/update-alternatives --display /).and_return(false)
      end.converge(described_recipe)
    end

    it 'installs cdap-security package' do
      expect(chef_run).to install_package('cdap-security')
    end

    it 'creates cdap-auth-server service, but does not run it' do
      expect(chef_run).not_to start_service('cdap-auth-server')
    end
  end
end

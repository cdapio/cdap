require 'spec_helper'

describe 'cdap::sdk' do
  context 'using default cdap version' do
    let(:chef_run) do
      ChefSpec::SoloRunner.new(platform: 'centos', version: 6.6) do |node|
        node.automatic['domain'] = 'example.com'
        node.default['cdap']['sdk']['install_dir'] = '/opt/cdap'
        stub_command('test -e /usr/bin/node').and_return(true)
      end.converge(described_recipe)
    end

    it 'does not create /usr/bin/node link' do
      expect(chef_run).not_to create_link('/usr/bin/node').with(
        to: '/usr/local/bin/node'
      )
    end

    it 'creates /opt/cdap directory' do
      expect(chef_run).to create_directory('/opt/cdap')
    end

    it 'creates cdap user' do
      expect(chef_run).to create_user('cdap')
    end

    it 'creates /etc/init.d/cdap-sdk from template' do
      expect(chef_run).to create_template('/etc/init.d/cdap-sdk')
    end

    it 'creates cdap-sdk service and starts it' do
      expect(chef_run).to start_service('cdap-sdk')
      expect(chef_run).to enable_service('cdap-sdk')
    end

    # There is no ark matcher, so we cannot test for it
    # ark[sdk]                           cdap/recipes/sdk.rb:48
  end
end

require 'spec_helper'

describe 'cdap::ui_init' do
  context 'using default cdap version' do
    let(:chef_run) do
      ChefSpec::SoloRunner.new(platform: 'centos', version: 6.6) do
        stub_command('test -e /usr/bin/node').and_return(true)
      end.converge(described_recipe)
    end

    it 'does not run execute[generate-ui-ssl-cert]' do
      expect(chef_run).not_to run_execute('generate-ui-ssl-cert')
    end
  end

  context 'with SSL' do
    let(:chef_run) do
      ChefSpec::SoloRunner.new(platform: 'centos', version: 6.6) do |node|
        node.override['cdap']['cdap_site']['ssl.enabled'] = true
        stub_command('test -e /usr/bin/node').and_return(true)
      end.converge(described_recipe)
    end

    it 'executes generate-ui-ssl-cert' do
      expect(chef_run).to run_execute('generate-ui-ssl-cert')
    end
  end
end

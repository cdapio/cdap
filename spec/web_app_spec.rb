require 'spec_helper'

describe 'cdap::web_app' do
  context 'on Centos 6.5 x86_64' do
    let(:chef_run) do
      ChefSpec::SoloRunner.new(platform: 'centos', version: 6.6) do |node|
        node.automatic['domain'] = 'example.com'
        node.default['hadoop']['hdfs_site']['dfs.datanode.max.transfer.threads'] = '4096'
        node.default['hadoop']['mapred_site']['mapreduce.framework.name'] = 'yarn'
        node.override['cdap']['version'] = '2.8.0-1'
        stub_command(/update-alternatives --display /).and_return(false)
        stub_command('test -e /usr/bin/node').and_return(true)
      end.converge(described_recipe)
    end
  end

  context 'using cdap <= 2.8.0' do
    let(:chef_run) do
      ChefSpec::SoloRunner.new(platform: 'centos', version: 6.6) do |node|
        node.automatic['domain'] = 'example.com'
        node.default['hadoop']['hdfs_site']['dfs.datanode.max.transfer.threads'] = '4096'
        node.default['hadoop']['mapred_site']['mapreduce.framework.name'] = 'yarn'
        node.override['cdap']['version'] = '2.8.0-1'
        stub_command('test -e /usr/bin/node').and_return(true)
        stub_command(/update-alternatives --display /).and_return(false)
      end.converge(described_recipe)
    end

    it 'installs cdap-web-app package' do
      expect(chef_run).to install_package('cdap-web-app')
    end

    it 'creates cdap-web-app service, but does not run it' do
      expect(chef_run).not_to start_service('cdap-web-app')
    end

    it 'does not create /usr/bin/node link' do
      expect(chef_run).not_to create_link('/usr/bin/node').with(
        to: '/usr/local/bin/node'
      )
    end
  end

  context 'using older nodejs cookbook' do
    let(:chef_run) do
      ChefSpec::SoloRunner.new(platform: 'centos', version: 6.6) do |node|
        node.automatic['domain'] = 'example.com'
        node.default['hadoop']['hdfs_site']['dfs.datanode.max.transfer.threads'] = '4096'
        node.default['hadoop']['mapred_site']['mapreduce.framework.name'] = 'yarn'
        node.override['cdap']['version'] = '2.8.0-1'
        stub_command(/update-alternatives --display /).and_return(false)
        stub_command('test -e /usr/bin/node').and_return(false)
      end.converge(described_recipe)
    end

    it 'creates /usr/bin/node link' do
      expect(chef_run).to create_link('/usr/bin/node').with(
        to: '/usr/local/bin/node'
      )
    end
  end
end

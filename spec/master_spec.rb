require 'spec_helper'

describe 'cdap::master' do
  context 'on Centos 6.5 x86_64' do
    let(:chef_run) do
      ChefSpec::SoloRunner.new(platform: 'centos', version: 6.5) do |node|
        node.automatic['domain'] = 'example.com'
        node.default['hadoop']['hdfs_site']['dfs.datanode.max.transfer.threads'] = '4096'
        node.default['hadoop']['mapred_site']['mapreduce.framework.name'] = 'yarn'
        stub_command(/update-alternatives --display /).and_return(false)
        stub_command(/test -L /).and_return(false)
      end.converge(described_recipe)
    end

    %w(cdap-hbase-compat-0.94 cdap-hbase-compat-0.96).each do |pkg|
      it "installs #{pkg} package" do
        expect(chef_run).to install_package(pkg)
      end
    end

    it 'installs cdap-master package' do
      expect(chef_run).to install_package('cdap-master')
    end

    it 'creates cdap-master service, but does not run it' do
      expect(chef_run).not_to start_service('cdap-master')
    end

    it 'does not install cdap-hbase-compat-0.98 package' do
      expect(chef_run).not_to install_package('cdap-hbase-compat-0.98')
    end
  end

  context 'using cdap 2.6.0' do
    let(:chef_run) do
      ChefSpec::SoloRunner.new(platform: 'centos', version: 6.5) do |node|
        node.automatic['domain'] = 'example.com'
        node.default['hadoop']['hdfs_site']['dfs.datanode.max.transfer.threads'] = '4096'
        node.default['hadoop']['mapred_site']['mapreduce.framework.name'] = 'yarn'
        node.override['cdap']['version'] = '2.6.0-1'
        stub_command(/update-alternatives --display /).and_return(false)
        stub_command(/test -L /).and_return(false)
      end.converge(described_recipe)
    end

    it 'installs cdap-hbase-compat-0.98 package' do
      expect(chef_run).to install_package('cdap-hbase-compat-0.98')
    end
  end
end

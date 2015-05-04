require 'spec_helper'

describe 'cdap::kafka' do
  context 'using default cdap version' do
    let(:chef_run) do
      ChefSpec::SoloRunner.new(platform: 'centos', version: 6.6) do |node|
        node.automatic['domain'] = 'example.com'
        node.default['hadoop']['hdfs_site']['dfs.datanode.max.transfer.threads'] = '4096'
        node.default['hadoop']['mapred_site']['mapreduce.framework.name'] = 'yarn'
        stub_command(/update-alternatives --display /).and_return(false)
        stub_command(/test -L /).and_return(false)
      end.converge(described_recipe)
    end

    it 'installs cdap-kafka package' do
      expect(chef_run).to install_package('cdap-kafka')
    end

    it 'creates /data/cdap/kafka-logs directory' do
      expect(chef_run).to create_directory('/data/cdap/kafka-logs')
    end

    it 'creates cdap-kafka-server service, but does not run it' do
      expect(chef_run).not_to start_service('cdap-kafka-server')
    end
  end
end

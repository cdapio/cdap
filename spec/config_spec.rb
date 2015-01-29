require 'spec_helper'

describe 'cdap::config' do
  context 'on Centos 6.5 x86_64' do
    let(:chef_run) do
      ChefSpec::SoloRunner.new(platform: 'centos', version: 6.5) do |node|
        node.automatic['domain'] = 'example.com'
        node.default['hadoop']['hdfs_site']['dfs.datanode.max.transfer.threads'] = '4096'
        node.default['hadoop']['mapred_site']['mapreduce.framework.name'] = 'yarn'
        node.default['cdap']['cdap_site']['hdfs.user'] = 'cdap'
        stub_command(/update-alternatives --display /).and_return(false)
      end.converge(described_recipe)
    end

    it 'creates /etc/cdap/conf.chef directory' do
      expect(chef_run).to create_directory('/etc/cdap/conf.chef')
    end

    it 'runs execute[copy logback.xml from conf.dist]' do
      expect(chef_run).to run_execute('copy logback.xml from conf.dist')
    end

    it 'runs execute[update cdap-conf alternatives]' do
      expect(chef_run).to run_execute('update cdap-conf alternatives')
    end
  end

  context 'using cdap 2.7.0' do
    let(:chef_run) do
      ChefSpec::SoloRunner.new(platform: 'centos', version: 6.5) do |node|
        node.automatic['domain'] = 'example.com'
        node.default['hadoop']['hdfs_site']['dfs.datanode.max.transfer.threads'] = '4096'
        node.default['hadoop']['mapred_site']['mapreduce.framework.name'] = 'yarn'
        node.override['cdap']['version'] = '2.7.0-1'
        node.default['cdap']['cdap_env']['log_dir'] = '/test/logs/cdap'
        stub_command(/update-alternatives --display /).and_return(false)
        stub_command(/test -L /).and_return(false)
      end.converge(described_recipe)
    end

    it 'creates /etc/cdap/conf.chef/cdap-env.sh' do
      expect(chef_run).to create_template('/etc/cdap/conf.chef/cdap-env.sh')
    end
  end
end

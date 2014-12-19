require 'spec_helper'

describe 'cdap::config' do
  context 'on Centos 6.5 x86_64' do
    let(:chef_run) do
      ChefSpec::SoloRunner.new(platform: 'centos', version: 6.5) do |node|
        node.automatic['domain'] = 'example.com'
        node.default['hadoop']['hdfs_site']['dfs.datanode.max.transfer.threads'] = '4096'
        node.default['hadoop']['mapred_site']['mapreduce.framework.name'] = 'yarn'
        stub_command('update-alternatives --display cdap-conf | grep best | awk \'{print $5}\' | grep /etc/cdap/conf.chef').and_return(false)
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
end

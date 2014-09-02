require 'spec_helper'

describe 'cdap::default' do
  context 'on Centos 6.4 x86_64' do
    let(:chef_run) do
      ChefSpec::Runner.new(platform: 'centos', version: 6.4) do |node|
        node.automatic['domain'] = 'example.com'
        node.default['cdap']['repo']['url'] = 'https://USER:PASS@cdap.repo/path/to/repo'
        node.default['hadoop']['hdfs_site']['dfs.datanode.max.transfer.threads'] = '4096'
        node.default['hadoop']['mapred_site']['mapreduce.framework.name'] = 'yarn'
        stub_command('update-alternatives --display cdap-conf | grep best | awk \'{print $5}\' | grep /etc/cdap/conf.chef').and_return(false)
        stub_command('update-alternatives --display hadoop-conf | grep best | awk \'{print $5}\' | grep /etc/hadoop/conf.chef').and_return(false)
        stub_command('update-alternatives --display hbase-conf | grep best | awk \'{print $5}\' | grep /etc/hbase/conf.chef').and_return(false)
        stub_command('update-alternatives --display hive-conf | grep best | awk \'{print $5}\' | grep /etc/hive/conf.chef').and_return(false)
      end.converge(described_recipe)
    end

    it 'installs cdap package' do
      expect(chef_run).to install_package('cdap')
    end

    it 'creates /etc/profile.d/cdap_home.sh file' do
      expect(chef_run).to create_file('/etc/profile.d/cdap_home.sh')
    end

    it 'creates /etc/cdap/conf.chef/cdap-site.xml template' do
      expect(chef_run).to create_template('/etc/cdap/conf.chef/cdap-site.xml')
    end

  end
end

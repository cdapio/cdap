require 'spec_helper'

describe 'cdap::default' do
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

    it 'installs cdap package' do
      expect(chef_run).to install_package('cdap')
    end

    it 'creates /etc/profile.d/cdap_home.sh file' do
      expect(chef_run).to create_file('/etc/profile.d/cdap_home.sh')
    end

    it 'creates /etc/cdap/conf.chef/cdap-site.xml template' do
      expect(chef_run).to create_template('/etc/cdap/conf.chef/cdap-site.xml')
    end

    it 'logs JAVA_HOME' do
      expect(chef_run).to write_log('JAVA_HOME = /usr/lib/jvm/java')
    end
  end

  context 'using cdap 2.7.0' do
    let(:chef_run) do
      ChefSpec::SoloRunner.new(platform: 'centos', version: 6.5) do |node|
        node.automatic['domain'] = 'example.com'
        node.default['hadoop']['hdfs_site']['dfs.datanode.max.transfer.threads'] = '4096'
        node.default['hadoop']['mapred_site']['mapreduce.framework.name'] = 'yarn'
        node.override['cdap']['version'] = '2.7.0-1'
        node.override['cdap']['cdap_env']['log_dir'] = '/test/logs/cdap'
        stub_command(/update-alternatives --display /).and_return(false)
        stub_command(/test -L /).and_return(false)
      end.converge(described_recipe)
    end

    it 'creates /test/logs/cdap directory' do
      expect(chef_run).to create_directory('/test/logs/cdap')
    end

    it 'deletes /var/log/cdap' do
      expect(chef_run).to delete_directory('/var/log/cdap')
    end

    it 'creates /var/log/cdap symlink' do
      link = chef_run.link('/var/log/cdap')
      expect(link).to link_to('/test/logs/cdap')
    end
  end
end

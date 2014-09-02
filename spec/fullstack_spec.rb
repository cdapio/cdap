require 'spec_helper'

describe 'cdap::fullstack' do
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

    %w(cdap-gateway cdap-kafka cdap-master cdap-web-app)
      .each do |pkg|
      it "installs #{pkg} package" do
        expect(chef_run).to install_package(pkg)
      end
    end

    %w(/etc/cdap/conf.chef /data/cdap/kafka-logs).each do |dir|
      it "creates #{dir} directory" do
        expect(chef_run).to create_directory(dir)
      end
    end

    %w(
      cdap-gateway
      cdap-router
      cdap-kafka-server
      cdap-master
      cdap-web-app
    ).each do |svc|
      it "creates #{svc} service, but does not run it" do
        expect(chef_run).not_to start_service(svc)
      end
    end
  end
end

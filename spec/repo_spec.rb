require 'spec_helper'

describe 'cdap::repo' do
  context 'on Centos 6.6 x86_64 using default version' do
    let(:chef_run) do
      ChefSpec::SoloRunner.new(platform: 'centos', version: 6.6).converge(described_recipe)
    end

    it 'adds cdap-4.0 yum repository' do
      expect(chef_run).to add_yum_repository('cdap-4.0')
    end

    it 'deletes cask yum repository' do
      expect(chef_run).to delete_file('/etc/yum.repos.d/cask.repo')
    end
  end

  context 'using 2.8.2' do
    let(:chef_run) do
      ChefSpec::SoloRunner.new(platform: 'centos', version: 6.6) do |node|
        node.override['cdap']['version'] = '2.8.2-1'
      end.converge(described_recipe)
    end

    it 'adds cdap-2.8 yum repository' do
      expect(chef_run).to add_yum_repository('cdap-2.8')
    end
  end

  context 'using 2.5.2' do
    let(:chef_run) do
      ChefSpec::SoloRunner.new(platform: 'centos', version: 6.6) do |node|
        node.override['cdap']['version'] = '2.5.2-1'
      end.converge(described_recipe)
    end

    it 'adds cdap-2.5 yum repository' do
      expect(chef_run).to add_yum_repository('cdap-2.5')
    end
  end

  context 'on Ubuntu 12.04 using default version' do
    let(:chef_run) do
      ChefSpec::SoloRunner.new(platform: 'ubuntu', version: 12.04).converge(described_recipe)
    end

    it 'adds cdap-4.0 apt repository' do
      expect(chef_run).to add_apt_repository('cdap-4.0')
    end

    it 'deletes cask apt repository' do
      expect(chef_run).to delete_file('/etc/apt/sources.list.d/cask.list')
    end
  end
end

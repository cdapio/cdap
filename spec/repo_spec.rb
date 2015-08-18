require 'spec_helper'

describe 'cdap::repo' do
  context 'on Centos 6.6 x86_64' do
    let(:chef_run) do
      ChefSpec::SoloRunner.new(platform: 'centos', version: 6.6).converge(described_recipe)
    end

    it 'adds cdap-3.1 yum repository' do
      expect(chef_run).to add_yum_repository('cdap-3.1')
    end

    it 'deletes cask yum repository' do
      expect(chef_run).to delete_file('/etc/yum.repos.d/cask.repo')
    end
  end

  context 'on Ubuntu 12.04' do
    let(:chef_run) do
      ChefSpec::SoloRunner.new(platform: 'ubuntu', version: 12.04).converge(described_recipe)
    end

    it 'adds cdap-3.1 apt repository' do
      expect(chef_run).to add_apt_repository('cdap-3.1')
    end

    it 'deletes cask apt repository' do
      expect(chef_run).to delete_file('/etc/apt/sources.list.d/cask.list')
    end
  end
end

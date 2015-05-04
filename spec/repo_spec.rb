require 'spec_helper'

describe 'cdap::repo' do
  context 'on Centos 6.6 x86_64' do
    let(:chef_run) do
      ChefSpec::SoloRunner.new(platform: 'centos', version: 6.6).converge(described_recipe)
    end

    it 'adds cask yum repository' do
      expect(chef_run).to add_yum_repository('cask')
    end
  end

  context 'on Ubuntu 12.04' do
    let(:chef_run) do
      ChefSpec::SoloRunner.new(platform: 'ubuntu', version: 12.04).converge(described_recipe)
    end

    it 'adds cask apt repository' do
      expect(chef_run).to add_apt_repository('cask')
    end
  end
end

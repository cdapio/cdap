require File.expand_path('../support/helpers', __FILE__)

describe 'cdap::repo' do
  include Helpers::CDAP

  # Example spec tests can be found at http://git.io/Fahwsw

  describe 'rhel' do
    it 'Verifies the repository file is present and correct' do
      skip unless node[:platform_family] == 'rhel'
      file('/etc/yum.repos.d/cask.repo').must_exist.with(:owner, 'root').and(:group, 'root').and(:mode, '0644')
      file('/etc/yum.repos.d/cask.repo').must_include "baseurl=#{node['cdap']['repo']['url']}"
      file('/etc/yum.repos.d/cask.repo').must_include 'enabled=1'
    end
  end

  describe 'debian' do
    it 'Verifies the repository file is present and correct' do
      skip unless node[:platform_family] == 'debian'
      file('/etc/apt/sources.list.d/cask.list').must_exist.with(:owner, 'root').and(:group, 'root').and(:mode, '0644')
      file('/etc/apt/sources.list.d/cask.list').must_include "#{node['cdap']['repo']['url']}"
    end
  end
end

require File.expand_path('../support/helpers', __FILE__)

describe 'cdap::gateway' do
  include Helpers::CDAP

  it 'verifies gateway installation' do
    package('cdap-gateway').must_be_installed
    file('/opt/cdap/gateway/VERSION').must_exist.with(:owner, 'cdap').and(:group, 'cdap')
    file('/etc/init.d/cdap-gateway').must_exist.with(:owner, 'root').and(:group, 'root').and(:mode, '0755')
    file('/etc/init.d/cdap-router').must_exist.with(:owner, 'root').and(:group, 'root').and(:mode, '0755')
  end
end

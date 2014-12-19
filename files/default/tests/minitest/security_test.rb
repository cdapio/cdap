require File.expand_path('../support/helpers', __FILE__)

describe 'cdap::security' do
  include Helpers::CDAP

  it 'verifies security installation' do
    package('cdap-security').must_be_installed
    file('/opt/cdap/security/VERSION').must_exist.with(:owner, 'cdap').and(:group, 'cdap')
    file('/etc/init.d/cdap-auth-server').must_exist.with(:owner, 'root').and(:group, 'root').and(:mode, '0755')
  end
end

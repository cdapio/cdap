require File.expand_path('../support/helpers', __FILE__)

describe 'cdap::master' do
  include Helpers::CDAP

  it 'verifies master installation' do
    package('cdap-master').must_be_installed
    file('/opt/cdap/master/VERSION').must_exist.with(:owner, 'cdap').and(:group, 'cdap')
    file('/etc/init.d/cdap-master').must_exist.with(:owner, 'root').and(:group, 'root').and(:mode, '0755')
  end
end

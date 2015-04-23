require File.expand_path('../support/helpers', __FILE__)

describe 'cdap::ui' do
  include Helpers::CDAP

  it 'verifies ui installation' do
    package('cdap-ui').must_be_installed
    file('/opt/cdap/ui/VERSION').must_exist.with(:owner, 'cdap').and(:group, 'cdap')
    file('/etc/init.d/cdap-ui').must_exist.with(:owner, 'root').and(:group, 'root').and(:mode, '0755')
  end
end

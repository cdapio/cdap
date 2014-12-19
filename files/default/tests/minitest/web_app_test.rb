require File.expand_path('../support/helpers', __FILE__)

describe 'cdap::web_app' do
  include Helpers::CDAP

  it 'verifies web-app installation' do
    package('cdap-web-app').must_be_installed
    file('/opt/cdap/web-app/VERSION').must_exist.with(:owner, 'cdap').and(:group, 'cdap')
    file('/etc/init.d/cdap-web-app').must_exist.with(:owner, 'root').and(:group, 'root').and(:mode, '0755')
  end
end

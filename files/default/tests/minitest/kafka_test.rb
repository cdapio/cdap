require File.expand_path('../support/helpers', __FILE__)

describe 'cdap::kafka' do
  include Helpers::CDAP

  it 'verifies kafka installation' do
    package('cdap-kafka').must_be_installed
    file('/opt/cdap/kafka/VERSION').must_exist.with(:owner, 'cdap').and(:group, 'cdap')
    file('/etc/init.d/cdap-kafka-server').must_exist.with(:owner, 'root').and(:group, 'root').and(:mode, '0755')
  end
end

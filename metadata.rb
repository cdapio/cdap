name             'cdap'
maintainer       'Cask'
maintainer_email 'ops@cask.co'
license          'All rights reserved'
description      'Installs/Configures Cask DAP (CDAP)'
long_description IO.read(File.join(File.dirname(__FILE__), 'README.md'))
version          '0.1.6'

%w(apt hadoop java nodejs ntp yum yum-epel).each do |cb|
  depends cb
end

depends 'krb5_utils'

%w(amazon centos debian redhat scientific ubuntu).each do |os|
  supports os
end

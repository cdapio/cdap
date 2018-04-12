#!/bin/bash
#
# Copyright © 2015-2017 Cask Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

#
# Download cookbooks from Chef Supermarket
#

die() { echo $*; exit 1; }

export GIT_MERGE_AUTOEDIT=no

# Grab cookbooks using knife
# Due to https://issues.cask.co/browse/CDAP-13308, we can no longer use knife cookbook site install
# for cb in cdap idea maven openssh; do
#   knife cookbook site install $cb || die "Cannot fetch cookbook $cb"
# done

# Instead we must manually download and extract known good versions
knife cookbook site download --force ambari 0.4.0 || die "Cannot download cookbook ambari"
knife cookbook site download --force apt 6.1.4 || die "Cannot download cookbook apt"
knife cookbook site download --force ark 3.1.0 || die "Cannot download cookbook ark"
knife cookbook site download --force build-essential 8.1.1 || die "Cannot download cookbook build-essential"
knife cookbook site download --force cdap 3.3.3 || die "Cannot download cookbook cdap"
knife cookbook site download --force dpkg_autostart 0.2.0 || die "Cannot download cookbook dpkg_autostart"
knife cookbook site download --force hadoop 2.13.0 || die "Cannot download cookbook hadoop"
knife cookbook site download --force homebrew 5.0.4 || die "Cannot download cookbook homebrew"
knife cookbook site download --force idea 0.6.0 || die "Cannot download cookbook idea"
knife cookbook site download --force iptables 4.3.4 || die "Cannot download cookbook iptables"
knife cookbook site download --force java 1.50.0 || die "Cannot download cookbook java"
knife cookbook site download --force krb5 2.2.1 || die "Cannot download cookbook krb5"
knife cookbook site download --force maven 5.1.0 || die "Cannot download cookbook maven"
knife cookbook site download --force mingw 2.0.2 || die "Cannot download cookbook mingw"
knife cookbook site download --force nodejs 5.0.0 || die "Cannot download cookbook nodejs"
knife cookbook site download --force ntp 3.5.6 || die "Cannot download cookbook ntp"
knife cookbook site download --force ohai 5.2.2 || die "Cannot download cookbook ohai"
knife cookbook site download --force openssh 2.6.3 || die "Cannot download cookbook openssh"
knife cookbook site download --force selinux 2.1.0 || die "Cannot download cookbook selinux"
knife cookbook site download --force seven_zip 2.0.2 || die "Cannot download cookbook seven_zip"
knife cookbook site download --force sysctl 1.0.3 || die "Cannot download cookbook sysctl"
knife cookbook site download --force ulimit 1.0.0 || die "Cannot download cookbook ulimit"
knife cookbook site download --force windows 4.1.4 || die "Cannot download cookbook windows"
knife cookbook site download --force yum 5.1.0 || die "Cannot download cookbook yum"

# extract to /var/chef/cookbooks
for cb in `ls *.tar.gz`; do
  tar xf $cb -C /var/chef/cookbooks
done

# Do not change HOME for cdap user
sed -i '/ home /d' /var/chef/cookbooks/cdap/recipes/sdk.rb

exit 0

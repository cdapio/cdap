# cdap cookbook

[![Cookbook Version](http://img.shields.io/cookbook/v/cdap.svg)](https://supermarket.getchef.com/cookbooks/cdap)
[![Build Status](http://img.shields.io/travis/caskdata/cdap_cookbook.svg)](http://travis-ci.org/caskdata/cdap_cookbook)

# Requirements

* Oracle Java JDK 6+ (provided by `java` cookbook)
* Hadoop 2.0+ HDFS, YARN, ZooKeeper, and HBase (provided by `hadoop` cookbook)
* Node.JS 0.8.16+ (provided by `nodejs` cookbook, tested with `1.3.0` and `2.1.0`)

# Usage

# Attributes

* `['cdap']['conf_dir']` - The directory used inside `/etc/cdap` and used via the alternatives system. Default `conf.chef`
* `['cdap']['repo']['apt_repo_url']` - Specifies URL for fetching packages from APT
* `['cdap']['repo']['apt_components']` - Repository components to use for APT repositories
* `['cdap']['repo']['yum_repo_url']` - Specifies URL for fetching packages from YUM

# Recipes

* `config` - Configures all services
* `default` - Installs `cdap` base package and performs configuration of `cdap-site.xml`
* `fullstack` - Installs all packages and services on a single node
* `gateway` - Installs the `cdap-gateway` package and `cdap-gateway` and `cdap-router` services
* `init` - Sets up HDFS
* `kafka` - Installs the `cdap-kafka` package and `cdap-kafka-server` service
* `master` - Installs the `cdap-master` package and service
* `repo` - Sets up package manager repositories for cdap packages
* `security` - Installs the `cdap-security` package and `cdap-auth-server` service
* `web_app` - Installs the `cdap-web-app` package and service

# Author

Author:: Cask Data, Inc. (<ops@cask.co>)

# Testing

This cookbook requires the `vagrant-omnibus` and `vagrant-berkshelf` Vagrant plugins to be installed.

# License

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this software except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

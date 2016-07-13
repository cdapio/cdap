# cdap cookbook

[![Cookbook Version](http://img.shields.io/cookbook/v/cdap.svg)](https://supermarket.chef.io/cookbooks/cdap)
[![Apache License 2.0](http://img.shields.io/badge/license-apache%202.0-green.svg)](http://opensource.org/licenses/Apache-2.0)
[![Build Status](http://img.shields.io/travis/caskdata/cdap_cookbook.svg)](http://travis-ci.org/caskdata/cdap_cookbook)

# Requirements

* Oracle Java JDK 7+ with JCE (provided by `java` cookbook)
* Hadoop 2.0+ HDFS, YARN, ZooKeeper, Hive, and HBase (provided by `hadoop` cookbook)

# Usage

## Distributed

The simplest usage is to install a complete CDAP stack on a single machine, using the `cdap::fullstack` recipe. Directories
in HDFS are created using the `cdap::init` recipe. The CDAP Upgrade Tool can be run after upgrading CDAP by using the
`cdap::upgrade` recipe.

## Standalone/SDK

Use the `cdap::sdk` recipe.

# Attributes

* `['cdap']['conf_dir']` - The directory used inside `/etc/cdap` and used via the alternatives system. Default `conf.chef`
* `['cdap']['repo']['apt_repo_url']` - Specifies URL for fetching packages from APT
* `['cdap']['repo']['apt_components']` - Repository components to use for APT repositories
* `['cdap']['repo']['yum_repo_url']` - Specifies URL for fetching packages from YUM
* `['cdap']['version']` - CDAP package version to install, must exist in the given repository

# Recipes

* `cli` - Installs `cdap-cli` package
* `config` - Configures all services
* `default` - Installs `cdap` base package and performs configuration of `cdap-site.xml`
* `fullstack` - Installs all packages and services on a single node
* `gateway` - Installs the `cdap-gateway` package and `cdap-gateway` and `cdap-router` services
* `init` - Sets up HDFS, run on Master node
* `kafka` - Installs the `cdap-kafka` package and `cdap-kafka-server` service
* `master` - Installs the `cdap-master` package and service
* `prerequisites` - Installs dependencies such as `hadoop`, `hbase`, `hive`, and `ntpd`
* `repo` - Sets up package manager repositories for cdap packages
* `sdk` - Installs the CDAP SDK and sets up a `cdap-sdk` service
* `security` - Installs the `cdap-security` package and `cdap-auth-server` service
* `ui` - Installs the `cdap-ui` package and service, replaces `web_app`
* `upgrade` - Executes the CDAP Upgrade Tool, run on Master node
* `web_app` - Installs the `cdap-web-app` package and service

# Author

Author:: Cask Data, Inc. (<ops@cask.co>)

# License

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this software except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

# cdap cookbook

# Requirements

This cookbook assumes that you have several things completed prior to use:
* a working Oracle Java JRE/JDK installed (tested using java cookbook 1.21.2)
* a working Hadoop HDFS and YARN, ZooKeeper, and HBase (tested using hadoop cookbook 1.0.0)
* For `web_app`, node.JS must be installed and `node` must be in the PATH (tested using nodejs cookbook 1.3.0)

# Usage

# Attributes

* `['cdap']['conf_dir']` - The directory used inside `/etc/cdap` and used via the alternatives system. Default `conf.chef`
* `['cdap']['repo']['url']` - Specifies URL for fetching packages
* `['cdap']['repo']['components']` - Repository components to use for APT repositories

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

Author:: Cask (<ops@cask.co>)

# Testing

This cookbook requires the `vagrant-omnibus` and `vagrant-berkshelf` Vagrant plugins to be installed.

# License

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this software except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

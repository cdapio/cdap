
# Cask Data Application Platform

![CDAP Logo](/docs/developer-guide/source/_images/CDAP.png)

**A Platform for Data Applications**

The Cask Data Application Platform (CDAP) is an application server providing such a
platform for the development, deployment and management of data applications and the
management of data.

Out-of-the-box, its features include transaction management, dataset abstractions, QoS,
performance, scalability, security, metrics and log collection, service discovery and a 
web-based management dashboard.

CDAP provides abstractions over Hadoop that do not require understanding the implementation or the 
complexity of Apache Hadoop&trade;, HBase or Zookeeper. It provides independence of Hadoop versions, 
and runs on any distribution of Hadoop.

CDAP's container model allows for the integration of different processing paradigms with these
features. It provides a common environment, the abstraction of a unified API, the lifecycle management
and a programming model for data applications and their data. You can package, deploy and 
manage applications as a single unit.

You can run applications ranging from simple MapReduce Jobs through complete ETL (extract, transform, and load) 
pipelines all the way up to complex, enterprise-scale data-intensive applications. 
Developers can build and test their applications end-to-end in a full-stack, single-node
installation. CDAP can be run either standalone, deployed within the Enterprise or hosted in the Cloud.

For more information, see our collection of 
[Developer Guides and other documentation](http://cask.co/docs/cdap/current/en/index.html).

## Is It Building?

Build                                                                    | Status / Version
-------------------------------------------------------------------------|-----------------
[Travis Continuous Integration Build](https://travis-ci.org/caskco/cdap) | ![travis](https://travis-ci.org/caskco/cdap.svg?branch=develop)
[GitHub Version](ttps://github.com/caskco/cdap/releases/latest)          | ![github](http://img.shields.io/github/release/caskco/cdap.svg)


## Getting Started

You can get started with CDAP by building directly from the latest source code::

```
  git clone https://github.com/cask/cdap.git
  cd cdap
  mvn clean package
```

After the build completes, you will have a distribution of the CDAP Single-node SDK under the
`cdap-distribution/target/` directory.  

Take the `cdap-<version>.tar.gz` file and unzip it into a suitable location.

### Quick Start

Visit our web site for a [Quick Start](http://cask.co/docs/cdap/current/en/quickstart.html)
that will guide you through installing CDAP, running an example that counts HTTP status codes and then
modifying the example’s Java code to include counting client IP addresses.  


## Where to Go Next

Now that you've had a look at the CDAP SDK, take a look at:

- Examples, located in the `/examples` directory of the CDAP SDK;
- [Selected Examples](http://cask.co/docs/cdap/current/en/examples.html) 
  (demonstrating basic features of the CDAP) are located on-line; and
- Developer Guides, located in the source distribution in `/docs/developer-guide/source`
  or [online](http://cask.co/docs/cdap/current/en/index.html).


## How to Contribute

Interested in helping to improve CDAP? We welcome all contributions, whether in filing detailed
bug reports, submitting pull requests for code changes and improvements, or by asking questions and
assisting others on the mailing list.

### Bug Reports & Feature Requests

Bugs and tasks are tracked in a public JIRA issue tracker. Details on access will be forthcoming.

### Pull Requests

We have a simple pull-based development model with a consensus-building phase, similar to Apache's
voting process. If you’d like to help make CDAP better by adding new features, enhancing existing
features, or fixing bugs, here's how to do it:

1. If you are planning a large change or contribution, discuss your plans on the `cask-cdap-dev`
   mailing list first.  This will help us understand your needs and best guide your solution in a
   way that fits the project.
2. Fork CDAP into your own GitHub repository.
3. Create a topic branch with an appropriate name.
4. Work on the code to your heart's content.
5. Once you’re satisfied, create a pull request from your GitHub repo (it’s helpful if you fill in
   all of the description fields).
6. After we review and accept your request, we’ll commit your code to the cask/cdap
   repository.

Thanks for helping to improve CDAP!

### Mailing List

CDAP User Group and Development Discussions: 
[cdap-dev@googlegroups.com](https://groups.google.com/d/forum/cdap-dev)

### IRC Channel

CDAP IRC Channel #cask-cdap on irc.freenode.net


## License and Trademarks

© Copyright 2014 Cask, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the 
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
either express or implied. See the License for the specific language governing permissions 
and limitations under the License.

Cask is a trademark of Cask, Inc. All rights reserved.

Apache, Apache HBase, and HBase are trademarks of The Apache Software Foundation. Used with
permission. No endorsement by The Apache Software Foundation is implied by the use of these marks.

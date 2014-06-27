.. :author: Continuuity, Inc.
   :version: 2.3.0

=========================
Continuuity Reactor 2.3.0
=========================

Continuuity Reactor Level 1 Dependencies
--------------------------------------------

.. rst2pdf: PageBreak
.. rst2pdf: .. contents::

.. rst2pdf: build ../../../developer-guide/licenses-pdf/
.. rst2pdf: config ../../../developer-guide/source/_templates/pdf-config
.. rst2pdf: stylesheets ../../../developer-guide/source/_templates/pdf-stylesheet

.. csv-table:: **Continuuity Reactor Level 1 Dependencies**
   :header: "Package","Artifact","License","License URL"
   :widths: 20, 20, 20, 40

   "org.slf4j","slf4j-api","The MIT License","http://slf4j.org/license.html
   http://www.slf4j.org/faq.html#license"
   "ch.qos.logback","logback-core","Dual License - LGPL & EPL","http://logback.qos.ch/faq.html#why_lgpl"
   "ch.qos.logback","logback-classic","Dual License - LGPL & EPL","http://logback.qos.ch/faq.html#why_lgpl"
   "org.mockito","mockito-core","The MIT License","http://code.google.com/p/mockito/wiki/License"
   "junit","junit","Common Public License Version 1.0","http://www.opensource.org/licenses/cpl1.0.txt"
   "it.unimi.dsi","fastutil","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "com.google.guava","guava","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "com.google.code.gson","gson","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "com.google.inject","guice","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "com.google.inject.extensions","guice-assistedinject","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "com.google.inject.extensions","guice-multibindings","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "org.apache.thrift","libthrift","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "org.ow2.asm","asm-all","BSD License","http://opensource.org/licenses/BSD-3-Clause"
   "org.apache.avro","avro","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "org.apache.avro","avro-ipc","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "org.apache.zookeeper","zookeeper","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "org.apache.flume","flume-ng-sdk","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "org.apache.flume","flume-ng-core","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "com.typesafe.akka","akka-actor","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "com.ning","async-http-client","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "commons-cli","commons-cli","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "commons-logging","commons-logging","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "org.apache.commons","commons-math","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "org.apache.commons","commons-io","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "commons-beanutils","commons-beanutils-core","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "org.apache.httpcomponents","httpclient","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "org.apache.httpcomponents","httpcore","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "org.apache.shiro","shiro-core","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "org.apache.shiro","shiro-guice","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "org.apache.hadoop","hadoop-common","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "org.apache.hadoop","hadoop-annotations","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "org.apache.hadoop","hadoop-hdfs","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "org.apache.hadoop","hadoop-yarn-common","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "org.apache.hadoop","hadoop-yarn-api","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "org.apache.hadoop","hadoop-yarn-client","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "org.apache.hadoop","hadoop-minicluster","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "org.apache.hadoop","hadoop-mapreduce-client-core","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "org.apache.hadoop","hadoop-auth","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "org.apache.hadoop","hadoop-yarn","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "org.apache.hbase","hbase","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "org.apache.hbase","hbase","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "org.apache.hbase","hbase-client","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "org.apache.hbase","hbase-common","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "org.apache.hbase","hbase-protocol","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "org.apache.hbase","hbase-server","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "org.apache.hbase","hbase-testing-util","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "org.mortbay.jetty","jetty","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "jetty","jetty-management","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "com.yammer.metrics","metrics-core","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "io.netty","netty","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "org.apache.mina","mina-core","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "org.apache.mina","mina-integration-beans","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "org.apache.mina","mina-integration-ognl","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "org.apache.mina","mina-integration-jmx","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "javax.ws.rs","jsr311-api","CDDL License","http://www.opensource.org/licenses/cddl1.php"
   "com.google.code.findbugs","jsr305","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "cglib","cglib-nodep","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "org.iq80.leveldb","leveldb","BSD 3.0","http://opensource.org/licenses/BSD-3-Clause"
   "org.apache.twill","twill-api","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "org.apache.twill","twill-common","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "org.apache.twill","twill-core","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "org.apache.twill","twill-discovery-api","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "org.apache.twill","twill-discovery-core","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "org.apache.twill","twill-yarn","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "org.apache.twill","twill-zookeeper","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "org.quartz-scheduler","quartz","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "org.quartz-scheduler","quartz-jobs","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"
   "com.continuuity","http","Apache License, Version 2.0","http://www.apache.org/licenses/LICENSE-2.0.html"

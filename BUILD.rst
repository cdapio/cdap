=====================================
Cask Data Application Platform - CDAP
=====================================

Prerequisites
=============

- Java 7+ SDK
- Maven 3.1+
- Git

CDAP Sandbox and Distributed CDAP
=================================

**Building CDAP with Maven**

- Clean all modules::

    mvn clean

- Run all tests, fail at the end::

    MAVEN_OPTS="-Xmx1024m -XX:MaxPermSize=128m" mvn test -fae

- Run tests skipping repeated compat module tests::
    MAVEN_OPTS="-Xmx1024m -XX:MaxPermSize=128m" mvn test -Pskip-hbase-compat-tests -fae

- Build all modules::

    mvn clean package

- Run checkstyle, skipping tests::

    mvn clean package -DskipTests

- Build a particular module::

    mvn clean package -pl [module] -am

- Run selected test::

    MAVEN_OPTS="-Xmx1024m -XX:MaxPermSize=128m" mvn -Dtest=TestClass,TestMore*Class,TestClassMethod#methodName \
    -DfailIfNoTests=false test

- Run App-Template tests::

    MAVEN_OPTS="-Xmx1024m -XX:MaxPermSize=128m" mvn test -fae -am -amd -P templates -pl cdap-app-templates/cdap-etl

  See `Surefire doc <http://maven.apache.org/surefire/maven-surefire-plugin/examples/single-test.html>`__ for details

- Build all examples::

    MAVEN_OPTS="-Xmx512m -XX:MaxPermSize=128m" mvn clean package -DskipTests -pl cdap-examples -am -amd -P examples

- Build CDAP Sandbox distribution ZIP::

    MAVEN_OPTS="-Xmx1024m -XX:MaxPermSize=128m" mvn clean package \
    -pl cdap-standalone,cdap-app-templates/cdap-etl,cdap-app-templates/cdap-data-quality,cdap-examples \
    -am -amd -DskipTests -P examples,templates,dist,release,unit-tests

- Build CDAP Sandbox distribution ZIP with additional system artifacts::

    MAVEN_OPTS="-Xmx1024m -XX:MaxPermSize=128m" mvn clean package \
    -pl cdap-standalone,cdap-app-templates/cdap-etl,cdap-app-templates/cdap-data-quality,cdap-examples \
    -am -amd -DskipTests -P examples,templates,dist,release,unit-tests \
    -Dadditional.artifacts.dir=</path/to/additional/artifacts>

  This will copy any .jar and .json files in any 'target' directories under the specified path to the artifacts directory.

- Build the limited set of Javadocs used in distribution ZIP::

    mvn clean package javadoc:javadoc -pl cdap-api -am -DskipTests -P release

- Build the limited set of Javadocs, including the ETL Application Templates, included in the CDAP documentation::

    MAVEN_OPTS="-Xmx1024m -XX:MaxPermSize=128m" mvn clean install -P examples,templates,release -DskipTests \
    -Dgpg.skip=true && mvn clean site -DskipTests -P templates -DisOffline=false

- Build the complete set of Javadocs, for all modules and examples::

    MAVEN_OPTS="-Xmx1024m -XX:MaxPermSize=128m" mvn clean install -P examples,templates,release -DskipTests \
    -Dgpg.skip=true && mvn clean javadoc:aggregate -DskipTests -P examples,templates -DisOffline=false

- Build distributions (rpm, deb, tgz)::

    MAVEN_OPTS="-Xmx1024m -XX:MaxPermSize=128m" mvn clean package -DskipTests \
    -P examples,templates,dist,release,rpm-prepare,rpm,deb-prepare,deb,tgz,unit-tests

- Build Cloudera Manager parcel::

    MAVEN_OPTS="-Xmx1024m -XX:MaxPermSize=128m" mvn clean package -DskipTests \
    -P templates,dist,tgz && ./cdap-distributions/bin/build_parcel.sh

- Show dependency tree::

    mvn package dependency:tree -DskipTests

- Show dependency tree for a particular module::

    mvn package dependency:tree -DskipTests -pl [module] -am

- Show test output to stdout::

    mvn -Dsurefire.redirectTestOutputToFile=false ...

- Generates findbugs report::

    mvn process-test-classes -P findbugs,examples

- Offline mode::

    mvn -o ....

- Change version::

    mvn versions:set -DnewVersion=[new_version] -DgenerateBackupPoms=false -P examples

- Running from IDE (Intellij and Eclipse)::

    mvn clean package -pl cdap-ui -am -DskipTests -P dist

  (Whenever there is a change in the UI packages.)

  Then, run CDAP Sandbox from IDE.


License and Trademarks
======================

Copyright © 2014-2017 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
either express or implied. See the License for the specific language governing permissions
and limitations under the License.

Cask is a trademark of Cask Data, Inc. All rights reserved.

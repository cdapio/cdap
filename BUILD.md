# Cask Data Application Platform - CDAP

**Standalone and Distributed CDAP**

## Building CDAP Maven

### Clean all modules
    mvn clean

### Run all tests, fail at the end
    MAVEN_OPTS="-Xmx1024m -XX:MaxPermSize=128m" mvn test -fae
    
### Build all modules
    mvn package

### Run checkstyle, skipping tests
    mvn package -DskipTests

### Build a particular module
    mvn package -pl [module] -am

### Run selected test
    MAVEN_OPTS="-Xmx1024m -XX:MaxPermSize=128m" mvn -Dtest=TestClass,TestMore*Class,TestClassMethod#methodName -DfailIfNoTests=false test

See [Surefire doc](http://maven.apache.org/surefire/maven-surefire-plugin/examples/single-test.html) for more details

### Build all examples
    MAVEN_OPTS="-Xmx512m" mvn package -DskipTests -pl cdap-examples -am -amd -P examples

### Build Standalone distribution ZIP
    MAVEN_OPTS="-Xmx512m" mvn clean package -DskipTests -P examples -pl cdap-examples -am -amd && MAVEN_OPTS="-Xmx512m" mvn package -pl cdap-standalone -am -DskipTests -P dist,release
    
### Build the limited set of Javadocs used in distribution ZIP
    mvn clean package javadoc:javadoc -pl cdap-api -am -DskipTests -P release

### Build the complete set of Javadocs, for all modules
    mvn clean site -DskipTests
    
### Build distributions (rpm, deb, tgz)
    mvn package -DskipTests -P dist,rpm-prepare,rpm,deb-prepare,deb,tgz

### Show dependency tree
    mvn package dependency:tree -DskipTests

### Show dependency tree for a particular module
    mvn package dependency:tree -DskipTests -pl [module] -am

### Show test output to stdout
    mvn -Dsurefire.redirectTestOutputToFile=false ...

### Generates findbugs report
    mvn process-test-classes -P findbugs,examples

### Offline mode
    mvn -o ....

### Change version
    mvn versions:set -DnewVersion=[new_version] -DgenerateBackupPoms=false -P examples
    
    
## License and Trademarks

Copyright © 2014 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the 
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
either express or implied. See the License for the specific language governing permissions 
and limitations under the License.

Cask is a trademark of Cask Data, Inc. All rights reserved.

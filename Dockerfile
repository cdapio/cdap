# Copyright © 2012-2014 Cask Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Cask is a trademark of Cask Data, Inc. All rights reserved.
###############################################################################################
# Please visit Docker.com and follow instructions to download Docker SW in your environment.
# This Dockerfile will build a CDAP image from scratch utilizing ubuntu 12.04 as a base image.
# The assumption is that you are running this from the root of the cdap directory structure.
#
FROM ubuntu:12.04
MAINTAINER Cask Data <ops@cask.co>
RUN apt-get update && \
    apt-get install -y software-properties-common python-software-properties && \
    add-apt-repository ppa:chris-lea/node.js && \
    apt-get update && \
    apt-get install -y curl && \
    apt-get install -y --no-install-recommends openjdk-7-jdk && \
    apt-get install -y nodejs && \
    apt-get install -y maven && \
    apt-get install -y unzip

# create Software directory
RUN mkdir /Build /Software

# copy the minimum needed software (to build it) to the container
COPY *xml /Build/
COPY cdap-adapters /Build/cdap-adapters
COPY cdap-api /Build/cdap-api
COPY cdap-app-fabric /Build/cdap-app-fabric
COPY cdap-archetypes /Build/cdap-archetypes
COPY cdap-cli /Build/cdap-cli
COPY cdap-cli-tests /Build/cdap-cli-tests
COPY cdap-client /Build/cdap-client
COPY cdap-client-tests /Build/cdap-client-tests
COPY cdap-common /Build/cdap-common
COPY cdap-common-unit-test /Build/cdap-common-unit-test
COPY cdap-data-fabric /Build/cdap-data-fabric
COPY cdap-data-fabric-tests /Build/cdap-data-fabric-tests
COPY cdap-distributions /Build/cdap-distributions
COPY cdap-docs /Build/cdap-docs
COPY cdap-examples /Build/cdap-examples
COPY cdap-explore /Build/cdap-explore
COPY cdap-explore-client /Build/cdap-explore-client
COPY cdap-explore-jdbc /Build/cdap-explore-jdbc
COPY cdap-gateway /Build/cdap-gateway
COPY cdap-hbase-compat-0.94 /Build/cdap-hbase-compat-0.94
COPY cdap-hbase-compat-0.96 /Build/cdap-hbase-compat-0.96
COPY cdap-hbase-compat-0.98 /Build/cdap-hbase-compat-0.98
COPY cdap-integration-test /Build/cdap-integration-test
COPY cdap-kafka /Build/cdap-kafka
COPY cdap-master /Build/cdap-master
COPY cdap-notifications /Build/cdap-notifications
COPY cdap-notifications-api /Build/cdap-notifications-api
COPY cdap-proto /Build/cdap-proto
COPY cdap-security /Build/cdap-security
COPY cdap-security-service /Build/cdap-security-service
COPY cdap-standalone /Build/cdap-standalone
COPY cdap-test /Build/cdap-test
COPY cdap-unit-test /Build/cdap-unit-test
COPY cdap-unit-test-standalone /Build/cdap-unit-test-standalone
COPY cdap-watchdog /Build/cdap-watchdog
COPY cdap-watchdog-api /Build/cdap-watchdog-api
COPY cdap-watchdog-tests /Build/cdap-watchdog-tests
COPY cdap-web-app /Build/cdap-web-app

# build cdap-standalone zip file, copy it to container and extract it
RUN cd Build && \
    MAVEN_OPTS="-Xmx512m" mvn clean package -DskipTests -P examples -pl cdap-examples -am -amd && \
    mvn package -pl cdap-standalone -am -DskipTests -P dist,release && \
    unzip cdap-standalone/target/cdap-sdk-[0-9]*.[0-9]*.[0-9]*.zip -d /Software && \
    cd /Software && \
    rm -rf /Build /root/.m2 /var/cache/debconf/*-old /usr/share/{doc,man} \
    /usr/share/locale/{a,b,c,d,e{l,o,s,t,u},f,g,h,i,j,k,lt,lv,m,n,o,p,r,s,t,u,v,w,x,z}*

# Expose Ports (9999 & 10000 for CDAP)
EXPOSE 9999
EXPOSE 10000

# Clean UP (reduce space usage of container as much as possible)
RUN apt-get purge -y maven unzip && \
    apt-get clean && \
    apt-get autoclean && \
    apt-get -y autoremove 

# start CDAP in the background and tail in the foreground
ENTRYPOINT /Software/cdap-sdk-[0-9]*.[0-9]*.[0-9]*/bin/cdap.sh start && \
    /usr/bin/tail -F /Software/cdap-sdk-[0-9]*.[0-9]*.[0-9]*/logs/*.log

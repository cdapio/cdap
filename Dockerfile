################################################################################################
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
################################################################################################
# Please visit Docker.com and follow instructions to download Docker SW in your environment.
# This Dockerfile will build a CDAP image from scratch utilizing ubuntu 12.04 as a base image.
# The assumption is that you are running this from the root of the cdap directory structure.
################################################################################################
FROM ubuntu:12.04
MAINTAINER Cask Data  <it-admin@cask.co>
RUN apt-get update && \
    apt-get install -y software-properties-common python-software-properties && \
    add-apt-repository ppa:chris-lea/node.js && \
    apt-get update && \
    apt-get install -y software-properties-common && \
    apt-get install -y python-software-properties && \
    apt-get install -y curl && \
    apt-get install -y --no-install-recommends openjdk-7-jdk && \
    apt-get install -y nodejs && \
    apt-get install -y maven
    apt-get install -y unzip zip vim

# create Software directory
RUN mkdir Software

# copy the minimum needed software (to build it) to the container
COPY KEYS LICENSE.txt *xml Software/ 
COPY cdap-api Software/cdap-api
COPY cdap-app-fabric Software/cdap-app-fabric
COPY cdap-archetypes Software/cdap-archetypes
COPY cdap-cli Software/cdap-cli
COPY cdap-cli-tests Software/cdap-cli-tests
COPY cdap-client Software/cdap-client
COPY cdap-client-tests Software/cdap-client-tests
COPY cdap-common Software/cdap-common
COPY cdap-common-unit-test Software/cdap-common-unit-test
COPY cdap-data-fabric Software/cdap-data-fabric
COPY cdap-data-fabric-tests Software/cdap-data-fabric-tests
COPY cdap-distributions Software/cdap-distributions
COPY cdap-docs Software/cdap-docs
COPY cdap-examples Software/cdap-examples
COPY cdap-explore Software/cdap-explore
COPY cdap-explore-client Software/cdap-explore-client
COPY cdap-explore-jdbc Software/cdap-explore-jdbc
COPY cdap-gateway Software/cdap-gateway
COPY cdap-hbase-compat-0.94 Software/cdap-hbase-compat-0.94
COPY cdap-hbase-compat-0.96 Software/cdap-hbase-compat-0.96
COPY cdap-hbase-compat-0.98 Software/cdap-hbase-compat-0.98
COPY cdap-kafka Software/cdap-kafka
COPY cdap-master Software/cdap-master
COPY cdap-proto Software/cdap-proto
COPY cdap-security Software/cdap-security
COPY cdap-security-service Software/cdap-security-service
COPY cdap-standalone Software/cdap-standalone
COPY cdap-unit-test Software/cdap-unit-test
COPY cdap-unit-test-standalone Software/cdap-unit-test-standalone
COPY cdap-watchdog Software/cdap-watchdog
COPY cdap-watchdog-api Software/cdap-watchdog-api
COPY cdap-watchdog-tests Software/cdap-watchdog-tests
COPY cdap-web-app Software/cdap-web-app

# build cdap-standalone zip file, copy it to container and extract it
RUN cd Software && MAVEN_OPTS="-Xmx512m" mvn clean package -DskipTests -P examples -pl cdap-examples -am -amd && mvn package -pl cdap-standalone -am -DskipTests -P dist,release && \
    mv cdap-standalone/target/cdap-sdk-[0-9]*.[0-9]*.[0-9]*.zip . && \
    unzip cdap-sdk-[0-9]*.[0-9]*.[0-9]*.zip && \
    rm cdap-sdk-[0-9]*.[0-9]*.[0-9]*.zip

# SSH
RUN DEBIAN_FRONTEND=noninteractive apt-get install -y openssh-server && mkdir -p /var/run/sshd && echo 'root:root' | chpasswd
RUN sed -i "s/session.*required.*pam_loginuid.so/#session    required     pam_loginuid.so/" /etc/pam.d/sshd
RUN sed -i "s/PermitRootLogin without-password/#PermitRootLogin without-password/" /etc/ssh/sshd_config

# Expose Ports (9999 & 10000 for CDAP)
EXPOSE 9999
EXPOSE 10000
EXPOSE 22

# CDAP
## Add dependent files
COPY cdap-standalone/cdap-site-docker.xml /tmp/cdap-site-docker.xml
COPY cdap-standalone/cdap-docker.sh /tmp/cdap-docker.sh

## modify CDAP configuration files
RUN cp /tmp/cdap-site-docker.xml /Software/cdap-sdk-[0-9]*.[0-9]*.[0-9]*/conf/cdap-site.xml
RUN cp /tmp/cdap-docker.sh /Software/cdap-sdk-[0-9]*.[0-9]*.[0-9]*/bin/cdap.sh

# start CDAP in the background and ssh in the foreground
CMD /Software/cdap-sdk-[0-9]*.[0-9]*.[0-9]*/bin/cdap.sh start && /usr/sbin/sshd -D

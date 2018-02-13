# Copyright © 2012-2017 Cask Data, Inc.
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

# This currently assumes that you are running against a singlenode cluster. You can pass the hadoop and docker hostnames and it will be applied ot the configs.
# EXAMPLE: docker build -t cdap/master:latest -f services.Dockerfile --build-arg CDAP_ROLE=master --build-arg CDAP_COMPONENT=master --build-arg HADOOP_HOST=<HADOOP_HOSTNAME> --build-arg DOCKER_HOST=<DOCKER_HOSTNAME> .
# You can also use this with docker-compose by running "docker-compose up -d"

FROM ubuntu:16.04
MAINTAINER Cask Data <ops@cask.co>

ARG CDAP_ROLE
ARG CDAP_COMPONENT
ARG PORTS
ARG HADOOP_HOST
ARG DOCKER_HOST
ENV CDAP_ROLE $CDAP_ROLE
ENV CDAP_COMPONENT $CDAP_COMPONENT
ENV HADOOP_HOST $HADOOP_HOST
ENV DOCKER_HOST $DOCKER_HOST

# update system
RUN apt-get update && \
  apt-get dist-upgrade -y && \
  apt-get install -y curl vim less net-tools

# grab gosu for easy step-down from root
ENV GOSU_VERSION 1.7
RUN apt-get install -y --no-install-recommends git && \
  curl -vL \
    "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$(dpkg --print-architecture)" > \
    /usr/local/bin/gosu && \
  curl -vL \
    "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$(dpkg --print-architecture).asc" > \
    /usr/local/bin/gosu.asc && \
  export GNUPGHOME="$(mktemp -d)" && \
  gpg --keyserver ha.pool.sks-keyservers.net --recv-keys B42F6819007F00F88E364FD4036A9C25BF357DD4 && \
  gpg --batch --verify /usr/local/bin/gosu.asc /usr/local/bin/gosu && \
  rm -r "$GNUPGHOME" /usr/local/bin/gosu.asc && \
  chmod +x /usr/local/bin/gosu && \
  gosu nobody true

# Copy scripts and files before using them below
COPY packer/scripts /tmp/scripts
COPY packer/files /tmp/files

# Install Chef, setup APT, config file setup, install Java(needed for UI), install spark, run Chef cdap::COMPONENT recipe, then clean up
RUN curl -vL https://chef.io/chef/install.sh | bash -s -- -v 12.21.31 && \
    for i in apt-setup.sh cookbook-dir.sh cookbook-setup.sh ; do /tmp/scripts/$i ; done && \
    sed -i -e "s/<HADOOP_HOST>/$HADOOP_HOST/g" /tmp/files/cdap-services.json && \
    sed -i -e "s/<DOCKER_HOST>/$DOCKER_HOST/g" /tmp/files/cdap-services.json && \
    chef-solo -o java::default -j /tmp/files/cdap-services.json && \
    if [ "$CDAP_ROLE" == "master" ]; then chef-solo -o hadoop::spark -j /tmp/files/cdap-services.json ; fi && \
    chef-solo -o cdap::${CDAP_COMPONENT} -j /tmp/files/cdap-services.json && \
    chef-solo -o cdap::config -j /tmp/files/cdap-services.json && \
    for i in remove-chef.sh apt-cleanup.sh ; do /tmp/scripts/$i ; done && \
    rm -rf /root/.m2 /var/cache/debconf/*-old /usr/share/{doc,man} /tmp/scripts /tmp/files \
      /var/lib/apt/lists/* \
      /usr/share/locale/{a,b,c,d,e{l,o,s,t,u},f,g,h,i,j,k,lt,lv,m,n,o,p,r,s,t,u,v,w,x,z}*

# Can't copy from outside the build dir
# temp hack to do dev work on functions.sh
COPY packer/bin /opt/cdap/$CDAP_COMPONENT/bin

ENV PATH /opt/cdap/$CDAP_COMPONENT/bin:${PATH}

# Copy entrypoint
COPY docker-service-entrypoint.sh /
RUN ["chmod", "+x", "/docker-service-entrypoint.sh"]

EXPOSE $PORTS

# start CDAP in the background and tail in the foreground
ENTRYPOINT ["/docker-service-entrypoint.sh"]
CMD ["sh","-c","cdap $CDAP_ROLE start --foreground"]

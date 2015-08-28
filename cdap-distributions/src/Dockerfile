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

# Copy scripts
COPY packer/scripts /tmp/scripts

# Copy json
COPY packer/files /tmp/files

# build cdap-standalone zip file, copy it to container and extract it
RUN apt-get update && \
    apt-get install -y curl git && \
    curl -L http://chef.io/chef/install.sh | bash && \
    for i in apt-setup.sh cookbook-dir.sh cookbook-setup.sh ; do /tmp/scripts/$i ; done && \
    chef-solo -o cdap::sdk -j /tmp/files/cdap-sdk.json && \
    for i in remove-chef.sh sdk-cleanup.sh apt-cleanup.sh ; do /tmp/scripts/$i ; done && \
    rm -rf /root/.m2 /var/cache/debconf/*-old /usr/share/{doc,man} /tmp/scripts /tmp/files \
    /usr/share/locale/{a,b,c,d,e{l,o,s,t,u},f,g,h,i,j,k,lt,lv,m,n,o,p,r,s,t,u,v,w,x,z}*

# Expose Ports (9999 & 10000 for CDAP)
EXPOSE 9999
EXPOSE 10000

# start CDAP in the background and tail in the foreground
ENTRYPOINT /opt/cdap/sdk/bin/cdap.sh start && \
    /usr/bin/tail -F /opt/cdap/sdk/logs/*.log

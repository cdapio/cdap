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

FROM eclipse-temurin:11-jdk-jammy
MAINTAINER Cask Data <ops@cask.co>

# Install OS packages, gosu, and Node.js 22.x
RUN apt-get update && \
  apt-get dist-upgrade -y && \
  apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    gnupg \
    gosu \
    jq \
    libxml2-utils \
    procps \
    unzip && \
  mkdir -p /etc/apt/keyrings && \
  curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key \
    | gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg && \
  echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_22.x nodistro main" \
    > /etc/apt/sources.list.d/nodesource.list && \
  apt-get update && \
  apt-get install -y --no-install-recommends nodejs && \
  gosu nobody true && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*

# Copy rendered cdap-sdk.json produced by docker-deploy.yml
COPY packer/files /tmp/files

# Create cdap user and install CDAP Sandbox directly without Chef
RUN groupadd -r cdap && \
    useradd -r -g cdap -d /opt/cdap -s /bin/bash cdap && \
    SANDBOX_URL="$(jq -r '.cdap.sdk.url' /tmp/files/cdap-sdk.json)" && \
    curl -fsSL -o /tmp/cdap-sandbox.zip "${SANDBOX_URL}" && \
    mkdir -p /opt/cdap && \
    unzip -q /tmp/cdap-sandbox.zip -d /opt/cdap && \
    mv /opt/cdap/cdap-sandbox-* /opt/cdap/sandbox && \
    chown -R cdap:cdap /opt/cdap && \
    rm -rf /tmp/cdap-sandbox.zip /tmp/files && \
    gosu cdap /opt/cdap/sandbox/bin/cdap version

ENV PATH /opt/cdap/sandbox/bin:${PATH}

# Copy entrypoint
COPY docker-entrypoint.sh /

# Expose Ports (11011 & 11015 for CDAP)
EXPOSE 11011 11015

# start CDAP in the background and tail in the foreground
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["cdap","sandbox","start","--foreground"]

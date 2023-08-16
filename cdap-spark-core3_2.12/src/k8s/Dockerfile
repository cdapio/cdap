#
# Copyright © 2021 Cask Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#

# Creates a customized Spark image that can be used to run CDAP Spark programs
# This should be run from the CDAP base directory.
# Before creating the image, cdap-spark-core and cdap-kubernetes must be built:
#
# mvn package -P dist -pl cdap-spark-core3_2.12,cdap-kubernetes,cdap-authenticator-ext-gcp -am -DskipTests
#
# Then build the image:
#
# docker build --no-cache --build-arg base_image=<image> \
#   -f cdap-spark-core3_2.12/src/k8s/Dockerfile \
#   . -t <image>:<tag>
ARG base_image

# create source tarball and download connectors, their dependencies, and their licenses
FROM openjdk:8-jdk AS sources
ENV DIR /cdap/build
COPY . $DIR/

RUN tar -zcvf /tmp/cdap-build-sources.tar.gz --exclude='.git*' --exclude='node_modules' --exclude='target' $DIR && \
  apt-get update && \
  apt-get -y install curl && \
  mkdir -p /tmp/connectors && \
  curl -L -o /tmp/connectors/gcs-connector-hadoop2-2.2.5.jar https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop2-2.2.5.jar && \
  # start aws dependency
  curl -L -o /tmp/connectors/hadoop-aws-3.2.0.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.0/hadoop-aws-3.2.0.jar && \
  curl -L -o /tmp/connectors/aws-java-sdk-bundle-1.11.199.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.199/aws-java-sdk-bundle-1.11.199.jar && \
  curl -L -o /tmp/connectors/jets3t-0.9.4.jar  https://repo1.maven.org/maven2/net/java/dev/jets3t/jets3t/0.9.4/jets3t-0.9.4.jar && \
  curl -L -o /tmp/connectors/commons-lang-2.6.jar  https://repo1.maven.org/maven2/commons-lang/commons-lang/2.6/commons-lang-2.6.jar && \
  # end aws dependency
  mkdir -p /tmp/LICENSES && \
  mkdir /tmp/LICENSES/gcs-connector-hadoop2-2.2.5 && curl -L -o /tmp/LICENSES/gcs-connector-hadoop2-2.2.5/LICENSE https://raw.githubusercontent.com/GoogleCloudDataproc/hadoop-connectors/v2.2.5/LICENSE && \
  mkdir /tmp/LICENSES/hadoop-aws-3.2.0 && jar xvf /tmp/connectors/hadoop-aws-3.2.0.jar META-INF/LICENSE META-INF/LICENSE.txt && cp ./META-INF/LICENSE* /tmp/LICENSES/hadoop-aws-3.2.0 && rm -r ./META-INF && \
  mkdir /tmp/LICENSES/aws-java-sdk-bundle-1.11.199 && jar xvf /tmp/connectors/aws-java-sdk-bundle-1.11.199.jar META-INF/LICENSE META-INF/LICENSE.txt && cp ./META-INF/LICENSE* /tmp/LICENSES/aws-java-sdk-bundle-1.11.199 && rm -r ./META-INF && \
  mkdir /tmp/LICENSES/jets3t-0.9.4 && curl -L -o /tmp/LICENSES/jets3t-0.9.4/LICENSE https://raw.githubusercontent.com/mondain/jets3t/master/LICENSE && \
  mkdir /tmp/LICENSES/commons-lang-2.6 && jar xvf /tmp/connectors/commons-lang-2.6.jar META-INF/LICENSE META-INF/LICENSE.txt && cp ./META-INF/LICENSE* /tmp/LICENSES/commons-lang-2.6 && rm -r ./META-INF

FROM ${base_image} AS build
USER root
RUN apt-get update && apt-get upgrade -y

# copy cdap-spark-core and its dependencies
COPY cdap-spark-core3_2.12/target/k8s /opt/cdap/cdap-spark-core
# spark base image sets user to spark_uid, which doesn't have permission to do anything in the RUN command
USER root
# make a symlink for the cdap-spark-core.jar without the cdap version
# this is so that it can be referred to as a hardcoded value local:/opt/cdap/cdap-spark-core/cdap-spark-core.jar
# for the Spark job jar
RUN ln -s /opt/cdap/cdap-spark-core/io.cdap.cdap.cdap-spark-core3_2.12-*.jar /opt/cdap/cdap-spark-core/cdap-spark-core.jar && \
  mkdir -p /etc/cdap/conf
# copy source code tarball, connectors, and licenses
COPY cdap-distributions/src/COPYRIGHT /opt/cdap/LICENSES
COPY --from=sources /tmp/cdap-build-sources.tar.gz /opt/cdap/cdap-build-sources.tar.gz
COPY --from=sources /tmp/connectors /opt/cdap/cdap-spark-core/lib/
COPY --from=sources /tmp/LICENSES /opt/cdap/LICENSES/

USER ${spark_uid}

# copy k8s master environment spi for service discovery
COPY cdap-kubernetes/target/libexec /opt/cdap/master/ext/environments/k8s
# copy gcp auth extension for tethered runs
COPY cdap-authenticator-ext-gcp/target/libexec /opt/cdap/master/ext/authenticators/gcp-remote-authenticator

# modified version of the Spark entrypoint that calls the CDAP SparkContainerLauncher
COPY cdap-spark-core3_2.12/src/k8s/entrypoint.sh /opt/cdap/cdap-spark-core/entrypoint.sh

RUN chown root:root /etc/passwd
RUN chmod 644 /etc/passwd
RUN passwd -l root
ENTRYPOINT [ "/opt/cdap/cdap-spark-core/entrypoint.sh" ]


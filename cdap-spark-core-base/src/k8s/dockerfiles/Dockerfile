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
# mvn package -P dist,k8s-dist -pl cdap-spark-core3_2.12,cdap-kubernetes -am -DskipTests
#
# Then build the image:
#
# docker build --no-cache --build-arg base_image=<image> -f cdap-spark-core-base/src/k8s/dockerfiles/Dockerfile . -t <image>:<tag>
# docker build --no-cache --build-arg base_image=<image> \
#   --build-arg cdap_version=`mvn -q -Dexec.executable=echo -Dexec.args='${project.version}' --non-recursive exec:exec` \
#   -f cdap-spark-core-base/src/k8s/dockerfiles/Dockerfile \
#   . -t <image>:<tag>

ARG base_image

FROM ${base_image}

ARG cdap_spark_module=cdap-spark-core3_2.12
ARG cdap_version

# copy cdap-spark-core and its dependencies
COPY ${cdap_spark_module}/target/k8s/${cdap_spark_module} /opt/cdap/cdap-spark-core
# spark base image sets user to spark_uid, which doesn't have permission to do anything in the RUN command
USER root
# download gcs-connector and make a symlink for the cdap-spark-core.jar without the cdap version
# this is so that it can be referred to as a hardcoded value local:/opt/cdap/cdap-spark-core/cdap-spark-core.jar
# for the Spark job jar
RUN apt-get update && \
  apt-get -y install curl && \
  curl -L -o /opt/cdap/cdap-spark-core/lib/gcs-connector-hadoop2-latest.jar https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop2-latest.jar && \
  ln -s /opt/cdap/cdap-spark-core/io.cdap.cdap.${cdap_spark_module}-${cdap_version}.jar /opt/cdap/cdap-spark-core/cdap-spark-core.jar && \
  mkdir -p /etc/cdap/conf
USER ${spark_uid}

# copy k8s master environment spi for service discovery
COPY cdap-kubernetes/target/libexec /opt/cdap/master/ext/environments/k8s

# overwrite spark-submit with a version that calls the CDAP SparkContainerLauncher
COPY cdap-spark-core-base/src/k8s/spark/bin/spark-submit $SPARK_HOME/bin/spark-submit

# modified version of the Spark entrypoint that calls the CDAP SparkContainerLauncher
COPY cdap-spark-core-base/src/k8s/spark/entrypoint.sh /opt/cdap/cdap-spark-core/entrypoint.sh
ENTRYPOINT [ "/opt/cdap/cdap-spark-core/entrypoint.sh" ]


# <!--
#   Copyright © 2014-2021 Cask Data, Inc.
#
#   Licensed under the Apache License, Version 2.0 (the "License"); you may not
#   use this file except in compliance with the License. You may obtain a copy of
#   the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#   License for the specific language governing permissions and limitations under
#   the License.
#   -->
# Dockerfile content for $CDAP_SRC/Dockerfile
FROM us-east1-docker.pkg.dev/cloud-data-fusion-images-ap/cdf/cloud-data-fusion:latest

# Define the service-specific library directory for Watchdog / Master
ENV WATCHDOG_LIB_DIR="/opt/cdap/master/lib"
ENV EXT_DIR="/opt/cdap/master/ext"
ENV CDAP_VERSION="6.12.0-SNAPSHOT"
ENV TWILL_VERSION="1.5.0-SNAPSHOT"
ENV CDAP_COMMON_VERSION="0.15.0-SNAPSHOT"
# Must match <guava.version> in the root pom.xml. The base image ships
# com.google.guava.guava-13.0.1.jar in /opt/cdap/master/lib (and older Guava
# versions in several /opt/cdap/master/ext/* directories), which predates
# overloads such as Preconditions.checkNotNull(Object, String, Object) (added in
# Guava 20). Code compiled against Guava 32 binds to those overloads directly,
# so leaving 13.0.1 on the classpath fails at runtime with:
#   NoSuchMethodError: 'java.lang.Object
#     com.google.common.base.Preconditions.checkNotNull(
#       java.lang.Object, java.lang.String, java.lang.Object)'
ENV GUAVA_VERSION="32.0.0-jre"

# Create the Watchdog lib directory, write /opt/cdap/VERSION so
# /opt/cdap/master/bin/functions.sh (line 829: cdap_version) does not log:
#   /opt/cdap/master/bin/functions.sh: line 829: /opt/cdap/VERSION: No such file or directory
# and install iproute2 + iptables required by the GKE/CDF edit-routes init
# container on task-worker pods (which runs `ip`, `iptables`, and
# `update-alternatives --set iptables /usr/sbin/iptables-legacy`).
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends iproute2 iptables && \
    rm -rf /var/lib/apt/lists/* && \
    mkdir -p "${WATCHDOG_LIB_DIR}" && \
    printf '%s\n' "${CDAP_VERSION}" > /opt/cdap/VERSION && \
    printf '%s\n' "${CDAP_VERSION}" > /opt/cdap/master/VERSION

# Remove old versions of the JARs being updated in /opt/cdap/master/lib and
# /opt/cdap/master/ext/*.
# NOTE:
# 1. Trailing "-"*.jar glob strips ANY previously installed version of the
#    artifact (e.g. Twill 1.4.0 vs 1.5.0-SNAPSHOT, common 0.13.1 vs
#    0.15.0-SNAPSHOT, Guava 13.0.1 vs 32.0.0-jre), which is required because
#    cdap_set_classpath sorts /opt/cdap/master/lib lexicographically.
# 2. Isolated extensions (cdap-storage-ext-spanner, cdap-messaging-ext-spanner,
#    cdap-metadata-ext-spanner, etc.) are explicitly removed from master/lib and
#    installed ONLY in their /opt/cdap/master/ext/* directories so ServiceLoader
#    does not load them on the system classloader (where protobuf-java 3.23.2
#    causes VerifyError against Spanner's protobuf-java 4.x stubs).
RUN set -eu; \
    for m in \
      cdap-api \
      cdap-api-common \
      cdap-api-spark3_2.12 \
      cdap-app-fabric \
      cdap-app-fabric-tests \
      cdap-authenticator-ext-gcp \
      cdap-cli \
      cdap-cli-tests \
      cdap-client \
      cdap-client-tests \
      cdap-common \
      cdap-common-unit-test \
      cdap-credential-ext-gcp-wi \
      cdap-data-fabric \
      cdap-data-fabric-tests \
      cdap-distributions \
      cdap-docs-gen \
      cdap-e2e-tests \
      cdap-elastic \
      cdap-encryption-ext-tink \
      cdap-error-api \
      cdap-event-common-spi \
      cdap-event-reader-spi \
      cdap-event-writer-spi \
      cdap-features \
      cdap-formats \
      cdap-gateway \
      cdap-integration-test \
      cdap-kafka \
      cdap-kubernetes \
      cdap-log-publisher-spi \
      cdap-master \
      cdap-master-spi \
      cdap-messaging-ext-spanner \
      cdap-messaging-spi \
      cdap-metadata-ext-spanner \
      cdap-metadata-spi \
      cdap-operational-stats-core \
      cdap-proto \
      cdap-runtime-ext-dataproc \
      cdap-runtime-ext-emr \
      cdap-runtime-ext-remote-hadoop \
      cdap-runtime-spi \
      cdap-securestore-ext-cloudkms \
      cdap-securestore-ext-gcp-secretstore \
      cdap-securestore-spi \
      cdap-security \
      cdap-security-spi \
      cdap-source-control \
      cdap-spark-python \
      cdap-standalone \
      cdap-storage-ext-spanner \
      cdap-storage-spi \
      cdap-support-bundle \
      cdap-system-app-api \
      cdap-system-app-unit-test \
      cdap-test \
      cdap-tms \
      cdap-tms-tests \
      cdap-ui \
      cdap-unit-test \
      cdap-unit-test-spark3_2.12 \
      cdap-watchdog \
      cdap-watchdog-api \
    ; do \
      rm -f "${WATCHDOG_LIB_DIR}/io.cdap.cdap.${m}-"*.jar; \
    done; \
    for m in \
      twill-core \
      twill-api \
      twill-common \
      twill-discovery-api \
      twill-discovery-core \
      twill-ext \
      twill-yarn \
      twill-zookeeper \
    ; do \
      rm -f "${WATCHDOG_LIB_DIR}/io.cdap.twill.${m}-"*.jar; \
    done; \
    for m in \
      common-cli \
      common-core \
      common-http \
      common-io \
      common-lang \
    ; do \
      rm -f "${WATCHDOG_LIB_DIR}/io.cdap.common.${m}-"*.jar; \
    done; \
    rm -f "${WATCHDOG_LIB_DIR}/com.google.guava.guava-"*.jar; \
    rm -f "${EXT_DIR}/storageproviders/gcp-spanner/io.cdap.cdap.cdap-storage-ext-spanner-"*.jar; \
    rm -f "${EXT_DIR}/messagingproviders/gcp-spanner/io.cdap.cdap.cdap-"*.jar \
          "${EXT_DIR}/messagingproviders/gcp-spanner/io.cdap.twill.twill-"*.jar; \
    rm -f "${EXT_DIR}/environments/k8s/io.cdap.cdap.cdap-"*.jar \
          "${EXT_DIR}/environments/k8s/io.cdap.twill.twill-"*.jar \
          "${EXT_DIR}/environments/k8s/com.google.guava.guava-"*.jar; \
    rm -f "${EXT_DIR}/authenticators/gcp-remote-authenticator/io.cdap.cdap.cdap-authenticator-ext-gcp-"*.jar \
          "${EXT_DIR}/authenticators/gcp-remote-authenticator/com.google.guava.guava-"*.jar; \
    rm -f "${EXT_DIR}/credentialproviders/gcp-wi-credential-provider/io.cdap.cdap.cdap-credential-ext-gcp-wi-"*.jar \
          "${EXT_DIR}/credentialproviders/gcp-wi-credential-provider/com.google.guava.guava-"*.jar; \
    rm -f "${EXT_DIR}/encryption/tink/io.cdap.cdap.cdap-encryption-ext-tink-"*.jar; \
    rm -f "${EXT_DIR}/operations/core/io.cdap.cdap.cdap-operational-stats-core-"*.jar; \
    rm -f "${EXT_DIR}/runtimeproviders/gcp-dataproc/io.cdap.cdap.cdap-runtime-ext-dataproc-"*.jar \
          "${EXT_DIR}/runtimeproviders/gcp-dataproc/io.cdap.twill.twill-"*.jar; \
    rm -f "${EXT_DIR}/runtimeproviders/emr/io.cdap.cdap.cdap-runtime-ext-emr-"*.jar \
          "${EXT_DIR}/runtimeproviders/emr/com.google.guava.guava-"*.jar; \
    rm -f "${EXT_DIR}/runtimeproviders/remote-hadoop/io.cdap.cdap.cdap-runtime-ext-remote-hadoop-"*.jar \
          "${EXT_DIR}/runtimeproviders/remote-hadoop/com.google.guava.guava-"*.jar; \
    rm -f "${EXT_DIR}/runtimes/spark3_2.12/io.cdap.cdap.cdap-"*.jar; \
    rm -f "${EXT_DIR}/securestores/gcp-cloudkms/io.cdap.cdap.cdap-securestore-ext-cloudkms-"*.jar \
          "${EXT_DIR}/securestores/gcp-cloudkms/com.google.guava.guava-"*.jar; \
    rm -f "${EXT_DIR}/securestores/gcp-secretstore/io.cdap.cdap.cdap-securestore-ext-gcp-secretstore-"*.jar \
          "${EXT_DIR}/securestores/gcp-secretstore/com.google.guava.guava-"*.jar; \
    rm -f "${EXT_DIR}/metricswriters/gcp-monitoring/io.cdap.twill.twill-common-"*.jar

# --- Core CDAP Master Modules (/opt/cdap/master/lib) ---
# Only modules that belong on the master system classpath are copied here.
# CLI fat jars (cdap-cli), standalone/test jars, and isolated extensions are
# excluded from master/lib to prevent SLF4J multiple-binding warnings and
# extension classloader conflicts.
COPY "cdap-api/target/cdap-api-${CDAP_VERSION}.jar" "${WATCHDOG_LIB_DIR}/io.cdap.cdap.cdap-api-${CDAP_VERSION}.jar"
COPY "cdap-api-common/target/cdap-api-common-${CDAP_VERSION}.jar" "${WATCHDOG_LIB_DIR}/io.cdap.cdap.cdap-api-common-${CDAP_VERSION}.jar"
COPY "cdap-app-fabric/target/cdap-app-fabric-${CDAP_VERSION}.jar" "${WATCHDOG_LIB_DIR}/io.cdap.cdap.cdap-app-fabric-${CDAP_VERSION}.jar"
COPY "cdap-client/target/cdap-client-${CDAP_VERSION}.jar" "${WATCHDOG_LIB_DIR}/io.cdap.cdap.cdap-client-${CDAP_VERSION}.jar"
COPY "cdap-common/target/cdap-common-${CDAP_VERSION}.jar" "${WATCHDOG_LIB_DIR}/io.cdap.cdap.cdap-common-${CDAP_VERSION}.jar"
COPY "cdap-data-fabric/target/cdap-data-fabric-${CDAP_VERSION}.jar" "${WATCHDOG_LIB_DIR}/io.cdap.cdap.cdap-data-fabric-${CDAP_VERSION}.jar"
COPY "cdap-elastic/target/cdap-elastic-${CDAP_VERSION}.jar" "${WATCHDOG_LIB_DIR}/io.cdap.cdap.cdap-elastic-${CDAP_VERSION}.jar"
COPY "cdap-error-api/target/cdap-error-api-${CDAP_VERSION}.jar" "${WATCHDOG_LIB_DIR}/io.cdap.cdap.cdap-error-api-${CDAP_VERSION}.jar"
COPY "cdap-event-common-spi/target/cdap-event-common-spi-${CDAP_VERSION}.jar" "${WATCHDOG_LIB_DIR}/io.cdap.cdap.cdap-event-common-spi-${CDAP_VERSION}.jar"
COPY "cdap-event-reader-spi/target/cdap-event-reader-spi-${CDAP_VERSION}.jar" "${WATCHDOG_LIB_DIR}/io.cdap.cdap.cdap-event-reader-spi-${CDAP_VERSION}.jar"
COPY "cdap-event-writer-spi/target/cdap-event-writer-spi-${CDAP_VERSION}.jar" "${WATCHDOG_LIB_DIR}/io.cdap.cdap.cdap-event-writer-spi-${CDAP_VERSION}.jar"
COPY "cdap-features/target/cdap-features-${CDAP_VERSION}.jar" "${WATCHDOG_LIB_DIR}/io.cdap.cdap.cdap-features-${CDAP_VERSION}.jar"
COPY "cdap-formats/target/cdap-formats-${CDAP_VERSION}.jar" "${WATCHDOG_LIB_DIR}/io.cdap.cdap.cdap-formats-${CDAP_VERSION}.jar"
COPY "cdap-gateway/target/cdap-gateway-${CDAP_VERSION}.jar" "${WATCHDOG_LIB_DIR}/io.cdap.cdap.cdap-gateway-${CDAP_VERSION}.jar"
COPY "cdap-log-publisher-spi/target/cdap-log-publisher-spi-${CDAP_VERSION}.jar" "${WATCHDOG_LIB_DIR}/io.cdap.cdap.cdap-log-publisher-spi-${CDAP_VERSION}.jar"
COPY "cdap-master/target/cdap-master-${CDAP_VERSION}.jar" "${WATCHDOG_LIB_DIR}/io.cdap.cdap.cdap-master-${CDAP_VERSION}.jar"
COPY "cdap-master-spi/target/cdap-master-spi-${CDAP_VERSION}.jar" "${WATCHDOG_LIB_DIR}/io.cdap.cdap.cdap-master-spi-${CDAP_VERSION}.jar"
COPY "cdap-messaging-spi/target/cdap-messaging-spi-${CDAP_VERSION}.jar" "${WATCHDOG_LIB_DIR}/io.cdap.cdap.cdap-messaging-spi-${CDAP_VERSION}.jar"
COPY "cdap-metadata-spi/target/cdap-metadata-spi-${CDAP_VERSION}.jar" "${WATCHDOG_LIB_DIR}/io.cdap.cdap.cdap-metadata-spi-${CDAP_VERSION}.jar"
COPY "cdap-proto/target/cdap-proto-${CDAP_VERSION}.jar" "${WATCHDOG_LIB_DIR}/io.cdap.cdap.cdap-proto-${CDAP_VERSION}.jar"
COPY "cdap-runtime-spi/target/cdap-runtime-spi-${CDAP_VERSION}.jar" "${WATCHDOG_LIB_DIR}/io.cdap.cdap.cdap-runtime-spi-${CDAP_VERSION}.jar"
COPY "cdap-securestore-spi/target/cdap-securestore-spi-${CDAP_VERSION}.jar" "${WATCHDOG_LIB_DIR}/io.cdap.cdap.cdap-securestore-spi-${CDAP_VERSION}.jar"
COPY "cdap-security/target/cdap-security-${CDAP_VERSION}.jar" "${WATCHDOG_LIB_DIR}/io.cdap.cdap.cdap-security-${CDAP_VERSION}.jar"
COPY "cdap-security-spi/target/cdap-security-spi-${CDAP_VERSION}.jar" "${WATCHDOG_LIB_DIR}/io.cdap.cdap.cdap-security-spi-${CDAP_VERSION}.jar"
COPY "cdap-source-control/target/cdap-source-control-${CDAP_VERSION}.jar" "${WATCHDOG_LIB_DIR}/io.cdap.cdap.cdap-source-control-${CDAP_VERSION}.jar"
COPY "cdap-storage-spi/target/cdap-storage-spi-${CDAP_VERSION}.jar" "${WATCHDOG_LIB_DIR}/io.cdap.cdap.cdap-storage-spi-${CDAP_VERSION}.jar"
COPY "cdap-support-bundle/target/cdap-support-bundle-${CDAP_VERSION}.jar" "${WATCHDOG_LIB_DIR}/io.cdap.cdap.cdap-support-bundle-${CDAP_VERSION}.jar"
COPY "cdap-system-app-api/target/cdap-system-app-api-${CDAP_VERSION}.jar" "${WATCHDOG_LIB_DIR}/io.cdap.cdap.cdap-system-app-api-${CDAP_VERSION}.jar"
COPY "cdap-tms/target/cdap-tms-${CDAP_VERSION}.jar" "${WATCHDOG_LIB_DIR}/io.cdap.cdap.cdap-tms-${CDAP_VERSION}.jar"
COPY "cdap-watchdog/target/cdap-watchdog-${CDAP_VERSION}.jar" "${WATCHDOG_LIB_DIR}/io.cdap.cdap.cdap-watchdog-${CDAP_VERSION}.jar"
COPY "cdap-watchdog-api/target/cdap-watchdog-api-${CDAP_VERSION}.jar" "${WATCHDOG_LIB_DIR}/io.cdap.cdap.cdap-watchdog-api-${CDAP_VERSION}.jar"

# --- io.cdap.twill ---
COPY "twill-core-${TWILL_VERSION}.jar" "${WATCHDOG_LIB_DIR}/io.cdap.twill.twill-core-${TWILL_VERSION}.jar"
COPY "twill-api-${TWILL_VERSION}.jar" "${WATCHDOG_LIB_DIR}/io.cdap.twill.twill-api-${TWILL_VERSION}.jar"
COPY "twill-common-${TWILL_VERSION}.jar" "${WATCHDOG_LIB_DIR}/io.cdap.twill.twill-common-${TWILL_VERSION}.jar"
COPY "twill-discovery-api-${TWILL_VERSION}.jar" "${WATCHDOG_LIB_DIR}/io.cdap.twill.twill-discovery-api-${TWILL_VERSION}.jar"
COPY "twill-ext-${TWILL_VERSION}.jar" "${WATCHDOG_LIB_DIR}/io.cdap.twill.twill-ext-${TWILL_VERSION}.jar"
COPY "twill-yarn-${TWILL_VERSION}.jar" "${WATCHDOG_LIB_DIR}/io.cdap.twill.twill-yarn-${TWILL_VERSION}.jar"
COPY "twill-zookeeper-${TWILL_VERSION}.jar" "${WATCHDOG_LIB_DIR}/io.cdap.twill.twill-zookeeper-${TWILL_VERSION}.jar"
COPY "twill-discovery-core-${TWILL_VERSION}.jar" "${WATCHDOG_LIB_DIR}/io.cdap.twill.twill-discovery-core-${TWILL_VERSION}.jar"

# --- io.cdap.common ---
COPY "common-cli-${CDAP_COMMON_VERSION}.jar" "${WATCHDOG_LIB_DIR}/io.cdap.common.common-cli-${CDAP_COMMON_VERSION}.jar"
COPY "common-core-${CDAP_COMMON_VERSION}.jar" "${WATCHDOG_LIB_DIR}/io.cdap.common.common-core-${CDAP_COMMON_VERSION}.jar"
COPY "common-http-${CDAP_COMMON_VERSION}.jar" "${WATCHDOG_LIB_DIR}/io.cdap.common.common-http-${CDAP_COMMON_VERSION}.jar"
COPY "common-io-${CDAP_COMMON_VERSION}.jar" "${WATCHDOG_LIB_DIR}/io.cdap.common.common-io-${CDAP_COMMON_VERSION}.jar"
COPY "common-lang-${CDAP_COMMON_VERSION}.jar" "${WATCHDOG_LIB_DIR}/io.cdap.common.common-lang-${CDAP_COMMON_VERSION}.jar"

# --- Third-party (/opt/cdap/master/lib) ---
# Stage guava-32.0.0-jre.jar into the build context first:
#   cp ~/.m2/repository/com/google/guava/guava/32.0.0-jre/guava-32.0.0-jre.jar .
COPY "guava-${GUAVA_VERSION}.jar" "${WATCHDOG_LIB_DIR}/com.google.guava.guava-${GUAVA_VERSION}.jar"

# --- Isolated extensions (/opt/cdap/master/ext/*) ---
# 1. Storage Provider: gcp-spanner
COPY "cdap-storage-ext-spanner/target/cdap-storage-ext-spanner-${CDAP_VERSION}.jar" "${EXT_DIR}/storageproviders/gcp-spanner/io.cdap.cdap.cdap-storage-ext-spanner-${CDAP_VERSION}.jar"

# 2. Messaging Provider: gcp-spanner
COPY "cdap-messaging-ext-spanner/target/cdap-messaging-ext-spanner-${CDAP_VERSION}.jar" "${EXT_DIR}/messagingproviders/gcp-spanner/io.cdap.cdap.cdap-messaging-ext-spanner-${CDAP_VERSION}.jar"
COPY "cdap-proto/target/cdap-proto-${CDAP_VERSION}.jar" "${EXT_DIR}/messagingproviders/gcp-spanner/io.cdap.cdap.cdap-proto-${CDAP_VERSION}.jar"
COPY "cdap-messaging-spi/target/cdap-messaging-spi-${CDAP_VERSION}.jar" "${EXT_DIR}/messagingproviders/gcp-spanner/io.cdap.cdap.cdap-messaging-spi-${CDAP_VERSION}.jar"
COPY "cdap-api/target/cdap-api-${CDAP_VERSION}.jar" "${EXT_DIR}/messagingproviders/gcp-spanner/io.cdap.cdap.cdap-api-${CDAP_VERSION}.jar"
COPY "cdap-error-api/target/cdap-error-api-${CDAP_VERSION}.jar" "${EXT_DIR}/messagingproviders/gcp-spanner/io.cdap.cdap.cdap-error-api-${CDAP_VERSION}.jar"
COPY "cdap-api-common/target/cdap-api-common-${CDAP_VERSION}.jar" "${EXT_DIR}/messagingproviders/gcp-spanner/io.cdap.cdap.cdap-api-common-${CDAP_VERSION}.jar"
COPY "cdap-security-spi/target/cdap-security-spi-${CDAP_VERSION}.jar" "${EXT_DIR}/messagingproviders/gcp-spanner/io.cdap.cdap.cdap-security-spi-${CDAP_VERSION}.jar"
COPY "cdap-runtime-spi/target/cdap-runtime-spi-${CDAP_VERSION}.jar" "${EXT_DIR}/messagingproviders/gcp-spanner/io.cdap.cdap.cdap-runtime-spi-${CDAP_VERSION}.jar"
COPY "twill-api-${TWILL_VERSION}.jar" "${EXT_DIR}/messagingproviders/gcp-spanner/io.cdap.twill.twill-api-${TWILL_VERSION}.jar"
COPY "twill-common-${TWILL_VERSION}.jar" "${EXT_DIR}/messagingproviders/gcp-spanner/io.cdap.twill.twill-common-${TWILL_VERSION}.jar"
COPY "twill-discovery-api-${TWILL_VERSION}.jar" "${EXT_DIR}/messagingproviders/gcp-spanner/io.cdap.twill.twill-discovery-api-${TWILL_VERSION}.jar"

# 3. Master Environment: k8s
COPY "cdap-kubernetes/target/cdap-kubernetes-${CDAP_VERSION}.jar" "${EXT_DIR}/environments/k8s/io.cdap.cdap.cdap-kubernetes-${CDAP_VERSION}.jar"
COPY "cdap-proto/target/cdap-proto-${CDAP_VERSION}.jar" "${EXT_DIR}/environments/k8s/io.cdap.cdap.cdap-proto-${CDAP_VERSION}.jar"
COPY "cdap-api/target/cdap-api-${CDAP_VERSION}.jar" "${EXT_DIR}/environments/k8s/io.cdap.cdap.cdap-api-${CDAP_VERSION}.jar"
COPY "cdap-error-api/target/cdap-error-api-${CDAP_VERSION}.jar" "${EXT_DIR}/environments/k8s/io.cdap.cdap.cdap-error-api-${CDAP_VERSION}.jar"
COPY "cdap-api-common/target/cdap-api-common-${CDAP_VERSION}.jar" "${EXT_DIR}/environments/k8s/io.cdap.cdap.cdap-api-common-${CDAP_VERSION}.jar"
COPY "cdap-runtime-spi/target/cdap-runtime-spi-${CDAP_VERSION}.jar" "${EXT_DIR}/environments/k8s/io.cdap.cdap.cdap-runtime-spi-${CDAP_VERSION}.jar"
COPY "twill-core-${TWILL_VERSION}.jar" "${EXT_DIR}/environments/k8s/io.cdap.twill.twill-core-${TWILL_VERSION}.jar"
COPY "guava-${GUAVA_VERSION}.jar" "${EXT_DIR}/environments/k8s/com.google.guava.guava-${GUAVA_VERSION}.jar"

# 4. Authenticators, Credential Providers, Encryption, Operations, Secure Stores
COPY "cdap-authenticator-ext-gcp/target/cdap-authenticator-ext-gcp-${CDAP_VERSION}.jar" "${EXT_DIR}/authenticators/gcp-remote-authenticator/io.cdap.cdap.cdap-authenticator-ext-gcp-${CDAP_VERSION}.jar"
COPY "guava-${GUAVA_VERSION}.jar" "${EXT_DIR}/authenticators/gcp-remote-authenticator/com.google.guava.guava-${GUAVA_VERSION}.jar"
COPY "cdap-credential-ext-gcp-wi/target/cdap-credential-ext-gcp-wi-${CDAP_VERSION}.jar" "${EXT_DIR}/credentialproviders/gcp-wi-credential-provider/io.cdap.cdap.cdap-credential-ext-gcp-wi-${CDAP_VERSION}.jar"
COPY "guava-${GUAVA_VERSION}.jar" "${EXT_DIR}/credentialproviders/gcp-wi-credential-provider/com.google.guava.guava-${GUAVA_VERSION}.jar"
COPY "cdap-encryption-ext-tink/target/cdap-encryption-ext-tink-${CDAP_VERSION}.jar" "${EXT_DIR}/encryption/tink/io.cdap.cdap.cdap-encryption-ext-tink-${CDAP_VERSION}.jar"
COPY "cdap-operational-stats-core/target/cdap-operational-stats-core-${CDAP_VERSION}.jar" "${EXT_DIR}/operations/core/io.cdap.cdap.cdap-operational-stats-core-${CDAP_VERSION}.jar"
COPY "cdap-securestore-ext-cloudkms/target/cdap-securestore-ext-cloudkms-${CDAP_VERSION}.jar" "${EXT_DIR}/securestores/gcp-cloudkms/io.cdap.cdap.cdap-securestore-ext-cloudkms-${CDAP_VERSION}.jar"
COPY "guava-${GUAVA_VERSION}.jar" "${EXT_DIR}/securestores/gcp-cloudkms/com.google.guava.guava-${GUAVA_VERSION}.jar"
COPY "cdap-securestore-ext-gcp-secretstore/target/cdap-securestore-ext-gcp-secretstore-${CDAP_VERSION}.jar" "${EXT_DIR}/securestores/gcp-secretstore/io.cdap.cdap.cdap-securestore-ext-gcp-secretstore-${CDAP_VERSION}.jar"
COPY "guava-${GUAVA_VERSION}.jar" "${EXT_DIR}/securestores/gcp-secretstore/com.google.guava.guava-${GUAVA_VERSION}.jar"

# 5. Runtime Providers & Spark Runtime
COPY "cdap-runtime-ext-dataproc/target/cdap-runtime-ext-dataproc-${CDAP_VERSION}.jar" "${EXT_DIR}/runtimeproviders/gcp-dataproc/io.cdap.cdap.cdap-runtime-ext-dataproc-${CDAP_VERSION}.jar"
COPY "twill-api-${TWILL_VERSION}.jar" "${EXT_DIR}/runtimeproviders/gcp-dataproc/io.cdap.twill.twill-api-${TWILL_VERSION}.jar"
COPY "twill-zookeeper-${TWILL_VERSION}.jar" "${EXT_DIR}/runtimeproviders/gcp-dataproc/io.cdap.twill.twill-zookeeper-${TWILL_VERSION}.jar"
COPY "twill-core-${TWILL_VERSION}.jar" "${EXT_DIR}/runtimeproviders/gcp-dataproc/io.cdap.twill.twill-core-${TWILL_VERSION}.jar"
COPY "twill-common-${TWILL_VERSION}.jar" "${EXT_DIR}/runtimeproviders/gcp-dataproc/io.cdap.twill.twill-common-${TWILL_VERSION}.jar"
COPY "twill-discovery-core-${TWILL_VERSION}.jar" "${EXT_DIR}/runtimeproviders/gcp-dataproc/io.cdap.twill.twill-discovery-core-${TWILL_VERSION}.jar"
COPY "twill-yarn-${TWILL_VERSION}.jar" "${EXT_DIR}/runtimeproviders/gcp-dataproc/io.cdap.twill.twill-yarn-${TWILL_VERSION}.jar"
COPY "twill-discovery-api-${TWILL_VERSION}.jar" "${EXT_DIR}/runtimeproviders/gcp-dataproc/io.cdap.twill.twill-discovery-api-${TWILL_VERSION}.jar"
COPY "cdap-runtime-ext-emr/target/cdap-runtime-ext-emr-${CDAP_VERSION}.jar" "${EXT_DIR}/runtimeproviders/emr/io.cdap.cdap.cdap-runtime-ext-emr-${CDAP_VERSION}.jar"
COPY "guava-${GUAVA_VERSION}.jar" "${EXT_DIR}/runtimeproviders/emr/com.google.guava.guava-${GUAVA_VERSION}.jar"
COPY "cdap-runtime-ext-remote-hadoop/target/cdap-runtime-ext-remote-hadoop-${CDAP_VERSION}.jar" "${EXT_DIR}/runtimeproviders/remote-hadoop/io.cdap.cdap.cdap-runtime-ext-remote-hadoop-${CDAP_VERSION}.jar"
COPY "guava-${GUAVA_VERSION}.jar" "${EXT_DIR}/runtimeproviders/remote-hadoop/com.google.guava.guava-${GUAVA_VERSION}.jar"
COPY "cdap-spark-python/target/cdap-spark-python-${CDAP_VERSION}.jar" "${EXT_DIR}/runtimes/spark3_2.12/io.cdap.cdap.cdap-spark-python-${CDAP_VERSION}.jar"
COPY "cdap-api-spark3_2.12/target/cdap-api-spark3_2.12-${CDAP_VERSION}.jar" "${EXT_DIR}/runtimes/spark3_2.12/io.cdap.cdap.cdap-api-spark3_2.12-${CDAP_VERSION}.jar"
COPY "cdap-spark-core3_2.12/target/cdap-spark-core3_2.12-${CDAP_VERSION}.jar" "${EXT_DIR}/runtimes/spark3_2.12/io.cdap.cdap.cdap-spark-core3_2.12-${CDAP_VERSION}.jar"
COPY "twill-common-${TWILL_VERSION}.jar" "${EXT_DIR}/metricswriters/gcp-monitoring/io.cdap.twill.twill-common-${TWILL_VERSION}.jar"

# Ensure correct permissions
RUN chmod -R 755 /opt/cdap

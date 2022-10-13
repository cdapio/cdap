/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.runtime.spi.runtimejob;

import org.apache.twill.internal.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Main class that will be called from dataproc driver.
 */
public class DataprocJobMain {

  public static final String RUNTIME_JOB_CLASS = "runtimeJobClass";
  public static final String SPARK_COMPAT = "sparkCompat";
  public static final String ARCHIVE = "archive";
  public static final String PROPERTY_PREFIX = "prop";

  private static final Logger LOG = LoggerFactory.getLogger(DataprocJobMain.class);
  // Hacky way to replicate class loader used by Hadoop job.
  // Spark class loader has extra stuff that interfers with CDAP code.
  private static final String[] HADOOP_JOB_CLASSPATH_URLS = {
    "file:/etc/hadoop/conf.empty/",
    "file:/usr/lib/hadoop/lib/curator-recipes-2.13.0.jar",
    "file:/usr/lib/hadoop/lib/jaxb-impl-2.2.3-1.jar",
    "file:/usr/lib/hadoop/lib/httpclient-4.5.13.jar",
    "file:/usr/lib/hadoop/lib/jsr305-3.0.2.jar",
    "file:/usr/lib/hadoop/lib/kerb-identity-1.0.1.jar",
    "file:/usr/lib/hadoop/lib/gson-2.2.4.jar",
    "file:/usr/lib/hadoop/lib/commons-net-3.6.jar",
    "file:/usr/lib/hadoop/lib/jackson-xc-1.9.13.jar",
    "file:/usr/lib/hadoop/lib/audience-annotations-0.5.0.jar",
    "file:/usr/lib/hadoop/lib/aliyun-java-sdk-sts-3.0.0.jar",
    "file:/usr/lib/hadoop/lib/json-smart-2.3.jar",
    "file:/usr/lib/hadoop/lib/avro-1.9.2.jar",
    "file:/usr/lib/hadoop/lib/j2objc-annotations-1.3.jar",
    "file:/usr/lib/hadoop/lib/token-provider-1.0.1.jar",
    "file:/usr/lib/hadoop/lib/re2j-1.1.jar",
    "file:/usr/lib/hadoop/lib/jackson-core-2.10.5.jar",
    "file:/usr/lib/hadoop/lib/jetty-util-ajax-9.4.40.v20210413.jar",
    "file:/usr/lib/hadoop/lib/woodstox-core-5.0.3.jar",
    "file:/usr/lib/hadoop/lib/error_prone_annotations-2.3.4.jar",
    "file:/usr/lib/hadoop/lib/commons-cli-1.2.jar",
    "file:/usr/lib/hadoop/lib/zookeeper-3.4.14.jar",
    "file:/usr/lib/hadoop/lib/failureaccess-1.0.1.jar",
    "file:/usr/lib/hadoop/lib/netty-3.10.6.Final.jar",
    "file:/usr/lib/hadoop/lib/jettison-1.1.jar",
    "file:/usr/lib/hadoop/lib/jsp-api-2.1.jar",
    "file:/usr/lib/hadoop/lib/commons-compress-1.19.jar",
    "file:/usr/lib/hadoop/lib/guava-28.2-jre.jar",
    "file:/usr/lib/hadoop/lib/azure-data-lake-store-sdk-2.2.9.jar",
    "file:/usr/lib/hadoop/lib/jcip-annotations-1.0-1.jar",
    "file:/usr/lib/hadoop/lib/listenablefuture-9999.0-empty-to-avoid-conflict-with-guava.jar",
    "file:/usr/lib/hadoop/lib/commons-collections-3.2.2.jar",
    "file:/usr/lib/hadoop/lib/kerby-pkix-1.0.1.jar",
    "file:/usr/lib/hadoop/lib/kerb-core-1.0.1.jar",
    "file:/usr/lib/hadoop/lib/kerb-common-1.0.1.jar",
    "file:/usr/lib/hadoop/lib/jersey-core-1.19.jar",
    "file:/usr/lib/hadoop/lib/aliyun-java-sdk-core-3.4.0.jar",
    "file:/usr/lib/hadoop/lib/metrics-core-3.2.4.jar",
    "file:/usr/lib/hadoop/lib/aws-java-sdk-bundle-1.11.563.jar",
    "file:/usr/lib/hadoop/lib/jackson-core-asl-1.9.13.jar",
    "file:/usr/lib/hadoop/lib/kerby-xdr-1.0.1.jar",
    "file:/usr/lib/hadoop/lib/kerb-crypto-1.0.1.jar",
    "file:/usr/lib/hadoop/lib/kerb-simplekdc-1.0.1.jar",
    "file:/usr/lib/hadoop/lib/curator-client-2.13.0.jar",
    "file:/usr/lib/hadoop/lib/ojalgo-43.0.jar",
    "file:/usr/lib/hadoop/lib/log4j-1.2.17.jar",
    "file:/usr/lib/hadoop/lib/jaxb-api-2.2.11.jar",
    "file:/usr/lib/hadoop/lib/slf4j-api-1.7.25.jar",
    "file:/usr/lib/hadoop/lib/jul-to-slf4j-1.7.25.jar",
    "file:/usr/lib/hadoop/lib/protobuf-java-2.5.0.jar",
    "file:/usr/lib/hadoop/lib/jersey-json-1.19.jar",
    "file:/usr/lib/hadoop/lib/jackson-mapper-asl-1.9.13.jar",
    "file:/usr/lib/hadoop/lib/azure-storage-7.0.0.jar",
    "file:/usr/lib/hadoop/lib/accessors-smart-1.2.jar",
    "file:/usr/lib/hadoop/lib/kerb-server-1.0.1.jar",
    "file:/usr/lib/hadoop/lib/asm-5.0.4.jar",
    "file:/usr/lib/hadoop/lib/jetty-io-9.4.40.v20210413.jar",
    "file:/usr/lib/hadoop/lib/htrace-core4-4.1.0-incubating.jar",
    "file:/usr/lib/hadoop/lib/curator-framework-2.13.0.jar",
    "file:/usr/lib/hadoop/lib/commons-lang3-3.10.jar",
    "file:/usr/lib/hadoop/lib/snappy-java-1.1.8.4.jar",
    "file:/usr/lib/hadoop/lib/kerb-client-1.0.1.jar",
    "file:/usr/lib/hadoop/lib/azure-keyvault-core-1.0.0.jar",
    "file:/usr/lib/hadoop/lib/jdom-1.1.jar",
    "file:/usr/lib/hadoop/lib/jetty-server-9.4.40.v20210413.jar",
    "file:/usr/lib/hadoop/lib/jsr311-api-1.1.1.jar",
    "file:/usr/lib/hadoop/lib/checker-qual-2.10.0.jar",
    "file:/usr/lib/hadoop/lib/jackson-databind-2.10.5.jar",
    "file:/usr/lib/hadoop/lib/jetty-security-9.4.40.v20210413.jar",
    "file:/usr/lib/hadoop/lib/httpcore-4.4.13.jar",
    "file:/usr/lib/hadoop/lib/javax.servlet-api-3.1.0.jar",
    "file:/usr/lib/hadoop/lib/stax2-api-3.1.4.jar",
    "file:/usr/lib/hadoop/lib/hadoop-lzo-0.4.20.jar",
    "file:/usr/lib/hadoop/lib/commons-codec-1.11.jar",
    "file:/usr/lib/hadoop/lib/jersey-servlet-1.19.jar",
    "file:/usr/lib/hadoop/lib/jetty-http-9.4.40.v20210413.jar",
    "file:/usr/lib/hadoop/lib/lz4-1.2.0.jar",
    "file:/usr/lib/hadoop/lib/kerby-util-1.0.1.jar",
    "file:/usr/lib/hadoop/lib/jetty-servlet-9.4.40.v20210413.jar",
    "file:/usr/lib/hadoop/lib/commons-configuration2-2.1.1.jar",
    "file:/usr/lib/hadoop/lib/commons-math3-3.1.1.jar",
    "file:/usr/lib/hadoop/lib/dnsjava-2.1.7.jar",
    "file:/usr/lib/hadoop/lib/jackson-annotations-2.10.5.jar",
    "file:/usr/lib/hadoop/lib/commons-io-2.5.jar",
    "file:/usr/lib/hadoop/lib/kerb-admin-1.0.1.jar",
    "file:/usr/lib/hadoop/lib/commons-text-1.4.jar",
    "file:/usr/lib/hadoop/lib/javax.activation-api-1.2.0.jar",
    "file:/usr/lib/hadoop/lib/aliyun-java-sdk-ram-3.0.0.jar",
    "file:/usr/lib/hadoop/lib/jetty-util-9.4.40.v20210413.jar",
    "file:/usr/lib/hadoop/lib/nimbus-jose-jwt-7.9.jar",
    "file:/usr/lib/hadoop/lib/commons-lang-2.6.jar",
    "file:/usr/lib/hadoop/lib/kerby-config-1.0.1.jar",
    "file:/usr/lib/hadoop/lib/kerby-asn1-1.0.1.jar",
    "file:/usr/lib/hadoop/lib/kafka-clients-0.8.2.1.jar",
    "file:/usr/lib/hadoop/lib/jersey-server-1.19.jar",
    "file:/usr/lib/hadoop/lib/aliyun-java-sdk-ecs-4.2.0.jar",
    "file:/usr/lib/hadoop/lib/wildfly-openssl-1.0.7.Final.jar",
    "file:/usr/lib/hadoop/lib/commons-logging-1.1.3.jar",
    "file:/usr/lib/hadoop/lib/aliyun-sdk-oss-3.4.1.jar",
    "file:/usr/lib/hadoop/lib/jsch-0.1.55.jar",
    "file:/usr/lib/hadoop/lib/kerb-util-1.0.1.jar",
    "file:/usr/lib/hadoop/lib/jackson-jaxrs-1.9.13.jar",
    "file:/usr/lib/hadoop/lib/slf4j-log4j12-1.7.25.jar",
    "file:/usr/lib/hadoop/lib/jetty-xml-9.4.40.v20210413.jar",
    "file:/usr/lib/hadoop/lib/commons-beanutils-1.9.4.jar",
    "file:/usr/lib/hadoop/lib/jetty-webapp-9.4.40.v20210413.jar",
    "file:/usr/lib/hadoop/hadoop-annotations-3.2.2.jar",
    "file:/usr/lib/hadoop/hadoop-archive-logs-3.2.2.jar",
    "file:/usr/lib/hadoop/hadoop-kafka-3.2.2.jar",
    "file:/usr/lib/hadoop/hadoop-rumen-3.2.2.jar",
    "file:/usr/lib/hadoop/hadoop-auth-3.2.2.jar",
    "file:/usr/lib/hadoop/hadoop-common-3.2.2-tests.jar",
    "file:/usr/lib/hadoop/hadoop-azure-datalake-3.2.2.jar",
    "file:/usr/lib/hadoop/hadoop-fs2img-3.2.2.jar",
    "file:/usr/lib/hadoop/hadoop-gridmix-3.2.2.jar",
    "file:/usr/lib/hadoop/hadoop-archives-3.2.2.jar",
    "file:/usr/lib/hadoop/hadoop-common-3.2.2.jar",
    "file:/usr/lib/hadoop/hadoop-streaming-3.2.2.jar",
    "file:/usr/lib/hadoop/hadoop-gridmix-3.2.2.jar",
    "file:/usr/lib/hadoop/hadoop-archive-logs-3.2.2.jar",
    "file:/usr/lib/hadoop/hadoop-common-3.2.2.jar",
    "file:/usr/lib/hadoop/hadoop-nfs-3.2.2.jar",
    "file:/usr/lib/hadoop/hadoop-distcp-3.2.2.jar",
    "file:/usr/lib/hadoop/hadoop-kms-3.2.2.jar",
    "file:/usr/lib/hadoop/hadoop-azure-3.2.2.jar",
    "file:/usr/lib/hadoop/hadoop-rumen-3.2.2.jar",
    "file:/usr/lib/hadoop/hadoop-aws-3.2.2.jar",
    "file:/usr/lib/hadoop/hadoop-azure-datalake-3.2.2.jar",
    "file:/usr/lib/hadoop/hadoop-fs2img-3.2.2.jar",
    "file:/usr/lib/hadoop/hadoop-datajoin-3.2.2.jar",
    "file:/usr/lib/hadoop/hadoop-extras-3.2.2.jar",
    "file:/usr/lib/hadoop/hadoop-nfs-3.2.2.jar",
    "file:/usr/lib/hadoop/hadoop-sls-3.2.2.jar",
    "file:/usr/lib/hadoop/hadoop-sls-3.2.2.jar",
    "file:/usr/lib/hadoop/hadoop-kafka-3.2.2.jar",
    "file:/usr/lib/hadoop/hadoop-azure-3.2.2.jar",
    "file:/usr/lib/hadoop/hadoop-resourceestimator-3.2.2.jar",
    "file:/usr/lib/hadoop/hadoop-datajoin-3.2.2.jar",
    "file:/usr/lib/hadoop/hadoop-resourceestimator-3.2.2.jar",
    "file:/usr/lib/hadoop/hadoop-auth-3.2.2.jar",
    "file:/usr/lib/hadoop/hadoop-annotations-3.2.2.jar",
    "file:/usr/lib/hadoop/hadoop-distcp-3.2.2.jar",
    "file:/usr/lib/hadoop/hadoop-aliyun-3.2.2.jar",
    "file:/usr/lib/hadoop/hadoop-aliyun-3.2.2.jar",
    "file:/usr/lib/hadoop/hadoop-streaming-3.2.2.jar",
    "file:/usr/lib/hadoop/hadoop-openstack-3.2.2.jar",
    "file:/usr/lib/hadoop/hadoop-aws-3.2.2.jar",
    "file:/usr/lib/hadoop/hadoop-kms-3.2.2.jar",
    "file:/usr/lib/hadoop/hadoop-archives-3.2.2.jar",
    "file:/usr/lib/hadoop/hadoop-openstack-3.2.2.jar",
    "file:/usr/lib/hadoop/hadoop-extras-3.2.2.jar",
    "file:/usr/lib/hadoop-hdfs/",
    "file:/usr/lib/hadoop-hdfs/lib/curator-recipes-2.13.0.jar",
    "file:/usr/lib/hadoop-hdfs/lib/json-simple-1.1.1.jar",
    "file:/usr/lib/hadoop-hdfs/lib/jaxb-impl-2.2.3-1.jar",
    "file:/usr/lib/hadoop-hdfs/lib/httpclient-4.5.13.jar",
    "file:/usr/lib/hadoop-hdfs/lib/jsr305-3.0.2.jar",
    "file:/usr/lib/hadoop-hdfs/lib/kerb-identity-1.0.1.jar",
    "file:/usr/lib/hadoop-hdfs/lib/gson-2.2.4.jar",
    "file:/usr/lib/hadoop-hdfs/lib/commons-net-3.6.jar",
    "file:/usr/lib/hadoop-hdfs/lib/jackson-xc-1.9.13.jar",
    "file:/usr/lib/hadoop-hdfs/lib/audience-annotations-0.5.0.jar",
    "file:/usr/lib/hadoop-hdfs/lib/json-smart-2.3.jar",
    "file:/usr/lib/hadoop-hdfs/lib/avro-1.9.2.jar",
    "file:/usr/lib/hadoop-hdfs/lib/j2objc-annotations-1.3.jar",
    "file:/usr/lib/hadoop-hdfs/lib/token-provider-1.0.1.jar",
    "file:/usr/lib/hadoop-hdfs/lib/re2j-1.1.jar",
    "file:/usr/lib/hadoop-hdfs/lib/jackson-core-2.10.5.jar",
    "file:/usr/lib/hadoop-hdfs/lib/jetty-util-ajax-9.4.40.v20210413.jar",
    "file:/usr/lib/hadoop-hdfs/lib/woodstox-core-5.0.3.jar",
    "file:/usr/lib/hadoop-hdfs/lib/error_prone_annotations-2.3.4.jar",
    "file:/usr/lib/hadoop-hdfs/lib/commons-cli-1.2.jar",
    "file:/usr/lib/hadoop-hdfs/lib/zookeeper-3.4.14.jar",
    "file:/usr/lib/hadoop-hdfs/lib/failureaccess-1.0.1.jar",
    "file:/usr/lib/hadoop-hdfs/lib/netty-3.10.6.Final.jar",
    "file:/usr/lib/hadoop-hdfs/lib/jettison-1.1.jar",
    "file:/usr/lib/hadoop-hdfs/lib/commons-compress-1.19.jar",
    "file:/usr/lib/hadoop-hdfs/lib/guava-28.2-jre.jar",
    "file:/usr/lib/hadoop-hdfs/lib/jcip-annotations-1.0-1.jar",
    "file:/usr/lib/hadoop-hdfs/lib/listenablefuture-9999.0-empty-to-avoid-conflict-with-guava.jar",
    "file:/usr/lib/hadoop-hdfs/lib/commons-collections-3.2.2.jar",
    "file:/usr/lib/hadoop-hdfs/lib/kerby-pkix-1.0.1.jar",
    "file:/usr/lib/hadoop-hdfs/lib/kerb-core-1.0.1.jar",
    "file:/usr/lib/hadoop-hdfs/lib/kerb-common-1.0.1.jar",
    "file:/usr/lib/hadoop-hdfs/lib/jersey-core-1.19.jar",
    "file:/usr/lib/hadoop-hdfs/lib/jackson-core-asl-1.9.13.jar",
    "file:/usr/lib/hadoop-hdfs/lib/kerby-xdr-1.0.1.jar",
    "file:/usr/lib/hadoop-hdfs/lib/kerb-crypto-1.0.1.jar",
    "file:/usr/lib/hadoop-hdfs/lib/kerb-simplekdc-1.0.1.jar",
    "file:/usr/lib/hadoop-hdfs/lib/curator-client-2.13.0.jar",
    "file:/usr/lib/hadoop-hdfs/lib/log4j-1.2.17.jar",
    "file:/usr/lib/hadoop-hdfs/lib/jaxb-api-2.2.11.jar",
    "file:/usr/lib/hadoop-hdfs/lib/protobuf-java-2.5.0.jar",
    "file:/usr/lib/hadoop-hdfs/lib/okhttp-2.7.5.jar",
    "file:/usr/lib/hadoop-hdfs/lib/jersey-json-1.19.jar",
    "file:/usr/lib/hadoop-hdfs/lib/jackson-mapper-asl-1.9.13.jar",
    "file:/usr/lib/hadoop-hdfs/lib/accessors-smart-1.2.jar",
    "file:/usr/lib/hadoop-hdfs/lib/kerb-server-1.0.1.jar",
    "file:/usr/lib/hadoop-hdfs/lib/asm-5.0.4.jar",
    "file:/usr/lib/hadoop-hdfs/lib/jetty-io-9.4.40.v20210413.jar",
    "file:/usr/lib/hadoop-hdfs/lib/htrace-core4-4.1.0-incubating.jar",
    "file:/usr/lib/hadoop-hdfs/lib/curator-framework-2.13.0.jar",
    "file:/usr/lib/hadoop-hdfs/lib/commons-lang3-3.10.jar",
    "file:/usr/lib/hadoop-hdfs/lib/snappy-java-1.1.8.4.jar",
    "file:/usr/lib/hadoop-hdfs/lib/kerb-client-1.0.1.jar",
    "file:/usr/lib/hadoop-hdfs/lib/commons-daemon-1.0.13.jar",
    "file:/usr/lib/hadoop-hdfs/lib/jetty-server-9.4.40.v20210413.jar",
    "file:/usr/lib/hadoop-hdfs/lib/okio-1.6.0.jar",
    "file:/usr/lib/hadoop-hdfs/lib/netty-all-4.1.48.Final.jar",
    "file:/usr/lib/hadoop-hdfs/lib/jsr311-api-1.1.1.jar",
    "file:/usr/lib/hadoop-hdfs/lib/checker-qual-2.10.0.jar",
    "file:/usr/lib/hadoop-hdfs/lib/jackson-databind-2.10.5.jar",
    "file:/usr/lib/hadoop-hdfs/lib/jetty-security-9.4.40.v20210413.jar",
    "file:/usr/lib/hadoop-hdfs/lib/httpcore-4.4.13.jar",
    "file:/usr/lib/hadoop-hdfs/lib/javax.servlet-api-3.1.0.jar",
    "file:/usr/lib/hadoop-hdfs/lib/stax2-api-3.1.4.jar",
    "file:/usr/lib/hadoop-hdfs/lib/commons-codec-1.11.jar",
    "file:/usr/lib/hadoop-hdfs/lib/jersey-servlet-1.19.jar",
    "file:/usr/lib/hadoop-hdfs/lib/jetty-http-9.4.40.v20210413.jar",
    "file:/usr/lib/hadoop-hdfs/lib/kerby-util-1.0.1.jar",
    "file:/usr/lib/hadoop-hdfs/lib/jetty-servlet-9.4.40.v20210413.jar",
    "file:/usr/lib/hadoop-hdfs/lib/commons-configuration2-2.1.1.jar",
    "file:/usr/lib/hadoop-hdfs/lib/commons-math3-3.1.1.jar",
    "file:/usr/lib/hadoop-hdfs/lib/dnsjava-2.1.7.jar",
    "file:/usr/lib/hadoop-hdfs/lib/jackson-annotations-2.10.5.jar",
    "file:/usr/lib/hadoop-hdfs/lib/commons-io-2.5.jar",
    "file:/usr/lib/hadoop-hdfs/lib/kerb-admin-1.0.1.jar",
    "file:/usr/lib/hadoop-hdfs/lib/commons-text-1.4.jar",
    "file:/usr/lib/hadoop-hdfs/lib/javax.activation-api-1.2.0.jar",
    "file:/usr/lib/hadoop-hdfs/lib/jetty-util-9.4.40.v20210413.jar",
    "file:/usr/lib/hadoop-hdfs/lib/nimbus-jose-jwt-7.9.jar",
    "file:/usr/lib/hadoop-hdfs/lib/commons-lang-2.6.jar",
    "file:/usr/lib/hadoop-hdfs/lib/kerby-config-1.0.1.jar",
    "file:/usr/lib/hadoop-hdfs/lib/kerby-asn1-1.0.1.jar",
    "file:/usr/lib/hadoop-hdfs/lib/leveldbjni-all-1.8.jar",
    "file:/usr/lib/hadoop-hdfs/lib/jersey-server-1.19.jar",
    "file:/usr/lib/hadoop-hdfs/lib/commons-logging-1.1.3.jar",
    "file:/usr/lib/hadoop-hdfs/lib/jsch-0.1.55.jar",
    "file:/usr/lib/hadoop-hdfs/lib/kerb-util-1.0.1.jar",
    "file:/usr/lib/hadoop-hdfs/lib/jackson-jaxrs-1.9.13.jar",
    "file:/usr/lib/hadoop-hdfs/lib/jetty-xml-9.4.40.v20210413.jar",
    "file:/usr/lib/hadoop-hdfs/lib/commons-beanutils-1.9.4.jar",
    "file:/usr/lib/hadoop-hdfs/lib/jetty-webapp-9.4.40.v20210413.jar",
    "file:/usr/lib/hadoop-hdfs/hadoop-hdfs-rbf-3.2.2.jar",
    "file:/usr/lib/hadoop-hdfs/hadoop-hdfs-3.2.2-tests.jar",
    "file:/usr/lib/hadoop-hdfs/hadoop-hdfs-rbf-3.2.2-tests.jar",
    "file:/usr/lib/hadoop-hdfs/hadoop-hdfs-nfs-3.2.2.jar",
    "file:/usr/lib/hadoop-hdfs/hadoop-hdfs-client-3.2.2-tests.jar",
    "file:/usr/lib/hadoop-hdfs/hadoop-hdfs-3.2.2.jar",
    "file:/usr/lib/hadoop-hdfs/hadoop-hdfs-httpfs-3.2.2.jar",
    "file:/usr/lib/hadoop-hdfs/hadoop-hdfs-native-client-3.2.2-tests.jar",
    "file:/usr/lib/hadoop-hdfs/hadoop-hdfs-nfs-3.2.2.jar",
    "file:/usr/lib/hadoop-hdfs/hadoop-hdfs-rbf-3.2.2.jar",
    "file:/usr/lib/hadoop-hdfs/hadoop-hdfs-client-3.2.2.jar",
    "file:/usr/lib/hadoop-hdfs/hadoop-hdfs-httpfs-3.2.2.jar",
    "file:/usr/lib/hadoop-hdfs/hadoop-hdfs-native-client-3.2.2.jar",
    "file:/usr/lib/hadoop-hdfs/hadoop-hdfs-client-3.2.2.jar",
    "file:/usr/lib/hadoop-hdfs/hadoop-hdfs-3.2.2.jar",
    "file:/usr/lib/hadoop-hdfs/hadoop-hdfs-native-client-3.2.2.jar",
    "file:/usr/lib/hadoop-mapreduce/hadoop-mapreduce-client-core-3.2.2.jar",
    "file:/usr/lib/hadoop-mapreduce/hadoop-mapreduce-client-hs-plugins-3.2.2.jar",
    "file:/usr/lib/hadoop-mapreduce/hadoop-mapreduce-client-nativetask-3.2.2.jar",
    "file:/usr/lib/hadoop-mapreduce/hadoop-mapreduce-client-core-3.2.2.jar",
    "file:/usr/lib/hadoop-mapreduce/hadoop-mapreduce-client-shuffle-3.2.2.jar",
    "file:/usr/lib/hadoop-mapreduce/hadoop-mapreduce-client-nativetask-3.2.2.jar",
    "file:/usr/lib/hadoop-mapreduce/hadoop-mapreduce-client-jobclient-3.2.2.jar",
    "file:/usr/lib/hadoop-mapreduce/hadoop-mapreduce-client-shuffle-3.2.2.jar",
    "file:/usr/lib/hadoop-mapreduce/hadoop-mapreduce-client-jobclient-3.2.2.jar",
    "file:/usr/lib/hadoop-mapreduce/hadoop-mapreduce-client-app-3.2.2.jar",
    "file:/usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples-3.2.2.jar",
    "file:/usr/lib/hadoop-mapreduce/hadoop-mapreduce-client-uploader-3.2.2.jar",
    "file:/usr/lib/hadoop-mapreduce/hadoop-mapreduce-client-hs-3.2.2.jar",
    "file:/usr/lib/hadoop-mapreduce/hadoop-mapreduce-client-common-3.2.2.jar",
    "file:/usr/lib/hadoop-mapreduce/hadoop-mapreduce-client-uploader-3.2.2.jar",
    "file:/usr/lib/hadoop-mapreduce/hadoop-mapreduce-client-hs-3.2.2.jar",
    "file:/usr/lib/hadoop-mapreduce/hadoop-mapreduce-client-hs-plugins-3.2.2.jar",
    "file:/usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples-3.2.2.jar",
    "file:/usr/lib/hadoop-mapreduce/hadoop-mapreduce-client-app-3.2.2.jar",
    "file:/usr/lib/hadoop-mapreduce/hadoop-mapreduce-client-common-3.2.2.jar",
    "file:/usr/lib/hadoop-mapreduce/hadoop-mapreduce-client-jobclient-3.2.2-tests.jar",
    "file:/usr/lib/hadoop-yarn/lib/HikariCP-java7-2.4.12.jar",
    "file:/usr/lib/hadoop-yarn/lib/json-io-2.5.1.jar",
    "file:/usr/lib/hadoop-yarn/lib/java-util-1.9.0.jar",
    "file:/usr/lib/hadoop-yarn/lib/mssql-jdbc-6.2.1.jre7.jar",
    "file:/usr/lib/hadoop-yarn/lib/jackson-jaxrs-base-2.10.5.jar",
    "file:/usr/lib/hadoop-yarn/lib/jakarta.xml.bind-api-2.3.2.jar",
    "file:/usr/lib/hadoop-yarn/lib/jersey-guice-1.19.jar",
    "file:/usr/lib/hadoop-yarn/lib/metrics-core-3.2.4.jar",
    "file:/usr/lib/hadoop-yarn/lib/guice-4.0.jar",
    "file:/usr/lib/hadoop-yarn/lib/jakarta.activation-api-1.2.1.jar",
    "file:/usr/lib/hadoop-yarn/lib/jersey-client-1.19.jar",
    "file:/usr/lib/hadoop-yarn/lib/geronimo-jcache_1.0_spec-1.0-alpha-1.jar",
    "file:/usr/lib/hadoop-yarn/lib/objenesis-1.0.jar",
    "file:/usr/lib/hadoop-yarn/lib/bcprov-jdk15on-1.60.jar",
    "file:/usr/lib/hadoop-yarn/lib/snakeyaml-1.16.jar",
    "file:/usr/lib/hadoop-yarn/lib/guice-servlet-4.0.jar",
    "file:/usr/lib/hadoop-yarn/lib/bcpkix-jdk15on-1.60.jar",
    "file:/usr/lib/hadoop-yarn/lib/ehcache-3.3.1.jar",
    "file:/usr/lib/hadoop-yarn/lib/javax.inject-1.jar",
    "file:/usr/lib/hadoop-yarn/lib/swagger-annotations-1.5.4.jar",
    "file:/usr/lib/hadoop-yarn/lib/jackson-jaxrs-json-provider-2.10.5.jar",
    "file:/usr/lib/hadoop-yarn/lib/fst-2.50.jar",
    "file:/usr/lib/hadoop-yarn/lib/aopalliance-1.0.jar",
    "file:/usr/lib/hadoop-yarn/lib/jackson-module-jaxb-annotations-2.10.5.jar",
    "file:/usr/lib/hadoop-yarn/hadoop-yarn-server-web-proxy-3.2.2.jar",
    "file:/usr/lib/hadoop-yarn/hadoop-yarn-submarine-3.2.2.jar",
    "file:/usr/lib/hadoop-yarn/hadoop-yarn-applications-distributedshell-3.2.2.jar",
    "file:/usr/lib/hadoop-yarn/hadoop-yarn-server-router-3.2.2.jar",
    "file:/usr/lib/hadoop-yarn/hadoop-yarn-server-applicationhistoryservice-3.2.2.jar",
    "file:/usr/lib/hadoop-yarn/hadoop-yarn-server-timeline-pluginstorage-3.2.2.jar",
    "file:/usr/lib/hadoop-yarn/hadoop-yarn-server-common-3.2.2.jar",
    "file:/usr/lib/hadoop-yarn/hadoop-yarn-services-core-3.2.2.jar",
    "file:/usr/lib/hadoop-yarn/hadoop-yarn-services-api-3.2.2.jar",
    "file:/usr/lib/hadoop-yarn/hadoop-yarn-api-3.2.2.jar",
    "file:/usr/lib/hadoop-yarn/hadoop-yarn-server-tests-3.2.2.jar",
    "file:/usr/lib/hadoop-yarn/hadoop-yarn-client-3.2.2.jar",
    "file:/usr/lib/hadoop-yarn/hadoop-yarn-server-applicationhistoryservice-3.2.2.jar",
    "file:/usr/lib/hadoop-yarn/hadoop-yarn-applications-unmanaged-am-launcher-3.2.2.jar",
    "file:/usr/lib/hadoop-yarn/hadoop-yarn-server-timeline-pluginstorage-3.2.2.jar",
    "file:/usr/lib/hadoop-yarn/hadoop-yarn-registry-3.2.2.jar",
    "file:/usr/lib/hadoop-yarn/hadoop-yarn-services-api-3.2.2.jar",
    "file:/usr/lib/hadoop-yarn/hadoop-yarn-services-core-3.2.2.jar",
    "file:/usr/lib/hadoop-yarn/hadoop-yarn-applications-unmanaged-am-launcher-3.2.2.jar",
    "file:/usr/lib/hadoop-yarn/hadoop-yarn-server-tests-3.2.2.jar",
    "file:/usr/lib/hadoop-yarn/hadoop-yarn-api-3.2.2.jar",
    "file:/usr/lib/hadoop-yarn/hadoop-yarn-common-3.2.2.jar",
    "file:/usr/lib/hadoop-yarn/hadoop-yarn-submarine-3.2.2.jar",
    "file:/usr/lib/hadoop-yarn/hadoop-yarn-server-web-proxy-3.2.2.jar",
    "file:/usr/lib/hadoop-yarn/hadoop-yarn-applications-distributedshell-3.2.2.jar",
    "file:/usr/lib/hadoop-yarn/hadoop-yarn-registry-3.2.2.jar",
    "file:/usr/lib/hadoop-yarn/hadoop-yarn-server-common-3.2.2.jar",
    "file:/usr/lib/hadoop-yarn/hadoop-yarn-server-sharedcachemanager-3.2.2.jar",
    "file:/usr/lib/hadoop-yarn/hadoop-yarn-server-nodemanager-3.2.2.jar",
    "file:/usr/lib/hadoop-yarn/hadoop-yarn-client-3.2.2.jar",
    "file:/usr/lib/hadoop-yarn/hadoop-yarn-common-3.2.2.jar",
    "file:/usr/lib/hadoop-yarn/hadoop-yarn-server-sharedcachemanager-3.2.2.jar",
    "file:/usr/lib/hadoop-yarn/hadoop-yarn-server-router-3.2.2.jar",
    "file:/usr/lib/hadoop-yarn/hadoop-yarn-server-resourcemanager-3.2.2.jar",
    "file:/usr/lib/hadoop-yarn/hadoop-yarn-server-resourcemanager-3.2.2.jar",
    "file:/usr/lib/hadoop-yarn/hadoop-yarn-server-nodemanager-3.2.2.jar",
    "file:/usr/local/share/google/dataproc/lib/gcs-connector-hadoop3-2.2.6.jar",
    "file:/usr/local/share/google/dataproc/lib/gcs-connector-hadoop3-2.2.6.jar",
    "file:/usr/local/share/google/dataproc/lib/spark-metrics-listener-1.0.1.jar",
    "file:/usr/local/share/google/dataproc/lib/spark-metrics-listener-1.0.1.jar",
  };

  /**
   * Main method to setup classpath and call the RuntimeJob.run() method.
   *
   * @param args the name of implementation of RuntimeJob class
   * @throws Exception any exception while running the job
   */
  public static void main(String[] args) throws Exception {
    Map<String, Collection<String>> arguments = fromPosixArray(args);

    if (!arguments.containsKey(RUNTIME_JOB_CLASS)) {
      throw new RuntimeException("Missing --" + RUNTIME_JOB_CLASS + " argument for the RuntimeJob classname");
    }
    if (!arguments.containsKey(SPARK_COMPAT)) {
      throw new RuntimeException("Missing --" + SPARK_COMPAT + " argument for the spark compat version");
    }

    Thread.setDefaultUncaughtExceptionHandler((t, e) -> LOG.error("Uncaught exception from thread {}", t, e));

    // Get the Java properties
    for (Map.Entry<String, Collection<String>> entry : arguments.entrySet()) {
      if (entry.getKey().startsWith(PROPERTY_PREFIX)) {
        System.setProperty(entry.getKey().substring(PROPERTY_PREFIX.length()), entry.getValue().iterator().next());
      }
    }

    // expand archive jars. This is needed because of CDAP-16456
    expandArchives(arguments.getOrDefault(ARCHIVE, Collections.emptySet()));

    String runtimeJobClassName = arguments.get(RUNTIME_JOB_CLASS).iterator().next();
    String sparkCompat = arguments.get(SPARK_COMPAT).iterator().next();

    ClassLoader cl = DataprocJobMain.class.getClassLoader();
    if (!(cl instanceof URLClassLoader)) {
      throw new RuntimeException("Classloader is expected to be an instance of URLClassLoader");
    }

    ArrayList<URL> haddopURLs = new ArrayList<>();
    for (String url : HADOOP_JOB_CLASSPATH_URLS) {
      try {
        URL u = new URL(url);
        haddopURLs.add(u);
      } catch (Exception e) {
        LOG.error("Exception while casting Hadoop job urls.", e);
      }
    }
    URLClassLoader hadoopClassLoader = new URLClassLoader(
      haddopURLs.toArray(new URL[0]),
      ClassLoader.getSystemClassLoader().getParent());

    // create classpath from resources, application and twill jars
    URL[] urls = getClasspath(hadoopClassLoader,
                              (URLClassLoader) DataprocJobMain.class.getClassLoader(),
                              Arrays.asList(Constants.Files.RESOURCES_JAR,
                                            Constants.Files.APPLICATION_JAR,
                                            Constants.Files.TWILL_JAR));
    LOG.error("Arjan: " + urls.length + " " + Arrays.toString(urls));
    Arrays.stream(urls).forEach(url -> LOG.debug("Classpath URL: {}", url));

    // Create new URL classloader with provided classpath.
    // Don't close the classloader since this is the main classloader,
    // which can be used for shutdown hook execution.
    // Closing it too early can result in NoClassDefFoundError in shutdown hook execution.
    ClassLoader newCL = createContainerClassLoader(urls);
    CompletableFuture<?> completion = new CompletableFuture<>();
    try {
      Thread.currentThread().setContextClassLoader(newCL);

      // load environment class and create instance of it
      String dataprocEnvClassName = DataprocRuntimeEnvironment.class.getName();
      Class<?> dataprocEnvClass = newCL.loadClass(dataprocEnvClassName);
      Object newDataprocEnvInstance = dataprocEnvClass.newInstance();

      try {
        // call initialize() method on dataprocEnvClass
        Method initializeMethod = dataprocEnvClass.getMethod("initialize", String.class);
        LOG.info("Invoking initialize() on {} with {}", dataprocEnvClassName, sparkCompat);
        initializeMethod.invoke(newDataprocEnvInstance, sparkCompat);

        // call run() method on runtimeJobClass
        Class<?> runEnvCls = newCL.loadClass(RuntimeJobEnvironment.class.getName());
        Class<?> runnerCls = newCL.loadClass(runtimeJobClassName);
        Method runMethod = runnerCls.getMethod("run", runEnvCls);
        Method stopMethod = runnerCls.getMethod("requestStop");

        Object runner = runnerCls.newInstance();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
          // Request the runtime job to stop if it it hasn't been completed
          if (completion.isDone()) {
            return;
          }
          try {
            stopMethod.invoke(runner);
          } catch (Exception e) {
            LOG.error("Exception raised when calling {}.stop()", runtimeJobClassName, e);
          }
        }));

        LOG.info("Invoking run() on {}", runtimeJobClassName);
        runMethod.invoke(runner, newDataprocEnvInstance);
      } finally {
        // call destroy() method on envProviderClass
        Method closeMethod = dataprocEnvClass.getMethod("destroy");
        LOG.info("Invoking destroy() on {}", dataprocEnvClassName);
        closeMethod.invoke(newDataprocEnvInstance);
      }

      LOG.info("Runtime job completed.");
      completion.complete(null);
    } catch (Throwable t) {
      // We log here and rethrow to make sure the exception log is captured in the job output
      LOG.error("Runtime job failed", t);
      completion.completeExceptionally(t);
      throw t;
    }
  }

  /**
   * This method will generate class path by adding following to urls to front of default classpath:
   * <p>
   * expanded.resource.jar
   * expanded.application.jar
   * expanded.application.jar/lib/*.jar
   * expanded.application.jar/classes
   * expanded.twill.jar
   * expanded.twill.jar/lib/*.jar
   * expanded.twill.jar/classes
   */
  private static URL[] getClasspath(URLClassLoader cl, URLClassLoader cl2, List<String> jarFiles) throws IOException {
    URL[] urls = cl.getURLs();
    List<URL> urlList = new ArrayList<>();
    for (String file : jarFiles) {
      File jarDir = new File(file);
      // add url for dir
      urlList.add(jarDir.toURI().toURL());
      if (file.equals(Constants.Files.RESOURCES_JAR)) {
        continue;
      }
      urlList.addAll(createClassPathURLs(jarDir));
    }

    urlList.addAll(Arrays.asList(urls));
    urlList.addAll(Arrays.asList(cl2.getURLs()));
    return urlList.toArray(new URL[0]);
  }

  private static List<URL> createClassPathURLs(File dir) throws MalformedURLException {
    List<URL> urls = new ArrayList<>();
    // add jar urls from lib under dir
    addJarURLs(new File(dir, "lib"), urls);
    // add classes under dir
    urls.add(new File(dir, "classes").toURI().toURL());
    return urls;
  }

  private static void addJarURLs(File dir, List<URL> result) throws MalformedURLException {
    File[] files = dir.listFiles(f -> f.getName().endsWith(".jar"));
    if (files == null) {
      return;
    }
    for (File file : files) {
      result.add(file.toURI().toURL());
    }
  }

  private static void expandArchives(Collection<String> archiveNames) throws IOException {
    for (String archive : archiveNames) {
      unpack(Paths.get(archive));
    }
  }

  private static void unpack(Path archiveFile) throws IOException {
    if (!Files.isRegularFile(archiveFile)) {
      LOG.warn("Skip archive expansion due to {} is not a file", archiveFile);
      return;
    }
    unJar(archiveFile);
  }

  private static void unJar(Path archiveFile) throws IOException {
    Path targetDir = archiveFile.resolveSibling(archiveFile.getFileName() + ".tmp");
    LOG.debug("Expanding archive {} to {}", archiveFile, targetDir);

    try (ZipInputStream zipIn = new ZipInputStream(Files.newInputStream(archiveFile))) {
      Files.createDirectories(targetDir);

      ZipEntry entry;
      while ((entry = zipIn.getNextEntry()) != null) {
        Path output = targetDir.resolve(entry.getName());

        if (entry.isDirectory()) {
          Files.createDirectories(output);
        } else {
          Files.createDirectories(output.getParent());
          Files.copy(zipIn, output);
        }
      }
    }

    Files.deleteIfExists(archiveFile);
    Files.move(targetDir, archiveFile);
    LOG.debug("Archive expanded to {}", targetDir);
  }

  /**
   * Converts a POSIX compliant program argument array to a String-to-String Map.
   *
   * @param args Array of Strings where each element is a POSIX compliant program argument (Ex: "--os=Linux" )
   * @return Map of argument Keys and Values (Ex: Key = "os" and Value = "Linux").
   */
  private static Map<String, Collection<String>> fromPosixArray(String[] args) {
    Map<String, Collection<String>> result = new LinkedHashMap<>();
    for (String arg : args) {
      int idx = arg.indexOf('=');
      int keyOff = arg.startsWith("--") ? "--".length() : 0;
      String key = idx < 0 ? arg.substring(keyOff) : arg.substring(keyOff, idx);
      String value = idx < 0 ? "" : arg.substring(idx + 1);
      // Remote quote from the value if it is quoted
      if (value.length() >= 2 && value.charAt(0) == '"' && value.charAt(value.length() - 1) == '"') {
        value = value.substring(1, value.length() - 1);
      }

      result.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
    }
    return result;
  }

  private static void logClassLoaderHeirarchy(ClassLoader cur) {
    while (cur != null) {
      LOG.error("Classloader: " + cur.getClass().getName());
      if (cur instanceof URLClassLoader) {
        StringBuilder paths = new StringBuilder("Class paths: [");
        for (URL url : ((URLClassLoader) cur).getURLs()) {
          paths.append(url.toString());
          paths.append(", ");
        }
        paths.append(']');
        LOG.error(paths.toString());
      }
      cur = cur.getParent();
    }
  }

  /**
   * Creates a {@link ClassLoader} for the the job execution.
   */
  private static ClassLoader createContainerClassLoader(URL[] classpath) {
    String containerClassLoaderName = System.getProperty(Constants.TWILL_CONTAINER_CLASSLOADER);
    logClassLoaderHeirarchy(DataprocJobMain.class.getClassLoader());

    URLClassLoader classLoader = new URLClassLoader(
      classpath, DataprocJobMain.class
      .getClassLoader()
      .getParent()
      .getParent()
    );
    if (containerClassLoaderName == null) {
      return classLoader;
    }

    try {
      @SuppressWarnings("unchecked")
      Class<? extends ClassLoader> cls = (Class<? extends ClassLoader>) classLoader.loadClass(containerClassLoaderName);

      // Instantiate with constructor (URL[] classpath, ClassLoader parentClassLoader)
      ClassLoader ret = cls.getConstructor(URL[].class, ClassLoader.class)
        .newInstance(classpath, classLoader.getParent());
      logClassLoaderHeirarchy(ret);
      return ret;
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Failed to load container class loader class " + containerClassLoaderName, e);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException("Container class loader must have a public constructor with " +
                                   "parameters (URL[] classpath, ClassLoader parent)", e);
    } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
      throw new RuntimeException("Failed to create container class loader of class " + containerClassLoaderName, e);
    }
  }
}

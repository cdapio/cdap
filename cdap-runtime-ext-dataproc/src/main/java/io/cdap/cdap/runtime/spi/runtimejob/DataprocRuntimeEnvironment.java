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


import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.FileContextLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.yarn.YarnTwillRunnerService;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Map;

/**
 * Dataproc runtime job environment . This class will provide implementation of {@link TwillRunner},
 * {@link DiscoveryService} and {@link DiscoveryServiceClient} to the runtime job.
 * All the public methods in this class are called through reflection from {@link DataprocJobMain}.
 */
@SuppressWarnings("unused")
public class DataprocRuntimeEnvironment implements RuntimeJobEnvironment {

  private static final Logger LOG = LoggerFactory.getLogger(DataprocRuntimeEnvironment.class);

  private static final String TWILL_ZK_SERVER_LOCALHOST = "twill.zk.server.localhost";
  private static final String ZK_QUORUM = "zookeeper.quorum";
  private static final String APP_SPARK_COMPAT = "app.program.spark.compat";
  private InMemoryZKServer zkServer;
  private TwillRunnerService yarnTwillRunnerService;
  private LocationFactory locationFactory;
  private Map<String, String> properties;

  /**
   * This method initializes the dataproc runtime environment.
   *
   * @param sparkCompat spark compat version supported by dataproc cluster
   * @throws Exception any exception while initializing the environment.
   */
  public void initialize(String sparkCompat) throws Exception {
    addConsoleAppender();
    System.setProperty(TWILL_ZK_SERVER_LOCALHOST, "false");
    zkServer = InMemoryZKServer.builder().build();
    zkServer.startAndWait();

    InetSocketAddress resolved = resolve(zkServer.getLocalAddress());
    String connectionStr = resolved.getHostString() + ":" + resolved.getPort();

    YarnConfiguration conf = new YarnConfiguration();
    locationFactory = new FileContextLocationFactory(conf);
    yarnTwillRunnerService = new YarnTwillRunnerService(conf, connectionStr, locationFactory);
    yarnTwillRunnerService.start();
    properties = ImmutableMap.of(ZK_QUORUM, connectionStr, APP_SPARK_COMPAT, sparkCompat);
  }

  @Override
  public LocationFactory getLocationFactory() {
    return locationFactory;
  }

  @Override
  public TwillRunner getTwillRunner() {
    return yarnTwillRunnerService;
  }

  @Override
  public Map<String, String> getProperties() {
    return properties;
  }

  /**
   * This method closes all the resources that are used for dataproc environment.
   */
  public void destroy() {
    if (yarnTwillRunnerService != null) {
      yarnTwillRunnerService.stop();
    }
    if (locationFactory != null) {
      Location location = locationFactory.create("/");
      try {
        location.delete(true);
      } catch (IOException e) {
        LOG.warn("Failed to delete location {}", location, e);
      }
    }
    if (zkServer != null) {
      zkServer.stopAndWait();
    }
  }

  private static InetSocketAddress resolve(InetSocketAddress bindAddress) throws Exception {
    // If domain of bindAddress is not resolvable, address of bindAddress is null.
    if (bindAddress.getAddress() != null && bindAddress.getAddress().isAnyLocalAddress()) {
      return new InetSocketAddress(InetAddress.getLocalHost().getHostName(), bindAddress.getPort());
    }
    return bindAddress;
  }

  /**
   * Adds a log appender for writing to stdout. This is for Dataproc job agent to include logs from the job main
   * in the job output.
   */
  private static void addConsoleAppender() {
    ILoggerFactory loggerFactory = LoggerFactory.getILoggerFactory();
    if (!(loggerFactory instanceof LoggerContext)) {
      return;
    }

    LoggerContext loggerContext = (LoggerContext) loggerFactory;

    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    URL url = classLoader.getResource("logback-console.xml");
    if (url == null) {
      LOG.warn("Cannot find logback-console.xml from classloader");
      return;
    }

    try {
      JoranConfigurator joranConfigurator = new JoranConfigurator();
      // Configure with the addition logback-console, don't reset the context so that it will add the new appender
      joranConfigurator.setContext(loggerContext);
      joranConfigurator.doConfigure(url);
    } catch (JoranException e) {
      LOG.warn("Failed to configure log appender, logs will be missing from the Dataproc Job output");
    }
  }
}

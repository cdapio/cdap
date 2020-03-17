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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
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
  private InMemoryZKServer zkServer;
  private TwillRunnerService yarnTwillRunnerService;
  private LocationFactory locationFactory;
  private String connectionStr;

  /**
   * This method initializes the dataproc runtime environment.
   *
   * @throws Exception any exception while initializing the environment.
   */
  public void initialize() throws Exception {
    System.setProperty(TWILL_ZK_SERVER_LOCALHOST, "false");
    zkServer = InMemoryZKServer.builder().build();
    zkServer.startAndWait();

    InetSocketAddress resolved = resolve(zkServer.getLocalAddress());
    connectionStr = resolved.getHostString() + ":" + resolved.getPort();

    YarnConfiguration conf = new YarnConfiguration();
    locationFactory = new FileContextLocationFactory(conf);
    yarnTwillRunnerService = new YarnTwillRunnerService(conf, connectionStr, locationFactory);
    yarnTwillRunnerService.start();
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
    return ImmutableMap.of(ZK_QUORUM, connectionStr);
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
}

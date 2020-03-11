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

package io.cdap.cdap.runtime.runtimejob;


import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.runtime.spi.runtimejob.RuntimeJobEnvironment;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ZKDiscoveryService;
import org.apache.twill.filesystem.FileContextLocationFactory;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.yarn.YarnTwillRunnerService;
import org.apache.twill.zookeeper.RetryStrategies;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.twill.zookeeper.ZKClientServices;
import org.apache.twill.zookeeper.ZKClients;

import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Dataproc runtime job environment . This class will provide implementation of {@link TwillRunner},
 * {@link DiscoveryService} and {@link DiscoveryServiceClient} to the runtime job.
 * All the public methods in this class are called through reflection from {@link DataprocJobMain}.
 */
@SuppressWarnings("unused")
public class DataprocRuntimeEnvironment implements RuntimeJobEnvironment {
  private static final String TWILL_ZK_SERVER_LOCALHOST = "twill.zk.server.localhost";
  private static final String ZK_QUORUM = "zookeeper.quorum";
  private ZKDiscoveryService zkDiscoveryService;
  private ZKClientService zkClientService;
  private TwillRunnerService yarnTwillRunnerService;
  private InMemoryZKServer zkServer;
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

    zkClientService = ZKClientServices.delegate(
      ZKClients.reWatchOnExpire(
        ZKClients.retryOnFailure(
          ZKClientService.Builder.of(connectionStr)
            .setSessionTimeout(40000)
            .build(),
          RetryStrategies.exponentialDelay(500, 2000, TimeUnit.MILLISECONDS)
        )
      )
    );

    // Invoke method through reflection. This is needed because of conflict in guava versions during compilation
    Method startAndWait = zkClientService.getClass().getMethod("startAndWait");
    startAndWait.invoke(zkClientService);

    zkDiscoveryService = new ZKDiscoveryService(zkClientService);

    YarnConfiguration conf = new YarnConfiguration();
    LocationFactory locationFactory = new FileContextLocationFactory(conf);
    yarnTwillRunnerService = new YarnTwillRunnerService(conf, connectionStr, locationFactory);
    yarnTwillRunnerService.start();
  }

  @Override
  public DiscoveryService getDiscoveryService() {
    return zkDiscoveryService;
  }

  @Override
  public DiscoveryServiceClient getDiscoveryServiceClient() {
    return zkDiscoveryService;
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
    if (zkDiscoveryService != null) {
      zkDiscoveryService.close();
    }
    if (zkClientService != null) {
      try {
        // Invoke method through reflection. This is needed because of conflict in guava versions during compilation
        Method stopAndWait = zkClientService.getClass().getMethod("stopAndWait");
        stopAndWait.invoke(zkClientService);
      } catch (Exception e) {
        throw new RuntimeException("Error occurred while invoking stop on zk client service", e);
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

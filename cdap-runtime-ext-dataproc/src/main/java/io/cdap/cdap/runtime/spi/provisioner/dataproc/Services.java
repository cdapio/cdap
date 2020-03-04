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

package io.cdap.cdap.runtime.spi.provisioner.dataproc;


import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.runtime.spi.runtimejob.JobContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.discovery.ZKDiscoveryService;
import org.apache.twill.filesystem.FileContextLocationFactory;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.yarn.YarnTwillRunnerService;
import org.apache.twill.zookeeper.RetryStrategies;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.twill.zookeeper.ZKClientServices;
import org.apache.twill.zookeeper.ZKClients;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class Services {
  private final ZKDiscoveryService zkDiscoveryService;
  private final YarnTwillRunnerService yarnTwillRunnerService;
  private final String connectionString;

  /**
   *
   */
  public Services() {
    InMemoryZKServer server = InMemoryZKServer.builder().build();
    server.startAndWait();

    InetSocketAddress resolved = resolve(server.getLocalAddress());
    this.connectionString = resolved.getHostString() + ":" + resolved.getPort();

    System.out.println("ZK host: " + this.connectionString);

    YarnConfiguration conf = new YarnConfiguration();
    LocationFactory locationFactory = new FileContextLocationFactory(conf);
    yarnTwillRunnerService = new YarnTwillRunnerService(conf, this.connectionString, locationFactory);
    yarnTwillRunnerService.start();

    ZKClientService service = ZKClientServices.delegate(
      ZKClients.reWatchOnExpire(
        ZKClients.retryOnFailure(
          ZKClientService.Builder.of(this.connectionString)
            .setSessionTimeout(40000)
            .build(),
          RetryStrategies.exponentialDelay(500, 2000, TimeUnit.MILLISECONDS)
        )
      )
    );

    this.zkDiscoveryService = new ZKDiscoveryService(service);
  }

  /**
   * called through reflection
   * @return
   */
  public JobContext getJobContext() {
    return new DataprocJobContext(zkDiscoveryService, zkDiscoveryService, yarnTwillRunnerService,
                                  ImmutableMap.of("zookeeper.quorum", connectionString));
  }

  public static InetSocketAddress resolve(InetSocketAddress bindAddress) {
    try {
      // If domain of bindAddress is not resolvable, address of bindAddress is null.
      if (bindAddress.getAddress() != null && bindAddress.getAddress().isAnyLocalAddress()) {
        return new InetSocketAddress(InetAddress.getLocalHost().getHostName(), bindAddress.getPort());
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return bindAddress;
  }
}

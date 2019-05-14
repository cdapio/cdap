/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.distributed.remote;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.Networks;
import io.cdap.cdap.internal.app.runtime.monitor.proxy.ServiceSocksProxy;
import io.cdap.cdap.master.spi.discovery.DefaultServiceDiscovered;
import io.cdap.cdap.proto.ProgramType;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.inject.Inject;

/**
 * A discovery service implementation used in remote runtime execution. It has a fixed list of services
 * that will go through {@link ServiceSocksProxy}, hence won't resolve the service name to an actual address.
 * It also use the {@link CConfiguration} as the backing store for service announcements, which is suitable for
 * the current remote runtime that has some "mini" system services running in the runtime process (the driver).
 */
public class RemoteExecutionDiscoveryService implements DiscoveryServiceClient, DiscoveryService {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteExecutionDiscoveryService.class);

  private final CConfiguration cConf;
  private final Map<String, DefaultServiceDiscovered> services;

  @Inject
  RemoteExecutionDiscoveryService(CConfiguration cConf) {
    this.cConf = cConf;
    this.services = new ConcurrentHashMap<>();
  }

  @Override
  public synchronized Cancellable register(Discoverable discoverable) {
    String serviceName = discoverable.getName();
    InetSocketAddress discoverableAddr = discoverable.getSocketAddress();
    DefaultServiceDiscovered serviceDiscovered = services.computeIfAbsent(serviceName, DefaultServiceDiscovered::new);

    // Add the address to cConf
    String key = Constants.RuntimeMonitor.DISCOVERY_SERVICE_PREFIX + serviceName;
    Networks.addAddress(cConf, key, discoverableAddr);
    LOG.debug("Discoverable {} added to configuration", discoverable);

    // Update the ServiceDiscovered
    updateServiceDiscovered(serviceDiscovered, cConf, key);

    return () -> {
      synchronized (RemoteExecutionDiscoveryService.this) {
        Networks.removeAddress(cConf, key, discoverableAddr);
        updateServiceDiscovered(serviceDiscovered, cConf, key);
      }
    };
  }

  @Override
  public ServiceDiscovered discover(String name) {
    // In Remote runtime, we don't support program discovery, hence always return an empty ServiceDiscovered
    for (ProgramType programType : ProgramType.values()) {
      if (programType.isDiscoverable() && name.startsWith(programType.getDiscoverableTypeName() + ".")) {
        return new DefaultServiceDiscovered(name);
      }
    }

    // Discovery for system services
    DefaultServiceDiscovered serviceDiscovered = services.get(name);
    String key = Constants.RuntimeMonitor.DISCOVERY_SERVICE_PREFIX + name;

    // Use a while loop to resolve races between discover and cancellable of register
    while (serviceDiscovered == null) {
      // Try to add the ServiceDiscovered object
      serviceDiscovered = new DefaultServiceDiscovered(name);
      synchronized (this) {
        updateServiceDiscovered(serviceDiscovered, cConf, key);
        services.putIfAbsent(name, serviceDiscovered);
      }

      serviceDiscovered = services.get(name);
    }

    return serviceDiscovered;
  }

  private void updateServiceDiscovered(DefaultServiceDiscovered serviceDiscovered, CConfiguration cConf, String key) {
    String name = serviceDiscovered.getName();
    Set<Discoverable> discoverables;
    Set<InetSocketAddress> addresses = Networks.getAddresses(cConf, key);

    if (addresses.isEmpty()) {
      // If there is no address from the configuration, assuming it is using the service proxy,
      // hence the discovery name is the hostname. Also use port == 0 so that the RemoteExecutionProxySelector
      // knows that it need to use service proxy
      discoverables = Collections.singleton(new Discoverable(name, InetSocketAddress.createUnresolved(name, 0)));
    } else {
      // Otherwise, includes all addresses from the config
      discoverables = addresses.stream().map(addr -> new Discoverable(name, addr)).collect(Collectors.toSet());
    }
    serviceDiscovered.setDiscoverables(discoverables);
  }
}

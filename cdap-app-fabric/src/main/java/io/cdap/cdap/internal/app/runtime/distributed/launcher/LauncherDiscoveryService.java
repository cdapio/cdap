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

package io.cdap.cdap.internal.app.runtime.distributed.launcher;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.Networks;
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
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 */
public class LauncherDiscoveryService implements DiscoveryServiceClient, DiscoveryService {

  private static final Logger LOG = LoggerFactory.getLogger(LauncherDiscoveryService.class);

  private final CConfiguration cConf;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final DiscoveryService discoveryService;
  private final Map<String, Integer> seenSoFar;
  private final Map<String, DefaultServiceDiscovered> services;

  @Inject
  public LauncherDiscoveryService(CConfiguration cConf,
                                  @Named(LauncherDiscoveryModule.DELEGATE_DISCOVERY_SERVICE_CLIENT)
                                    DiscoveryServiceClient client,
                                  @Named(LauncherDiscoveryModule.DELEGATE_DISCOVERY_SERVICE)
                                      DiscoveryService service) {
    this.cConf = cConf;
    this.discoveryServiceClient = client;
    this.discoveryService = service;
    this.services = new ConcurrentHashMap<>();
    this.seenSoFar = new ConcurrentHashMap<>();
  }

  @Override
  public Cancellable register(Discoverable discoverable) {
    LOG.info("### Service name to register: {}, address {}, payload {}", discoverable.getName(),
             discoverable.getSocketAddress(),
             Bytes.toString(discoverable.getPayload()));
    String serviceName = discoverable.getName();
    DefaultServiceDiscovered serviceDiscovered = services.computeIfAbsent(serviceName, DefaultServiceDiscovered::new);
    // Add the address to cConf
    String key = Constants.RuntimeMonitor.DISCOVERY_SERVICE_PREFIX + discoverable.getName();
    synchronized (this) {
      //seenSoFar.putIfAbsent(discoverable.getName(), 1);
      Networks.addDiscoverable(cConf, key, discoverable);
      LOG.info("### Discoverable {} with address {} added to configuration with payload {}",
                discoverable.getName(), discoverable.getSocketAddress(), Bytes.toString(discoverable.getPayload()));
      updateServiceDiscovered(serviceDiscovered, cConf, key);
    }
    return () -> {
      synchronized (LauncherDiscoveryService.this) {
        Networks.removeDiscoverable(cConf, key, discoverable);
        updateServiceDiscovered(serviceDiscovered, cConf, key);
      }
    };
  }

  @Override
  public ServiceDiscovered discover(String name) {
    LOG.info("### Service to discover: {}", name);
    // In Remote runtime, we don't support program discovery, hence always return an empty ServiceDiscovered
    for (ProgramType programType : ProgramType.values()) {
      if (programType.isDiscoverable() && name.startsWith(programType.getDiscoverableTypeName() + ".")) {
        LOG.info("### returning empty discovered for {}", name);
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

    LOG.info("### remote service discovery for {}", name);
    return serviceDiscovered;
  }

  private void updateServiceDiscovered(DefaultServiceDiscovered serviceDiscovered, CConfiguration cConf, String key) {
    String name = serviceDiscovered.getName();
    Set<Discoverable> discoverables = Networks.getDiscoverables(cConf, key);

    if (discoverables.isEmpty()) {
      discoverables = Collections.singleton(new Discoverable(name, InetSocketAddress.createUnresolved(
        "six-one-one-cluster-vini-project-238000-dot-usw1.datafusion.googleusercontent.com/api", 0),
                                                             "https://".getBytes(StandardCharsets.UTF_8)));
    }

    for (Discoverable d : discoverables) {
      LOG.info("### Launcher discoverable {} with address {}",
               d.getName(), d.getSocketAddress());
    }

    serviceDiscovered.setDiscoverables(discoverables);
  }

  public Set<Discoverable> toSet(Iterator<Discoverable> collection) {
    HashSet<Discoverable> set = new HashSet<>();
    while (collection.hasNext()) {
      set.add(collection.next());
    }
    return set;
  }
}

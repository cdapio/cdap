/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tephra.runtime;

import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.apache.twill.discovery.ServiceDiscovered;
import org.apache.twill.discovery.ZKDiscoveryService;
import org.apache.twill.zookeeper.ZKClientService;

/**
 * Provides access to Google Guice modules for in-memory, single-node, and distributed operation for
 * {@link DiscoveryService} and {@link DiscoveryServiceClient}.
 */
public final class DiscoveryModules {

  public Module getInMemoryModules() {
    return new InMemoryDiscoveryModule();
  }

  public Module getSingleNodeModules() {
    return new InMemoryDiscoveryModule();
  }

  public Module getDistributedModules() {
    return new ZKDiscoveryModule();
  }

  private static final class InMemoryDiscoveryModule extends AbstractModule {

    // ensuring to be singleton across JVM
    private static final InMemoryDiscoveryService IN_MEMORY_DISCOVERY_SERVICE = new InMemoryDiscoveryService();

    @Override
    protected void configure() {
      InMemoryDiscoveryService discovery = IN_MEMORY_DISCOVERY_SERVICE;
      bind(DiscoveryService.class).toInstance(discovery);
      bind(DiscoveryServiceClient.class).toInstance(discovery);
    }
  }

  private static final class ZKDiscoveryModule extends PrivateModule {

    @Override
    protected void configure() {
      expose(DiscoveryService.class);
      expose(DiscoveryServiceClient.class);
    }

    @Provides
    @Singleton
    private ZKDiscoveryService providesZKDiscoveryService(ZKClientService zkClient) {
      return new ZKDiscoveryService(zkClient);
    }

    @Provides
    @Singleton
    private DiscoveryService providesDiscoveryService(final ZKClientService zkClient,
                                                      final ZKDiscoveryService delegate) {
      return new DiscoveryService() {
        @Override
        public Cancellable register(Discoverable discoverable) {
          if (!zkClient.isRunning()) {
            zkClient.startAndWait();
          }
          return delegate.register(discoverable);
        }
      };
    }

    @Provides
    @Singleton
    private DiscoveryServiceClient providesDiscoveryServiceClient(final ZKClientService zkClient,
                                                                  final ZKDiscoveryService delegate) {
      return new DiscoveryServiceClient() {
        @Override
        public ServiceDiscovered discover(String s) {
          if (!zkClient.isRunning()) {
            zkClient.startAndWait();
          }
          return delegate.discover(s);
        }
      };
    }
  }
}

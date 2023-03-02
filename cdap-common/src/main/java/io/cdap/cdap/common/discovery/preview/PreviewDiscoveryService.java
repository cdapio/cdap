/*
 * Copyright © 2021 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.common.discovery.preview;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.cdap.cdap.common.guice.preview.PreviewDiscoveryRuntimeModule;
import io.netty.util.internal.ConcurrentSet;
import java.util.Set;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.apache.twill.discovery.ServiceDiscovered;

/**
 * Discovery service that delegates either to in memory discovery service(local) or shared (actual)
 * discovery service.
 */
public class PreviewDiscoveryService implements DiscoveryService, DiscoveryServiceClient {

  private final DiscoveryServiceClient actual;
  private final InMemoryDiscoveryService local;
  private final Set<String> registeredLocalServices;

  @Inject
  PreviewDiscoveryService(
      @Named(PreviewDiscoveryRuntimeModule.ACTUAL_DISCOVERY_CLIENT) DiscoveryServiceClient actual) {
    this.actual = actual;
    this.local = new InMemoryDiscoveryService();
    this.registeredLocalServices = new ConcurrentSet<>();
  }

  // we only allow register using local discovery service as preview should not affect actual environment
  @Override
  public Cancellable register(Discoverable discoverable) {
    Cancellable cancellable = local.register(discoverable);
    registeredLocalServices.add(discoverable.getName());
    return () -> {
      cancellable.cancel();
      registeredLocalServices.remove(discoverable.getName());
    };
  }

  @Override
  public ServiceDiscovered discover(String name) {
    // if this service is registered in local, discover it using local discovery service
    if (registeredLocalServices.contains(name)) {
      return local.discover(name);
    }
    return actual.discover(name);
  }
}

/*
 * Copyright © 2018 Cask Data, Inc.
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

package io.cdap.cdap.common.guice;

import com.google.inject.AbstractModule;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.InMemoryDiscoveryService;

/**
 * The Guice module for providing bindings for {@link DiscoveryService} and {@link DiscoveryServiceClient}
 * for service discovery within the same process.
 */
public final class InMemoryDiscoveryModule extends AbstractModule {

  // ensuring to be singleton across JVM
  private static final InMemoryDiscoveryService IN_MEMORY_DISCOVERY_SERVICE = new InMemoryDiscoveryService();

  @Override
  protected void configure() {
    InMemoryDiscoveryService discovery = IN_MEMORY_DISCOVERY_SERVICE;
    bind(DiscoveryService.class).toInstance(discovery);
    bind(DiscoveryServiceClient.class).toInstance(discovery);
  }
}

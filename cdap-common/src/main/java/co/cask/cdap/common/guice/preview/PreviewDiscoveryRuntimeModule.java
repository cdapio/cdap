/*
 * Copyright © 2016 Cask Data, Inc.
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
package co.cask.cdap.common.guice.preview;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.google.inject.name.Names;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.InMemoryDiscoveryService;

/**
 * Provides Guice bindings for DiscoveryService and DiscoveryServiceClient for preview mode.
 */
public class PreviewDiscoveryRuntimeModule extends AbstractModule {
  private DiscoveryService discoveryService;

  public PreviewDiscoveryRuntimeModule(DiscoveryService discoveryService) {
    this.discoveryService = discoveryService;
  }

  @Override
  protected void configure() {
    // we want to share the actual discovery service, so preview http service can register when it starts up
    // and router can route directly to preview service. for other services we want to use local discovery service.
    bind(DiscoveryService.class).annotatedWith(
      Names.named("shared-discovery-service")).toInstance(discoveryService);
    bind(InMemoryDiscoveryService.class).in(Singleton.class);
    bind(DiscoveryService.class).to(InMemoryDiscoveryService.class);
    bind(DiscoveryServiceClient.class).to(InMemoryDiscoveryService.class);
  }
}

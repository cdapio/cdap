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
package io.cdap.cdap.common.guice.preview;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.google.inject.name.Names;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.InMemoryDiscoveryService;

// TODO: we may no longer need this
/**
 * Provides Guice bindings for DiscoveryService and DiscoveryServiceClient for preview mode.
 */
public class PreviewDiscoveryRuntimeModule extends AbstractModule {
  private DiscoveryService discoveryService;
  private DiscoveryServiceClient discoveryServiceClient;

  public PreviewDiscoveryRuntimeModule(DiscoveryService discoveryService,
                                       DiscoveryServiceClient discoveryServiceClient) {
    this.discoveryService = discoveryService;
    this.discoveryServiceClient = discoveryServiceClient;
  }

  @Override
  protected void configure() {
    bind(DiscoveryService.class).toInstance(discoveryService);
    bind(DiscoveryServiceClient.class).toInstance(discoveryServiceClient);
  }
}

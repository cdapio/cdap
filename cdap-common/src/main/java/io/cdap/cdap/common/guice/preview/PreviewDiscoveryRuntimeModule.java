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
import com.google.inject.Scopes;
import com.google.inject.name.Names;
import io.cdap.cdap.common.discovery.preview.PreviewDiscoveryService;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;

/**
 * Provides Guice bindings for DiscoveryService and DiscoveryServiceClient for preview mode.
 */
public class PreviewDiscoveryRuntimeModule extends AbstractModule {

  public static final String ACTUAL_DISCOVERY_CLIENT = "actualDiscoveryClient";

  private final DiscoveryServiceClient actualDiscoveryClient;

  public PreviewDiscoveryRuntimeModule(DiscoveryServiceClient actualDiscoveryClient) {
    this.actualDiscoveryClient = actualDiscoveryClient;
  }

  @Override
  protected void configure() {
    bind(DiscoveryServiceClient.class).annotatedWith(
        Names.named(ACTUAL_DISCOVERY_CLIENT)).toInstance(actualDiscoveryClient);
    bind(DiscoveryService.class).to(PreviewDiscoveryService.class);
    bind(DiscoveryServiceClient.class).to(PreviewDiscoveryService.class);
    bind(PreviewDiscoveryService.class).in(Scopes.SINGLETON);
  }
}

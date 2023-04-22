/*
 * Copyright © 2021-2023 Cask Data, Inc.
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

package io.cdap.cdap.common.internal.remote;

import com.google.inject.AbstractModule;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;

/**
 * Module for Runnable tasks.
 */
public class RunnableTaskModule extends AbstractModule {

  private final DiscoveryService discoveryService;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final MetricsCollectionService metricsCollectionService;

  public RunnableTaskModule(DiscoveryService discoveryService,
      DiscoveryServiceClient discoveryServiceClient,
      MetricsCollectionService metricsCollectionService) {
    this.discoveryService = discoveryService;
    this.discoveryServiceClient = discoveryServiceClient;
    this.metricsCollectionService = metricsCollectionService;
  }

  @Override
  protected void configure() {
    bind(DiscoveryService.class).toInstance(discoveryService);
    bind(DiscoveryServiceClient.class).toInstance(discoveryServiceClient);
    bind(MetricsCollectionService.class).toInstance(metricsCollectionService);
  }
}

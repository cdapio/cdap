/*
 * Copyright © 2014-2017 Cask Data, Inc.
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
package co.cask.cdap.metrics.runtime;

import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.common.resource.ResourceBalancerService;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.zookeeper.ZKClientService;

import javax.annotation.Nullable;

/**
 * Provides base abstract implementation of Metrics processor service.
 */
public abstract class AbstractMetricsRuntimeProcessorService extends ResourceBalancerService {

  @Nullable
  protected MetricsContext metricsContext;

  public AbstractMetricsRuntimeProcessorService(String serviceName,
                                                int partitionCount,
                                                ZKClientService zkClient,
                                                DiscoveryService discoveryService,
                                                DiscoveryServiceClient discoveryServiceClient) {
    super(serviceName, partitionCount, zkClient, discoveryService, discoveryServiceClient);
  }

  public void setMetricsContext(MetricsContext metricsContext) {
    this.metricsContext = metricsContext;
  }
}

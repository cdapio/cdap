/*
 * Copyright © 2017 Cask Data, Inc.
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
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.resource.ResourceBalancerService;
import co.cask.cdap.metrics.process.MessagingMetricsProcessorServiceFactory;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.zookeeper.ZKClient;

import java.util.Set;
import javax.annotation.Nullable;

/**
 * A service that creates {@link co.cask.cdap.metrics.process.MessagingMetricsProcessorService} dynamically at runtime
 * according to the given numerical topic suffices from {@link ResourceBalancerService}.
 */
public final class MessagingMetricsProcessorRuntimeService extends ResourceBalancerService {

  @Nullable
  private MetricsContext metricsContext;

  private static final String SERVICE_NAME = "metrics.processor.messaging.fetcher";

  private final MessagingMetricsProcessorServiceFactory factory;

  @Inject
  public MessagingMetricsProcessorRuntimeService(CConfiguration conf,
                                                 ZKClient zkClient,
                                                 DiscoveryService discoveryService,
                                                 DiscoveryServiceClient discoveryServiceClient,
                                                 MessagingMetricsProcessorServiceFactory
                                                            metricsProcessorFactory) {
    super(SERVICE_NAME, conf.getInt(Constants.Metrics.MESSAGING_TOPIC_NUM),
          zkClient, discoveryService, discoveryServiceClient);
    this.factory = metricsProcessorFactory;
  }

  @Override
  protected Service createService(Set<Integer> topicNumbers) {
    co.cask.cdap.metrics.process.MessagingMetricsProcessorService service = factory.create(topicNumbers);
    service.setMetricsContext(metricsContext);
    return service;
  }

  public void setMetricsContext(MetricsContext metricsContext) {
    this.metricsContext = metricsContext;
  }
}

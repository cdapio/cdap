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

import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.resource.ResourceBalancerService;
import co.cask.cdap.metrics.process.MessagingMetricsProcessorService;
import co.cask.cdap.metrics.process.MessagingMetricsProcessorServiceFactory;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.zookeeper.ZKClient;

import java.util.Set;

/**
 * A service that creates {@link MessagingMetricsProcessorService} dynamically at runtime
 * according to the given numerical topic suffices from {@link ResourceBalancerService}.
 */
public final class MessagingMetricsProcessorRuntimeService extends ResourceBalancerService {

  private static final String SERVICE_NAME = "metrics.processor.messaging.fetcher";

  private final MessagingMetricsProcessorServiceFactory factory;
  private final MetricsCollectionService metricsCollectionService;

  @Inject
  MessagingMetricsProcessorRuntimeService(CConfiguration conf,
                                          ZKClient zkClient,
                                          DiscoveryService discoveryService,
                                          DiscoveryServiceClient discoveryServiceClient,
                                          MetricsCollectionService metricsCollectionService,
                                          MessagingMetricsProcessorServiceFactory metricsProcessorFactory) {
    super(SERVICE_NAME, conf.getInt(Constants.Metrics.MESSAGING_TOPIC_NUM),
          zkClient, discoveryService, discoveryServiceClient);
    this.factory = metricsProcessorFactory;
    this.metricsCollectionService = metricsCollectionService;
  }

  @Override
  protected Service createService(Set<Integer> topicNumbers) {
    return factory.create(topicNumbers,
                          metricsCollectionService.getContext(Constants.Metrics.METRICS_PROCESSOR_CONTEXT));
  }
}

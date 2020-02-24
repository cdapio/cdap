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

package io.cdap.cdap.metrics.runtime;

import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.resource.ResourceBalancerService;
import io.cdap.cdap.metrics.process.MessagingMetricsProcessorManagerService;
import io.cdap.cdap.metrics.process.MessagingMetricsProcessorServiceFactory;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.zookeeper.ZKClient;

import java.util.Set;

/**
 * A service that creates {@link MessagingMetricsProcessorManagerService} dynamically at runtime
 * according to the given numerical topic suffices from {@link ResourceBalancerService}.
 */
public final class MessagingMetricsProcessorRuntimeService extends ResourceBalancerService {

  private static final String SERVICE_NAME = "metrics.processor.messaging.fetcher";

  private final MessagingMetricsProcessorServiceFactory factory;
  private final MetricsCollectionService metricsCollectionService;
  private final Integer instanceId;

  @Inject
  MessagingMetricsProcessorRuntimeService(CConfiguration conf,
                                          ZKClient zkClient,
                                          DiscoveryService discoveryService,
                                          DiscoveryServiceClient discoveryServiceClient,
                                          MetricsCollectionService metricsCollectionService,
                                          MessagingMetricsProcessorServiceFactory metricsProcessorFactory,
                                          @Named(Constants.Metrics.TWILL_INSTANCE_ID) Integer instanceId) {
    super(SERVICE_NAME, conf.getInt(Constants.Metrics.MESSAGING_TOPIC_NUM),
          zkClient, discoveryService, discoveryServiceClient);
    this.factory = metricsProcessorFactory;
    this.metricsCollectionService = metricsCollectionService;
    this.instanceId = instanceId;
  }

  @Override
  protected Service createService(Set<Integer> topicNumbers) {
    return factory.create(topicNumbers,
                          metricsCollectionService.getContext(Constants.Metrics.METRICS_PROCESSOR_CONTEXT), instanceId);
  }
}

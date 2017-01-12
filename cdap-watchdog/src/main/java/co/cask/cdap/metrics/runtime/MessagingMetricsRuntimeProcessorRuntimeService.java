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

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.metrics.process.MessagingMetricsProcessorServiceFactory;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.zookeeper.ZKClientService;

import java.util.Set;

/**
 * Metrics processor service that processes events from Messaging.
 */
public final class MessagingMetricsRuntimeProcessorRuntimeService extends AbstractMetricsRuntimeProcessorService {

  private static final String SERVICE_NAME = "metrics.processor.messaging.fetcher";

  private final MessagingMetricsProcessorServiceFactory factory;

  @Inject
  public MessagingMetricsRuntimeProcessorRuntimeService(CConfiguration conf,
                                                        ZKClientService zkClient,
                                                        DiscoveryService discoveryService,
                                                        DiscoveryServiceClient discoveryServiceClient,
                                                        MessagingMetricsProcessorServiceFactory
                                                            metricsProcessorFactory) {
    super(SERVICE_NAME,
          conf.getInt(Constants.Metrics.MESSAGING_PARTITION_SIZE, Constants.Metrics.DEFAULT_KAFKA_PARTITION_SIZE),
          zkClient, discoveryService, discoveryServiceClient);
    this.factory = metricsProcessorFactory;
  }

  @Override
  protected Service createService(Set<Integer> partitions) {
    co.cask.cdap.metrics.process.MessagingMetricsProcessorService service = factory.create(partitions);
    service.setMetricsContext(metricsContext);
    return service;
  }
}

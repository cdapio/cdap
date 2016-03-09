/*
 * Copyright © 2014 Cask Data, Inc.
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
package co.cask.cdap.logging.save;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.resource.ResourceBalancerService;
import co.cask.cdap.logging.LoggingConfiguration;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.zookeeper.ZKClientService;

import java.util.Set;

/**
 * Log saver service that processes events from Kafka.
 */
public final class KafkaLogSaverService extends ResourceBalancerService {

  private static final String SERVICE_NAME = "log.saver.kafka.consumer";

  private final LogSaverFactory logSaverFactory;

  @Inject
  public KafkaLogSaverService(CConfiguration conf,
                              ZKClientService zkClient,
                              DiscoveryService discoveryService,
                              DiscoveryServiceClient discoveryServiceClient,
                              LogSaverFactory logSaverFactory) {
    super(SERVICE_NAME,
          Integer.valueOf(conf.get(LoggingConfiguration.NUM_PARTITIONS, LoggingConfiguration.DEFAULT_NUM_PARTITIONS)),
          zkClient, discoveryService, discoveryServiceClient);

    this.logSaverFactory = logSaverFactory;
  }

  @Override
  protected Service createService(Set<Integer> partitions) {
    return logSaverFactory.create(partitions);
  }
}

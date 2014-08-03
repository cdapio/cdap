/*
 * Copyright 2014 Cask, Inc.
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
package co.cask.cdap.common.guice;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.KafkaConstants;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.twill.internal.kafka.client.ZKKafkaClientService;
import org.apache.twill.kafka.client.KafkaClient;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKClients;

/**
 * Guice module for {@link KafkaClient} and {@link KafkaClientService}. Requires bindings from
 * {@link ConfigModule} and {@link ZKClientModule}.
 */
public class KafkaClientModule extends AbstractModule {

  @Override
  protected void configure() {
    /**
     * Bindings to {@link KafkaClientService} is done by
     * {@link #providesKafkaClientService(CConfiguration, ZKClient)}
     */
    bind(KafkaClient.class).to(KafkaClientService.class);
  }

  @Provides
  @Singleton
  private KafkaClientService providesKafkaClientService(CConfiguration cConf, ZKClient zkClient) {
    String kafkaZKNamespace = cConf.get(KafkaConstants.ConfigKeys.ZOOKEEPER_NAMESPACE_CONFIG);
    return new ZKKafkaClientService(
      kafkaZKNamespace == null ? zkClient : ZKClients.namespace(zkClient, "/" + kafkaZKNamespace)
    );
  }
}

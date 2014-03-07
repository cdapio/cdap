/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.guice;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.KafkaConstants;
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

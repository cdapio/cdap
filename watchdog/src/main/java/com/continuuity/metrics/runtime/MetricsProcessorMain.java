/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.runtime;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.conf.KafkaConstants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.common.runtime.DaemonMain;
import com.continuuity.internal.kafka.client.ZKKafkaClientService;
import com.continuuity.kafka.client.KafkaClientService;
import com.continuuity.metrics.MetricsConstants;
import com.continuuity.metrics.guice.DistributedMetricsTableModule;
import com.continuuity.metrics.guice.MetricsProcessorModule;
import com.continuuity.metrics.process.KafkaMetricsProcessingService;
import com.continuuity.metrics.process.MessageCallbackFactory;
import com.continuuity.metrics.process.MetricsMessageCallbackFactory;
import com.continuuity.weave.common.Services;
import com.continuuity.weave.zookeeper.RetryStrategies;
import com.continuuity.weave.zookeeper.ZKClient;
import com.continuuity.weave.zookeeper.ZKClientService;
import com.continuuity.weave.zookeeper.ZKClientServices;
import com.continuuity.weave.zookeeper.ZKClients;
import com.google.common.util.concurrent.Futures;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.name.Named;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Main class for starting a metrics processor in distributed mode.
 */
public final class MetricsProcessorMain extends DaemonMain {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsProcessorMain.class);

  private ZKClientService zkClientService;
  private KafkaClientService kafkaClientService;
  private KafkaMetricsProcessingService processingService;

  public static void main(String[] args) throws Exception {
    new MetricsProcessorMain().doMain(args);
  }

  @Override
  public void init(String[] args) {
    CConfiguration cConf = CConfiguration.create();
    Configuration hConf = HBaseConfiguration.create();

    // Connect to Zookeeper for kafka client
    zkClientService =
      ZKClientServices.delegate(
        ZKClients.reWatchOnExpire(
          ZKClients.retryOnFailure(
            ZKClientService.Builder.of(cConf.get(Constants.CFG_ZOOKEEPER_ENSEMBLE)).setSessionTimeout(10000).build(),
            RetryStrategies.fixDelay(2, TimeUnit.SECONDS)
          )
        )
      );

    // For talking to kafka
    String kafkaZKNamespace = cConf.get(KafkaConstants.ConfigKeys.ZOOKEEPER_NAMESPACE_CONFIG);
    ZKClient kafkaZKClient = (kafkaZKNamespace == null)
                                  ? zkClientService
                                  : ZKClients.namespace(zkClientService, "/" + kafkaZKNamespace);

    kafkaClientService = new ZKKafkaClientService(kafkaZKClient);

    LOG.info("Kafka ZK: {}", kafkaZKClient.getConnectString());

    Injector injector = Guice.createInjector(new ConfigModule(cConf, hConf),
                                             new IOModule(),
                                             new DistributedMetricsTableModule(),
                                             new MetricsProcessorModule(),
                                             new PrivateModule() {
      @Override
      protected void configure() {
        bind(KafkaClientService.class).toInstance(kafkaClientService);
        bind(MessageCallbackFactory.class).to(MetricsMessageCallbackFactory.class);
        bind(KafkaMetricsProcessingService.class).in(Scopes.SINGLETON);
        expose(KafkaMetricsProcessingService.class);
      }

      @Provides
      @Named(MetricsConstants.ConfigKeys.KAFKA_CONSUMER_PERSIST_THRESHOLD)
      public int providesConsumerPersistThreshold(CConfiguration cConf) {
        return cConf.getInt(MetricsConstants.ConfigKeys.KAFKA_CONSUMER_PERSIST_THRESHOLD,
                            MetricsConstants.DEFAULT_KAFKA_CONSUMER_PERSIST_THRESHOLD);
      }

      @Provides
      @Named(MetricsConstants.ConfigKeys.KAFKA_TOPIC_PREFIX)
      public String providesKafkaTopicPrefix(CConfiguration cConf) {
        return cConf.get(MetricsConstants.ConfigKeys.KAFKA_TOPIC_PREFIX, MetricsConstants.DEFAULT_KAFKA_TOPIC_PREFIX);
      }

      @Provides
      @Named(MetricsConstants.ConfigKeys.KAFKA_PARTITION_SIZE)
      public int providesPartitionSize(CConfiguration cConf) {
        return cConf.getInt(MetricsConstants.ConfigKeys.KAFKA_PARTITION_SIZE,
                            MetricsConstants.DEFAULT_KAFKA_PARTITION_SIZE);
      }
    });

    processingService = injector.getInstance(KafkaMetricsProcessingService.class);
  }

  @Override
  public void start() {
    LOG.info("Starting Metrics Processor ...");
    Futures.getUnchecked(Services.chainStart(zkClientService, kafkaClientService, processingService));
  }

  @Override
  public void stop() {
    LOG.info("Stopping Metrics Processor ...");
    Futures.getUnchecked(Services.chainStop(processingService, kafkaClientService, zkClientService));
  }

  @Override
  public void destroy() {
    // no-op
  }
}

/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.runtime;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.conf.KafkaConstants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.common.runtime.DaemonMain;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data2.OperationException;
import com.continuuity.internal.kafka.client.ZKKafkaClientService;
import com.continuuity.internal.migrate.MetricsTableMigrator_2_0_to_2_1;
import com.continuuity.internal.migrate.TableMigrator;
import com.continuuity.kafka.client.KafkaClientService;
import com.continuuity.metrics.MetricsConstants;
import com.continuuity.metrics.data.DefaultMetricsTableFactory;
import com.continuuity.metrics.data.MetricsTableFactory;
import com.continuuity.metrics.guice.MetricsProcessorModule;
import com.continuuity.metrics.process.KafkaConsumerMetaTable;
import com.continuuity.metrics.process.KafkaMetricsProcessorService;
import com.continuuity.metrics.process.KafkaMetricsProcessorServiceFactory;
import com.continuuity.metrics.process.MessageCallbackFactory;
import com.continuuity.metrics.process.MetricsMessageCallbackFactory;
import com.continuuity.watchdog.election.MultiLeaderElection;
import com.continuuity.watchdog.election.PartitionChangeHandler;
import com.continuuity.weave.common.Services;
import com.continuuity.weave.zookeeper.RetryStrategies;
import com.continuuity.weave.zookeeper.ZKClient;
import com.continuuity.weave.zookeeper.ZKClientService;
import com.continuuity.weave.zookeeper.ZKClientServices;
import com.continuuity.weave.zookeeper.ZKClients;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Named;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Main class for starting a metrics processor in distributed mode.
 */
public final class MetricsProcessorMain extends DaemonMain {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsProcessorMain.class);

  private ZKClientService zkClientService;
  private KafkaClientService kafkaClientService;
  private MultiLeaderElection multiElection;
  private TableMigrator tableMigrator;
  private final SettableFuture<?> completion = SettableFuture.create();

  public static void main(String[] args) throws Exception {
    new MetricsProcessorMain().doMain(args);
  }

  @Override
  public void init(String[] args) {
    CConfiguration cConf = CConfiguration.create();
    Configuration hConf = HBaseConfiguration.create(new HdfsConfiguration());

    // Connect to Zookeeper for kafka client
    zkClientService =
      ZKClientServices.delegate(
        ZKClients.reWatchOnExpire(
          ZKClients.retryOnFailure(
            ZKClientService.Builder.of(
              cConf.get(Constants.Zookeeper.QUORUM))
              .setSessionTimeout(cConf.getInt(
              Constants.Zookeeper.CFG_SESSION_TIMEOUT_MILLIS,
              Constants.Zookeeper.DEFAULT_SESSION_TIMEOUT_MILLIS))
              .build(),
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
                                             new LocationRuntimeModule().getDistributedModules(),
                                             new DataFabricModules(cConf, hConf).getDistributedModules(),
                                             new PrivateModule() {
      @Override
      protected void configure() {
        install(new MetricsProcessorModule());
        bind(MetricsTableFactory.class).to(DefaultMetricsTableFactory.class).in(Scopes.SINGLETON);
        bind(ZKClient.class).toInstance(zkClientService);
        bind(KafkaClientService.class).toInstance(kafkaClientService);
        bind(MessageCallbackFactory.class).to(MetricsMessageCallbackFactory.class);
        bind(TableMigrator.class).to(MetricsTableMigrator_2_0_to_2_1.class);
        install(new FactoryModuleBuilder().build(KafkaMetricsProcessorServiceFactory.class));

        expose(TableMigrator.class);
        expose(KafkaMetricsProcessorServiceFactory.class);
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
      @Singleton
      public KafkaConsumerMetaTable providesKafkaConsumerMetaTable(MetricsTableFactory tableFactory) {
        return tableFactory.createKafkaConsumerMeta("default");
      }
    });

    int partitionSize = cConf.getInt(MetricsConstants.ConfigKeys.KAFKA_PARTITION_SIZE,
                                     MetricsConstants.DEFAULT_KAFKA_PARTITION_SIZE);
    multiElection = new MultiLeaderElection(
      zkClientService, "metrics-processor", partitionSize,
      createPartitionChangeHandler(injector.getInstance(KafkaMetricsProcessorServiceFactory.class)));
    tableMigrator = injector.getInstance(TableMigrator.class);
  }

  @Override
  public void start() {
    LOG.info("Starting Metrics Processor ...");
    try {
      tableMigrator.migrateIfRequired();
    } catch (OperationException e) {
      LOG.error("Error while checking for the necessity of, or execution of, a metrics table update", e);
      Throwables.propagate(e);
    }
    Futures.getUnchecked(Services.chainStart(zkClientService, kafkaClientService, multiElection));

    try {
      completion.get();
    } catch (InterruptedException e) {
      LOG.debug("Interrupted while waiting for completion.", e);
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) {
      // Propagate the execution exception will causes this process exit with error.
      LOG.error("Completed with exception. Exception get propagated", e);
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void stop() {
    LOG.info("Stopping Metrics Processor ...");
    // Stopping all services with timeout.
    try {
      Services.chainStop(multiElection, kafkaClientService, zkClientService).get(30, TimeUnit.SECONDS);
      completion.set(null);
    } catch (Exception e) {
      LOG.error("Exception while shutting down.", e);
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void destroy() {
    // no-op
  }

  private PartitionChangeHandler createPartitionChangeHandler(final KafkaMetricsProcessorServiceFactory factory) {
    return new PartitionChangeHandler() {

      private KafkaMetricsProcessorService service;

      @Override
      public void partitionsChanged(Set<Integer> partitions) {
        LOG.info("Metrics Kafka partition changed {}", partitions);
        try {
          if (service != null) {
            service.stopAndWait();
          }
          if (partitions.isEmpty() || !multiElection.isRunning()) {
            service = null;
          } else {
            service = factory.create(partitions);
            service.startAndWait();
          }
        } catch (Throwable t) {
          // Any exception happened during partition change would cause the MetricsProcessorMain exit.
          // It assumes that the monitoring daemon would restart the process.
          LOG.error("Failed to change Kafka partition. Terminating", t);
          completion.setException(t);
        }
      }
    };
  }
}

/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.runtime;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data2.OperationException;
import com.continuuity.internal.migrate.TableMigrator;
import com.continuuity.metrics.MetricsConstants;
import com.continuuity.metrics.process.KafkaMetricsProcessorServiceFactory;
import com.continuuity.watchdog.election.MultiLeaderElection;
import com.continuuity.watchdog.election.PartitionChangeHandler;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;
import org.apache.twill.common.Services;
import org.apache.twill.zookeeper.ZKClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Metrics processor service that processes events from Kafka.
 */
public final class KafkaMetricsProcessorService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaMetricsProcessorService.class);

  private final MultiLeaderElection multiElection;
  private final TableMigrator tableMigrator;
  private final ZKClientService zkClientService;
  private final SettableFuture<?> completion = SettableFuture.create();

  @Inject
  public KafkaMetricsProcessorService(CConfiguration conf, TableMigrator tableMigrator,
                                      ZKClientService zkClientService,
                                      KafkaMetricsProcessorServiceFactory kafkaMetricsProcessorServiceFactory) {
    this.zkClientService = zkClientService;
    this.tableMigrator = tableMigrator;

    int partitionSize = conf.getInt(MetricsConstants.ConfigKeys.KAFKA_PARTITION_SIZE,
                                    MetricsConstants.DEFAULT_KAFKA_PARTITION_SIZE);
    multiElection = new MultiLeaderElection(
      zkClientService, "metrics-processor", partitionSize,
      createPartitionChangeHandler(kafkaMetricsProcessorServiceFactory));

  }


  @Override
  protected void startUp() throws Exception {
    try {
      LOG.info("Starting Metrics Processor ...");
      tableMigrator.migrateIfRequired();
    } catch (OperationException e) {
      LOG.error("Error while checking for the necessity of, or execution of, a metrics table update", e);
      Throwables.propagate(e);
    }
    Futures.getUnchecked(Services.chainStart(multiElection));

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
  protected void shutDown() throws Exception {
    LOG.info("Stopping Metrics Processor ...");
    // Stopping all services with timeout.
    try {
      Services.chainStop(multiElection).get(30, TimeUnit.SECONDS);
      completion.set(null);
    } catch (Exception e) {
      LOG.error("Exception while shutting down.", e);
      throw Throwables.propagate(e);
    }
  }

  private PartitionChangeHandler createPartitionChangeHandler(final KafkaMetricsProcessorServiceFactory factory) {
    return new PartitionChangeHandler() {

      private com.continuuity.metrics.process.KafkaMetricsProcessorService service;

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

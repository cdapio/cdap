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
package co.cask.cdap.metrics.runtime;

import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.metrics.process.KafkaMetricsProcessorServiceFactory;
import co.cask.cdap.watchdog.election.MultiLeaderElection;
import co.cask.cdap.watchdog.election.PartitionChangeHandler;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;
import org.apache.twill.zookeeper.ZKClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.Executor;
import javax.annotation.Nullable;

/**
 * Metrics processor service that processes events from Kafka.
 */
public final class KafkaMetricsProcessorService extends AbstractExecutionThreadService {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaMetricsProcessorService.class);

  private final MultiLeaderElection multiElection;
  private final SettableFuture<?> completion;

  @Nullable
  private MetricsContext metricsContext;

  @Inject
  public KafkaMetricsProcessorService(CConfiguration conf,
                                      ZKClientService zkClientService,
                                      KafkaMetricsProcessorServiceFactory kafkaMetricsProcessorServiceFactory) {
    int partitionSize = conf.getInt(Constants.Metrics.KAFKA_PARTITION_SIZE,
                                    Constants.Metrics.DEFAULT_KAFKA_PARTITION_SIZE);
    multiElection = new MultiLeaderElection(
      zkClientService, "metrics-processor", partitionSize,
      createPartitionChangeHandler(kafkaMetricsProcessorServiceFactory));
    this.completion = SettableFuture.create();
  }

  public void setMetricsContext(MetricsContext metricsContext) {
    this.metricsContext = metricsContext;
  }

  @Override
  protected Executor executor() {
    return new Executor() {
      @Override
      public void execute(Runnable command) {
        Thread t = new Thread(command, getServiceName());
        t.setDaemon(true);
        t.start();
      }
    };
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting Metrics Processor ...");
    multiElection.start();
  }

  @Override
  protected void run() throws Exception {
    completion.get();
  }

  @Override
  protected void triggerShutdown() {
    completion.set(null);
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping Metrics Processor ...");
    try {
      multiElection.stop();
    } catch (Exception e) {
      LOG.error("Exception while shutting down.", e);
      throw Throwables.propagate(e);
    }
  }

  private PartitionChangeHandler createPartitionChangeHandler(final KafkaMetricsProcessorServiceFactory factory) {
    return new PartitionChangeHandler() {

      private co.cask.cdap.metrics.process.KafkaMetricsProcessorService service;

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
            service.setMetricsContext(metricsContext);
            service.startAndWait();
          }
        } catch (Throwable t) {
          LOG.error("Failed to change Kafka partition.", t);
          completion.setException(t);
        }
      }
    };
  }
}

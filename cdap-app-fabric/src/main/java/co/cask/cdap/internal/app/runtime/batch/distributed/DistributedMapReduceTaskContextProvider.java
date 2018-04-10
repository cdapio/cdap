/*
 * Copyright © 2014-2018 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.batch.distributed;

import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.app.guice.DistributedProgramContainerModule;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.SystemArguments;
import co.cask.cdap.internal.app.runtime.batch.MapReduceClassLoader;
import co.cask.cdap.internal.app.runtime.batch.MapReduceContextConfig;
import co.cask.cdap.internal.app.runtime.batch.MapReduceTaskContextProvider;
import co.cask.cdap.logging.appender.LogAppenderInitializer;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.internal.Services;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.zookeeper.ZKClientService;

import java.util.List;

/**
 * A {@link MapReduceTaskContextProvider} used in distributed mode. It creates a separate injector
 * that is used for the whole task process. It starts the necessary CDAP system services (ZK, Kafka, Metrics, Logging)
 * for the process. This class should only be used by {@link MapReduceClassLoader}.
 */
public final class DistributedMapReduceTaskContextProvider extends MapReduceTaskContextProvider {

  private final ZKClientService zkClientService;
  private final KafkaClientService kafkaClientService;
  private final MetricsCollectionService metricsCollectionService;
  private final MapReduceContextConfig mapReduceContextConfig;
  private final LogAppenderInitializer logAppenderInitializer;

  public DistributedMapReduceTaskContextProvider(CConfiguration cConf, Configuration hConf,
                                                 MapReduceClassLoader mapReduceClassLoader) {
    super(createInjector(cConf, hConf), mapReduceClassLoader);

    Injector injector = getInjector();

    this.zkClientService = injector.getInstance(ZKClientService.class);
    this.kafkaClientService = injector.getInstance(KafkaClientService.class);
    this.metricsCollectionService = injector.getInstance(MetricsCollectionService.class);
    this.logAppenderInitializer = injector.getInstance(LogAppenderInitializer.class);
    this.mapReduceContextConfig = new MapReduceContextConfig(hConf);
  }

  @Override
  protected void startUp() throws Exception {
    super.startUp();
    try {
      List<ListenableFuture<State>> startFutures = Services.chainStart(zkClientService,
                                                                       kafkaClientService,
                                                                       metricsCollectionService).get();
      // All services should be started
      for (ListenableFuture<State> future : startFutures) {
        Preconditions.checkState(future.get() == State.RUNNING, "Failed to start services: %s, %s, %s, %s",
                                 zkClientService, kafkaClientService, metricsCollectionService);
      }
      logAppenderInitializer.initialize();
      ProgramOptions programOptions = mapReduceContextConfig.getProgramOptions();
      SystemArguments.setLogLevel(programOptions.getUserArguments(), logAppenderInitializer);
    } catch (Exception e) {
      // Try our best to stop services. Chain stop guarantees it will stop everything, even some of them failed.
      try {
        shutDown();
      } catch (Exception stopEx) {
        e.addSuppressed(stopEx);
      }
      throw e;
    }
  }

  @Override
  protected void shutDown() throws Exception {
    super.shutDown();
    Exception failure = null;
    try {
      logAppenderInitializer.close();
    } catch (Exception e) {
      failure = e;
    }
    try {
      Services.chainStop(metricsCollectionService, kafkaClientService, zkClientService).get();
    } catch (Exception e) {
      if (failure != null) {
        failure.addSuppressed(e);
      } else {
        failure = e;
      }
    }
    if (failure != null) {
      throw failure;
    }
  }

  private static Injector createInjector(CConfiguration cConf, Configuration hConf) {
    MapReduceContextConfig mapReduceContextConfig = new MapReduceContextConfig(hConf);
    // principal will be null if running on a kerberos distributed cluster
    Arguments arguments = mapReduceContextConfig.getProgramOptions().getArguments();
    String principal = arguments.getOption(ProgramOptionConstants.PRINCIPAL);
    String runId = arguments.getOption(ProgramOptionConstants.RUN_ID);
    String instanceId = arguments.getOption(ProgramOptionConstants.INSTANCE_ID);
    return Guice.createInjector(
      DistributedProgramContainerModule
        .builder(cConf, hConf, mapReduceContextConfig.getProgramId().run(runId), instanceId)
        .setPrincipal(principal)
        .build()
    );
  }
}

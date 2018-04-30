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
import co.cask.cdap.app.guice.ClusterMode;
import co.cask.cdap.app.guice.DistributedProgramContainerModule;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.ProgramRunners;
import co.cask.cdap.internal.app.runtime.SystemArguments;
import co.cask.cdap.internal.app.runtime.batch.MapReduceClassLoader;
import co.cask.cdap.internal.app.runtime.batch.MapReduceContextConfig;
import co.cask.cdap.internal.app.runtime.batch.MapReduceTaskContextProvider;
import co.cask.cdap.logging.appender.LogAppenderInitializer;
import com.google.common.util.concurrent.Service;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.zookeeper.ZKClientService;

import java.util.Deque;
import java.util.LinkedList;

/**
 * A {@link MapReduceTaskContextProvider} used in distributed mode. It creates a separate injector
 * that is used for the whole task process. It starts the necessary CDAP system services (ZK, Kafka, Metrics, Logging)
 * for the process. This class should only be used by {@link MapReduceClassLoader}.
 */
public final class DistributedMapReduceTaskContextProvider extends MapReduceTaskContextProvider {

  private final Deque<Service> coreServices;
  private final MapReduceContextConfig mapReduceContextConfig;
  private final LogAppenderInitializer logAppenderInitializer;

  public DistributedMapReduceTaskContextProvider(CConfiguration cConf, Configuration hConf,
                                                 MapReduceClassLoader mapReduceClassLoader) {
    super(createInjector(cConf, hConf), mapReduceClassLoader);

    MapReduceContextConfig mapReduceContextConfig = new MapReduceContextConfig(hConf);

    Injector injector = getInjector();

    Deque<Service> coreServices = new LinkedList<>();
    coreServices.add(injector.getInstance(ZKClientService.class));
    coreServices.add(injector.getInstance(MetricsCollectionService.class));

    if (ProgramRunners.getClusterMode(mapReduceContextConfig.getProgramOptions()) == ClusterMode.ON_PREMISE) {
      coreServices.add(injector.getInstance(KafkaClientService.class));
    }

    this.coreServices = coreServices;
    this.logAppenderInitializer = injector.getInstance(LogAppenderInitializer.class);
    this.mapReduceContextConfig = new MapReduceContextConfig(hConf);
  }

  @Override
  protected void startUp() throws Exception {
    super.startUp();
    try {
      for (Service service : coreServices) {
        service.startAndWait();
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
    for (Service service : (Iterable<Service>) coreServices::descendingIterator) {
      try {
        service.stopAndWait();
      } catch (Exception e) {
        if (failure != null) {
          failure.addSuppressed(e);
        } else {
          failure = e;
        }
      }
    }
    if (failure != null) {
      throw failure;
    }
  }

  private static Injector createInjector(CConfiguration cConf, Configuration hConf) {
    MapReduceContextConfig mapReduceContextConfig = new MapReduceContextConfig(hConf);
    // principal will be null if running on a kerberos distributed cluster
    ProgramOptions programOptions = mapReduceContextConfig.getProgramOptions();
    Arguments systemArgs = programOptions.getArguments();
    String principal = systemArgs.getOption(ProgramOptionConstants.PRINCIPAL);
    String runId = systemArgs.getOption(ProgramOptionConstants.RUN_ID);
    String instanceId = systemArgs.getOption(ProgramOptionConstants.INSTANCE_ID);
    return Guice.createInjector(
      DistributedProgramContainerModule
        .builder(cConf, hConf, mapReduceContextConfig.getProgramId().run(runId), instanceId)
        .setPrincipal(principal)
        .setClusterMode(ProgramRunners.getClusterMode(programOptions))
        .build()
    );
  }
}

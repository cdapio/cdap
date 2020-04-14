/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.batch.distributed;

import com.google.common.util.concurrent.Service;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.app.guice.ClusterMode;
import io.cdap.cdap.app.guice.DistributedProgramContainerModule;
import io.cdap.cdap.app.runtime.Arguments;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.ProgramRunners;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.runtime.batch.MapReduceClassLoader;
import io.cdap.cdap.internal.app.runtime.batch.MapReduceContextConfig;
import io.cdap.cdap.internal.app.runtime.batch.MapReduceTaskContextProvider;
import io.cdap.cdap.internal.app.runtime.monitor.RuntimeMonitors;
import io.cdap.cdap.logging.appender.LogAppenderInitializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.zookeeper.ZKClientService;

import java.net.Authenticator;
import java.net.ProxySelector;
import java.util.Deque;
import java.util.LinkedList;

/**
 * A {@link MapReduceTaskContextProvider} used in distributed mode. It creates a separate injector
 * that is used for the whole task process. It starts the necessary CDAP system services (ZK, Kafka, Metrics, Logging)
 * for the process. This class should only be used by {@link MapReduceClassLoader}.
 */
public final class DistributedMapReduceTaskContextProvider extends MapReduceTaskContextProvider {

  private final ClusterMode clusterMode;
  private final Deque<Service> coreServices;
  private final MapReduceContextConfig mapReduceContextConfig;
  private final LogAppenderInitializer logAppenderInitializer;
  private ProxySelector oldProxySelector;

  public DistributedMapReduceTaskContextProvider(CConfiguration cConf, Configuration hConf,
                                                 MapReduceClassLoader mapReduceClassLoader) {
    super(createInjector(cConf, hConf), mapReduceClassLoader);

    MapReduceContextConfig mapReduceContextConfig = new MapReduceContextConfig(hConf);
    this.clusterMode = ProgramRunners.getClusterMode(mapReduceContextConfig.getProgramOptions());

    Injector injector = getInjector();

    Deque<Service> coreServices = new LinkedList<>();
    if (clusterMode == ClusterMode.ON_PREMISE) {
      coreServices.add(injector.getInstance(ZKClientService.class));
      coreServices.add(injector.getInstance(KafkaClientService.class));
    }
    coreServices.add(injector.getInstance(MetricsCollectionService.class));

    this.coreServices = coreServices;
    this.logAppenderInitializer = injector.getInstance(LogAppenderInitializer.class);
    this.mapReduceContextConfig = new MapReduceContextConfig(hConf);
  }

  @Override
  protected void startUp() throws Exception {
    super.startUp();
    ProgramOptions programOptions = mapReduceContextConfig.getProgramOptions();
    try {
      oldProxySelector = ProxySelector.getDefault();
      if (clusterMode == ClusterMode.ISOLATED) {
        RuntimeMonitors.setupMonitoring(getInjector(), programOptions);
      }

      for (Service service : coreServices) {
        service.startAndWait();
      }
      logAppenderInitializer.initialize();
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

    Authenticator.setDefault(null);
    ProxySelector.setDefault(oldProxySelector);

    if (failure != null) {
      throw failure;
    }
  }

  private static Injector createInjector(CConfiguration cConf, Configuration hConf) {
    MapReduceContextConfig mapReduceContextConfig = new MapReduceContextConfig(hConf);
    // principal will be null if running on a kerberos distributed cluster
    ProgramOptions programOptions = mapReduceContextConfig.getProgramOptions();
    Arguments systemArgs = programOptions.getArguments();
    String runId = systemArgs.getOption(ProgramOptionConstants.RUN_ID);
    return Guice.createInjector(
      new DistributedProgramContainerModule(cConf, hConf, mapReduceContextConfig.getProgramId().run(runId),
                                            programOptions)
    );
  }
}

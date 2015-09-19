/*
 * Copyright © 2014-2015 Cask Data, Inc.
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
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.guice.KafkaClientModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.data.runtime.DataFabricDistributedModule;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data2.transaction.metrics.TransactionManagerMetricsCollector;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.HBaseTableUtilFactory;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.internal.app.runtime.batch.MapReduceClassLoader;
import co.cask.cdap.internal.app.runtime.batch.MapReduceTaskContextProvider;
import co.cask.cdap.logging.appender.LogAppender;
import co.cask.cdap.logging.appender.LogAppenderInitializer;
import co.cask.cdap.logging.appender.kafka.KafkaLogAppender;
import co.cask.cdap.metrics.guice.MetricsClientRuntimeModule;
import co.cask.tephra.distributed.ThriftClientProvider;
import co.cask.tephra.metrics.TxMetricsCollector;
import co.cask.tephra.runtime.TransactionModules;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
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
  private final LogAppenderInitializer logAppenderInitializer;

  public DistributedMapReduceTaskContextProvider(CConfiguration cConf, Configuration hConf) {
    super(createInjector(cConf, hConf));

    Injector injector = getInjector();

    this.zkClientService = injector.getInstance(ZKClientService.class);
    this.kafkaClientService = injector.getInstance(KafkaClientService.class);
    this.metricsCollectionService = injector.getInstance(MetricsCollectionService.class);
    this.logAppenderInitializer = injector.getInstance(LogAppenderInitializer.class);
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
        Preconditions.checkState(future.get() == State.RUNNING,
                                 "Failed to start services: zkClient %s, kafkaClient %s, metricsCollection %s",
                                 zkClientService.state(), kafkaClientService.state(), metricsCollectionService.state());
      }
      logAppenderInitializer.initialize();
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
    return Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new LocationRuntimeModule().getDistributedModules(),
      new IOModule(),
      new ZKClientModule(),
      new KafkaClientModule(),
      new DiscoveryRuntimeModule().getDistributedModules(),
      new MetricsClientRuntimeModule().getDistributedModules(),
      new ExploreClientModule(),
      new DataSetsModules().getDistributedModules(),
      new TransactionModules().getDistributedModules(),
      new AbstractModule() {
        @Override
        protected void configure() {
          // Data-fabric bindings
          bind(HBaseTableUtil.class).toProvider(HBaseTableUtilFactory.class);
          // For log publishing
          bind(LogAppender.class).to(KafkaLogAppender.class);
          // For transaction modules
          bind(TxMetricsCollector.class).to(TransactionManagerMetricsCollector.class).in(Scopes.SINGLETON);
          bind(ThriftClientProvider.class).toProvider(DataFabricDistributedModule.ThriftClientProviderSupplier.class);
        }
      }
    );
  }
}

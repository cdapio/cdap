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

package co.cask.cdap.data.preferences;

import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.hooks.MetricsReporterHook;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.common.logging.ServiceLoggingContext;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.data.Namespace;
import co.cask.cdap.data2.datafabric.DefaultDatasetNamespace;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.NamespacedDatasetFramework;
import co.cask.cdap.data2.dataset2.lib.table.PreferenceTable;
import co.cask.cdap.data2.dataset2.lib.table.PreferenceTableDataset;
import co.cask.cdap.gateway.handlers.PingHandler;
import co.cask.http.HttpHandler;
import co.cask.http.NettyHttpService;
import co.cask.tephra.TransactionExecutorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import javax.annotation.Nullable;
import javax.inject.Inject;

/**
 *
 */
public class PreferencesHttpService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(PreferencesHttpService.class);
  private final DiscoveryService discoveryService;
  private final NettyHttpService.Builder builder;
  private final DatasetFramework framework;
  private final TransactionExecutorFactory executorFactory;
  private NettyHttpService httpService;
  private Cancellable cancellable;

  @Inject
  public PreferencesHttpService(CConfiguration cConf, DiscoveryService discoveryService,
                                DatasetFramework dsFramework, TransactionExecutorFactory executorFactory,
                                @Nullable MetricsCollectionService metricsCollectionService) {
    String address = cConf.get(Constants.Preferences.ADDRESS);
    int backlogcnxs = cConf.getInt(Constants.Preferences.BACKLOG_CONNECTIONS, 20000);
    int execthreads = cConf.getInt(Constants.Preferences.EXEC_THREADS, 20);
    int bossthreads = cConf.getInt(Constants.Preferences.BOSS_THREADS, 1);
    int workerthreads = cConf.getInt(Constants.Preferences.WORKER_THREADS, 10);
    builder = NettyHttpService.builder();

    builder.setHandlerHooks(ImmutableList.of(new MetricsReporterHook(metricsCollectionService,
                                                                     Constants.Preferences.HTTP_HANDLER)));
    builder.setHost(address);
    builder.setConnectionBacklog(backlogcnxs);
    builder.setExecThreadPoolSize(execthreads);
    builder.setBossThreadPoolSize(bossthreads);
    builder.setWorkerThreadPoolSize(workerthreads);

    this.discoveryService = discoveryService;
    this.executorFactory = executorFactory;
    this.framework = new NamespacedDatasetFramework(dsFramework, new DefaultDatasetNamespace(cConf, Namespace.SYSTEM));
    LOG.info("Configuring ConfigService " +
               ", address: " + address +
               ", backlog connections: " + backlogcnxs +
               ", execthreads: " + execthreads +
               ", bossthreads: " + bossthreads +
               ", workerthreads: " + workerthreads);
  }

  @Override
  protected void startUp() throws Exception {
    LoggingContextAccessor.setLoggingContext(new ServiceLoggingContext(Constants.Logging.SYSTEM_NAME,
                                                                       Constants.Logging.COMPONENT_NAME,
                                                                       Constants.Service.PREFERENCES));
    PreferenceTableDataset table = DatasetsUtil.getOrCreateDataset(framework, Constants.Preferences.PROPERTY_TABLE,
                                                                   PreferenceTable.class.getName(),
                                                                   DatasetProperties.EMPTY, null, null);
    builder.addHttpHandlers(ImmutableList.<HttpHandler>of(new PreferencesHandler(table, executorFactory),
                                                          new PingHandler()));
    httpService = builder.build();
    LOG.info("Starting Config Service...");
    httpService.startAndWait();
    LOG.info("Started Config Service...");
    cancellable = discoveryService.register(new Discoverable() {
      @Override
      public String getName() {
        return Constants.Service.PREFERENCES;
      }

      @Override
      public InetSocketAddress getSocketAddress() {
        return httpService.getBindAddress();
      }
    });

    LOG.info("Config Service started successfully on {}", httpService.getBindAddress());
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping Config Service");
    cancellable.cancel();
    httpService.stopAndWait();
  }
}

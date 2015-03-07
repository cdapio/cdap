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

package co.cask.cdap.data2.datafabric.dataset.service;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.hooks.MetricsReporterHook;
import co.cask.cdap.common.http.CommonNettyHttpServiceBuilder;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.data2.datafabric.dataset.instance.DatasetInstanceManager;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutor;
import co.cask.cdap.data2.datafabric.dataset.service.mds.MDSDatasetsRegistry;
import co.cask.cdap.data2.datafabric.dataset.type.DatasetTypeManager;
import co.cask.cdap.data2.metrics.DatasetMetricsReporter;
import co.cask.cdap.explore.client.ExploreFacade;
import co.cask.http.NettyHttpService;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * DatasetService implemented using the common http netty framework.
 */
public class DatasetService extends AbstractExecutionThreadService {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetService.class);

  private final NettyHttpService httpService;
  private final DiscoveryService discoveryService;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final DatasetOpExecutor opExecutorClient;
  private final Set<DatasetMetricsReporter> metricReporters;
  private Cancellable cancelDiscovery;

  private final DatasetTypeManager typeManager;
  private final MDSDatasetsRegistry mdsDatasets;

  private Cancellable opExecutorServiceWatch;
  private SettableFuture<ServiceDiscovered> opExecutorDiscovered;
  private volatile boolean stopping = false;

  @Inject
  public DatasetService(CConfiguration cConf,
                        LocationFactory locationFactory,
                        DiscoveryService discoveryService,
                        DiscoveryServiceClient discoveryServiceClient,
                        DatasetTypeManager typeManager,
                        DatasetInstanceManager instanceManager,
                        MetricsCollectionService metricsCollectionService,
                        DatasetOpExecutor opExecutorClient,
                        MDSDatasetsRegistry mdsDatasets,
                        ExploreFacade exploreFacade,
                        Set<DatasetMetricsReporter> metricReporters,
                        UnderlyingSystemNamespaceAdmin underlyingSystemNamespaceAdmin) throws Exception {

    this.typeManager = typeManager;
    DatasetTypeHandler datasetTypeHandler = new DatasetTypeHandler(typeManager, locationFactory, cConf);
    DatasetTypeHandlerV2 datasetTypeHandlerV2 = new DatasetTypeHandlerV2(datasetTypeHandler);
    DatasetInstanceHandler datasetInstanceHandler = new DatasetInstanceHandler(typeManager, instanceManager,
                                                                               opExecutorClient, exploreFacade);
    DatasetInstanceHandlerV2 datasetInstanceHandlerV2 = new DatasetInstanceHandlerV2(datasetInstanceHandler);
    UnderlyingSystemNamespaceHandler underlyingSystemNamespaceHandler =
      new UnderlyingSystemNamespaceHandler(underlyingSystemNamespaceAdmin);
    NettyHttpService.Builder builder = new CommonNettyHttpServiceBuilder(cConf);
    builder.addHttpHandlers(ImmutableList.of(datasetTypeHandler, datasetTypeHandlerV2,
                                             datasetInstanceHandler, datasetInstanceHandlerV2,
                                             underlyingSystemNamespaceHandler));

    builder.setHandlerHooks(ImmutableList.of(new MetricsReporterHook(metricsCollectionService,
                                                                     Constants.Service.DATASET_MANAGER)));

    builder.setHost(cConf.get(Constants.Dataset.Manager.ADDRESS));

    builder.setConnectionBacklog(cConf.getInt(Constants.Dataset.Manager.BACKLOG_CONNECTIONS,
                                              Constants.Dataset.Manager.DEFAULT_BACKLOG));
    builder.setExecThreadPoolSize(cConf.getInt(Constants.Dataset.Manager.EXEC_THREADS,
                                               Constants.Dataset.Manager.DEFAULT_EXEC_THREADS));
    builder.setBossThreadPoolSize(cConf.getInt(Constants.Dataset.Manager.BOSS_THREADS,
                                               Constants.Dataset.Manager.DEFAULT_BOSS_THREADS));
    builder.setWorkerThreadPoolSize(cConf.getInt(Constants.Dataset.Manager.WORKER_THREADS,
                                                 Constants.Dataset.Manager.DEFAULT_WORKER_THREADS));

    this.httpService = builder.build();
    this.discoveryService = discoveryService;
    this.discoveryServiceClient = discoveryServiceClient;
    this.opExecutorClient = opExecutorClient;
    this.mdsDatasets = mdsDatasets;
    this.metricReporters = metricReporters;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting DatasetService...");

    mdsDatasets.startAndWait();
    typeManager.startAndWait();
    opExecutorClient.startAndWait();
    httpService.startAndWait();

    // setting watch for ops executor service that we need to be running to operate correctly
    ServiceDiscovered discover = discoveryServiceClient.discover(Constants.Service.DATASET_EXECUTOR);
    opExecutorDiscovered = SettableFuture.create();
    opExecutorServiceWatch = discover.watchChanges(
      new ServiceDiscovered.ChangeListener() {
        @Override
        public void onChange(ServiceDiscovered serviceDiscovered) {
          if (!Iterables.isEmpty(serviceDiscovered)) {
            LOG.info("Discovered {} service", Constants.Service.DATASET_EXECUTOR);
            opExecutorDiscovered.set(serviceDiscovered);
          }
        }
      }, MoreExecutors.sameThreadExecutor());

    for (DatasetMetricsReporter metricsReporter : metricReporters) {
      metricsReporter.start();
    }
  }

  @Override
  protected String getServiceName() {
    return "DatasetService";
  }

  @Override
  protected void run() throws Exception {
    waitForOpExecutorToStart();

    LOG.info("Announcing DatasetService for discovery...");
    // Register the service
    cancelDiscovery = discoveryService.register(new Discoverable() {
      @Override
      public String getName() {
        return Constants.Service.DATASET_MANAGER;
      }

      @Override
      public InetSocketAddress getSocketAddress() {
        return httpService.getBindAddress();
      }
    });

    LOG.info("DatasetService started successfully on {}", httpService.getBindAddress());
    while (isRunning()) {
      try {
        TimeUnit.SECONDS.sleep(1);
      } catch (InterruptedException e) {
        // It's triggered by stop
        Thread.currentThread().interrupt();
        break;
      }
    }
  }

  private void waitForOpExecutorToStart() throws Exception {
    LOG.info("Waiting for {} service to be discoverable", Constants.Service.DATASET_EXECUTOR);
    while (!stopping) {
      try {
        opExecutorDiscovered.get(1, TimeUnit.SECONDS);
        opExecutorServiceWatch.cancel();
        break;
      } catch (TimeoutException e) {
        // re-try
      } catch (InterruptedException e) {
        LOG.warn("Got interrupted while waiting for service {}", Constants.Service.DATASET_EXECUTOR);
        Thread.currentThread().interrupt();
        opExecutorServiceWatch.cancel();
        break;
      } catch (ExecutionException e) {
        LOG.error("Error during discovering service {}, DatasetService start failed",
                  Constants.Service.DATASET_EXECUTOR);
        opExecutorServiceWatch.cancel();
        throw e;
      }
    }
  }

  @Override
  protected void triggerShutdown() {
    stopping = true;
    super.triggerShutdown();
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping DatasetService...");

    for (DatasetMetricsReporter metricsReporter : metricReporters) {
      metricsReporter.stop();
    }

    if (opExecutorServiceWatch != null) {
      opExecutorServiceWatch.cancel();
    }
    
    mdsDatasets.shutDown();

    typeManager.stopAndWait();

    if (cancelDiscovery != null) {
      cancelDiscovery.cancel();
    }

    // Wait for a few seconds for requests to stop
    try {
      TimeUnit.SECONDS.sleep(3);
    } catch (InterruptedException e) {
      LOG.error("Interrupted while waiting...", e);
    }

    httpService.stopAndWait();
    opExecutorClient.stopAndWait();
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("bindAddress", httpService.getBindAddress())
      .toString();
  }
}

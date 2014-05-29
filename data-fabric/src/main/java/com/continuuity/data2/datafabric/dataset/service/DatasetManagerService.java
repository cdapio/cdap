package com.continuuity.data2.datafabric.dataset.service;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.hooks.MetricsReporterHook;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data2.datafabric.ReactorDatasetNamespace;
import com.continuuity.data2.datafabric.dataset.instance.DatasetInstanceManager;
import com.continuuity.data2.datafabric.dataset.type.DatasetTypeManager;
import com.continuuity.data2.dataset2.executor.DatasetOpExecutor;
import com.continuuity.data2.dataset2.manager.DatasetManager;
import com.continuuity.data2.dataset2.manager.NamespacedDatasetManager;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.http.NettyHttpService;
import com.continuuity.internal.data.dataset.module.DatasetModule;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

/**
 * DatasetManagerService implemented using the common http netty framework.
 */
public class DatasetManagerService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetManagerService.class);

  private final NettyHttpService httpService;
  private final DiscoveryService discoveryService;
  private final DatasetOpExecutor opExecutorClient;
  private Cancellable cancelDiscovery;

  private final DatasetInstanceManager instanceManager;
  private final DatasetTypeManager typeManager;

  private final DatasetManager mdsDatasetManager;
  private final SortedMap<String, Class<? extends DatasetModule>> defaultModules;

  @Inject
  public DatasetManagerService(CConfiguration cConf,
                               LocationFactory locationFactory,
                               DiscoveryService discoveryService,
                               DiscoveryServiceClient discoveryServiceClient,
                               @Named("datasetMDS") DatasetManager mdsDatasetManager,
                               @Named("defaultDatasetModules")
                               SortedMap<String, Class<? extends DatasetModule>> defaultModules,
                               TransactionSystemClient txSystemClient,
                               MetricsCollectionService metricsCollectionService,
                               DatasetOpExecutor opExecutorClient) throws Exception {

    NettyHttpService.Builder builder = NettyHttpService.builder();

    // todo: refactor once DataSetAccessor is removed.
    this.mdsDatasetManager =
      new NamespacedDatasetManager(mdsDatasetManager,
                                   new ReactorDatasetNamespace(cConf, DataSetAccessor.Namespace.SYSTEM));
    this.defaultModules = defaultModules;

    this.typeManager = new DatasetTypeManager(mdsDatasetManager, txSystemClient, locationFactory, defaultModules);
    this.instanceManager = new DatasetInstanceManager(mdsDatasetManager, txSystemClient);

    builder.addHttpHandlers(ImmutableList.of(new DatasetTypeHandler(typeManager, locationFactory, cConf),
                                             new DatasetInstanceHandler(discoveryServiceClient, typeManager,
                                                                        instanceManager, opExecutorClient)));

    builder.setHandlerHooks(ImmutableList.of(new MetricsReporterHook(metricsCollectionService,
                                                                     Constants.Service.DATASET_MANAGER)));

    builder.setHost(cConf.get(Constants.Dataset.Manager.ADDRESS));
    builder.setPort(cConf.getInt(Constants.Dataset.Manager.PORT));

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
    this.opExecutorClient = opExecutorClient;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting DatasetManagerService...");

    // adding default modules to init dataset manager used by mds (directly)
    for (Map.Entry<String, Class<? extends DatasetModule>> module : defaultModules.entrySet()) {
      mdsDatasetManager.register(module.getKey(), module.getValue());
    }

    typeManager.startAndWait();
    instanceManager.startAndWait();

    opExecutorClient.startAndWait();
    httpService.startAndWait();

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

    LOG.info("DatasetManagerService started successfully on {}", httpService.getBindAddress());
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping DatasetManagerService...");

    typeManager.stopAndWait();
    instanceManager.stopAndWait();

    // Unregister the service
    cancelDiscovery.cancel();
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

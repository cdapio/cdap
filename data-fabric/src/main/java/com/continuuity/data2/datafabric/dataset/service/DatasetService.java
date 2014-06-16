package com.continuuity.data2.datafabric.dataset.service;

import com.continuuity.api.dataset.module.DatasetModule;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.hooks.MetricsReporterHook;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data2.datafabric.ReactorDatasetNamespace;
import com.continuuity.data2.datafabric.dataset.instance.DatasetInstanceManager;
import com.continuuity.data2.datafabric.dataset.service.executor.DatasetOpExecutor;
import com.continuuity.data2.datafabric.dataset.type.DatasetTypeManager;
import com.continuuity.data2.dataset2.DatasetFramework;
import com.continuuity.data2.dataset2.NamespacedDatasetFramework;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.http.NettyHttpService;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * DatasetService implemented using the common http netty framework.
 */
public class DatasetService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetService.class);

  private final NettyHttpService httpService;
  private final DiscoveryService discoveryService;
  private final DatasetOpExecutor opExecutorClient;
  private Cancellable cancelDiscovery;

  private final DatasetInstanceManager instanceManager;
  private final DatasetTypeManager typeManager;

  private final DatasetFramework mdsDatasetFramework;
  private final Map<String, DatasetModule> defaultModules;

  @Inject
  public DatasetService(CConfiguration cConf,
                        LocationFactory locationFactory,
                        DiscoveryService discoveryService,
                        @Named("datasetMDS") DatasetFramework mdsDatasetFramework,
                        @Named("defaultDatasetModules")
                        Map<String, DatasetModule> defaultModules,
                        TransactionSystemClient txSystemClient,
                        MetricsCollectionService metricsCollectionService,
                        DatasetOpExecutor opExecutorClient) throws Exception {

    NettyHttpService.Builder builder = NettyHttpService.builder();

    // todo: refactor once DataSetAccessor is removed.
    this.mdsDatasetFramework =
      new NamespacedDatasetFramework(mdsDatasetFramework,
                                     new ReactorDatasetNamespace(cConf, DataSetAccessor.Namespace.SYSTEM));
    // NOTE: order matters
    this.defaultModules = Maps.newLinkedHashMap(defaultModules);
    this.typeManager = new DatasetTypeManager(mdsDatasetFramework, txSystemClient, locationFactory, defaultModules);
    this.instanceManager = new DatasetInstanceManager(mdsDatasetFramework, txSystemClient);

    builder.addHttpHandlers(ImmutableList.of(new DatasetTypeHandler(typeManager, locationFactory, cConf),
                                             new DatasetInstanceHandler(typeManager, instanceManager,
                                                                        opExecutorClient)));

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
    LOG.info("Starting DatasetService...");

    // adding default modules to init dataset manager used by mds (directly)
    for (Map.Entry<String, DatasetModule> module : defaultModules.entrySet()) {
      mdsDatasetFramework.addModule(module.getKey(), module.getValue());
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

    LOG.info("DatasetService started successfully on {}", httpService.getBindAddress());
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping DatasetService...");

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

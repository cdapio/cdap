package com.continuuity.data2.datafabric.dataset.service;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.http.core.NettyHttpService;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.data2.datafabric.dataset.instance.DatasetInstanceManager;
import com.continuuity.data2.datafabric.dataset.type.DatasetTypeManager;
import com.continuuity.data2.dataset2.manager.DatasetManager;
import com.continuuity.data2.transaction.TransactionSystemClient;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.filesystem.LocationFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * DatasetManagerService implemented using the common http netty framework.
 */
public class DatasetManagerService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetManagerService.class);

  private final NettyHttpService httpService;
  private final DiscoveryService discoveryService;
  private Cancellable cancelDiscovery;

  private final DatasetInstanceManager instanceManager;
  private final DatasetTypeManager typeManager;

  @Inject
  public DatasetManagerService(CConfiguration cConf,
                               LocationFactory locationFactory,
                               @Named(Constants.Dataset.Manager.ADDRESS) InetAddress hostname,
                               DiscoveryService discoveryService,
                               @Nullable MetricsCollectionService metricsCollectionService,
                               @Named("datasetMDS") DatasetManager datasetManager,
                               TransactionSystemClient txSystemClient) throws Exception {

    NettyHttpService.Builder builder = NettyHttpService.builder();

    // todo: to be injected?
    this.typeManager = new DatasetTypeManager(datasetManager, txSystemClient, locationFactory);
    this.instanceManager = new DatasetInstanceManager(datasetManager, txSystemClient);

    builder.addHttpHandlers(ImmutableList.of(new DatasetTypeHandler(typeManager, locationFactory, cConf),
                                             new DatasetInstanceHandler(typeManager, instanceManager)));
    // todo: collect metrics?
//    builder.setHandlerHooks(ImmutableList.of(new MetricsReporterHook(metricsCollectionService)));

    builder.setHost(hostname.getCanonicalHostName());
    builder.setPort(cConf.getInt(Constants.Dataset.Manager.PORT, Constants.Dataset.Manager.DEFAULT_PORT));

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
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting DatasetManagerService...");

    typeManager.startAndWait();
    instanceManager.startAndWait();

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
  }

  public InetSocketAddress getBindAddress() {
    return httpService.getBindAddress();
  }
}

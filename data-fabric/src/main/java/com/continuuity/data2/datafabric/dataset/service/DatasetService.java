package com.continuuity.data2.datafabric.dataset.service;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.hooks.MetricsReporterHook;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.data2.datafabric.dataset.instance.DatasetInstanceManager;
import com.continuuity.data2.datafabric.dataset.service.executor.DatasetOpExecutor;
import com.continuuity.data2.datafabric.dataset.service.mds.MDSDatasetsRegistry;
import com.continuuity.data2.datafabric.dataset.type.DatasetTypeManager;
import com.continuuity.explore.client.DatasetExploreFacade;
import com.continuuity.http.NettyHttpService;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
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

  private final DatasetTypeManager typeManager;


  private final MDSDatasetsRegistry mdsDatasets;

  @Inject
  public DatasetService(CConfiguration cConf,
                        LocationFactory locationFactory,
                        DiscoveryService discoveryService,
                        DatasetTypeManager typeManager,
                        DatasetInstanceManager instanceManager,
                        MetricsCollectionService metricsCollectionService,
                        DatasetOpExecutor opExecutorClient,
                        MDSDatasetsRegistry mdsDatasets,
                        DatasetExploreFacade datasetExploreFacade) throws Exception {

    NettyHttpService.Builder builder = NettyHttpService.builder();

    this.typeManager = typeManager;

    builder.addHttpHandlers(ImmutableList.of(new DatasetTypeHandler(typeManager, locationFactory, cConf),
                                             new DatasetInstanceHandler(typeManager, instanceManager, opExecutorClient,
                                                                        datasetExploreFacade, cConf)));

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
    this.opExecutorClient = opExecutorClient;
    this.mdsDatasets = mdsDatasets;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting DatasetService...");

    mdsDatasets.startUp();
    typeManager.startAndWait();
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

    mdsDatasets.shutDown();

    typeManager.stopAndWait();

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

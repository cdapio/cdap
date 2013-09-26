/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.app.runtime;

import com.continuuity.app.guice.AppFabricServiceRuntimeModule;
import com.continuuity.app.guice.ProgramRunnerRuntimeModule;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.conf.KafkaConstants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.common.runtime.DaemonMain;
import com.continuuity.common.zookeeper.election.ElectionHandler;
import com.continuuity.common.zookeeper.election.LeaderElection;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.internal.app.services.AppFabricServer;
import com.continuuity.internal.kafka.client.ZKKafkaClientService;
import com.continuuity.kafka.client.KafkaClientService;
import com.continuuity.metrics.guice.MetricsClientRuntimeModule;
import com.continuuity.weave.api.WeaveRunnerService;
import com.continuuity.weave.common.Cancellable;
import com.continuuity.weave.common.Services;
import com.continuuity.weave.zookeeper.RetryStrategies;
import com.continuuity.weave.zookeeper.ZKClientService;
import com.continuuity.weave.zookeeper.ZKClientServices;
import com.continuuity.weave.zookeeper.ZKClients;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * AppFabric server main. It can be started as regular Java Application
 * or started using apache commons daemon (jsvc)
 */
public final class AppFabricMain extends DaemonMain {
  private static final Logger LOG = LoggerFactory.getLogger(AppFabricMain.class);
  private ZKClientService zkClientService;
  private AppFabricServer appFabricServer;
  private MetricsCollectionService metricsCollectionService;
  private KafkaClientService kafkaClientService;
  private Injector injector;
  private LeaderElection leaderElection;
  private final List<Cancellable> electionCancel = Lists.newArrayList();

  public static void main(final String[] args) throws Exception {
    new AppFabricMain().doMain(args);
  }

  @Override
  public void init(String[] args) {
    CConfiguration cConf = CConfiguration.create();
    zkClientService =
      ZKClientServices.delegate(
        ZKClients.reWatchOnExpire(
          ZKClients.retryOnFailure(
            ZKClientService.Builder.of(cConf.get(Constants.Zookeeper.QUORUM)).setSessionTimeout(10000).build(),
            RetryStrategies.fixDelay(2, TimeUnit.SECONDS)
          )
        )
      );
    String kafkaZKNamespace = cConf.get(KafkaConstants.ConfigKeys.ZOOKEEPER_NAMESPACE_CONFIG);
    kafkaClientService = new ZKKafkaClientService(
      kafkaZKNamespace == null
        ? zkClientService
        : ZKClients.namespace(zkClientService, "/" + kafkaZKNamespace)
    );

    injector = Guice.createInjector(
      new MetricsClientRuntimeModule(kafkaClientService).getDistributedModules(),
      new ConfigModule(HBaseConfiguration.create()),
      new IOModule(),
      new LocationRuntimeModule().getDistributedModules(),
      new DiscoveryRuntimeModule(zkClientService).getDistributedModules(),
      new AppFabricServiceRuntimeModule().getDistributedModules(),
      new ProgramRunnerRuntimeModule().getDistributedModules(),
      new DataFabricModules().getDistributedModules()
    );
  }

  @Override
  public void start() {
    LeaderElection election = new LeaderElection(zkClientService, "/election/appfabric",
                                                 new ElectionHandler() {

                                                   @Override
                                                   public void leader() {
                                                     LOG.info("Starting App Fabric ...");
                                                     injector.getInstance(WeaveRunnerService.class).startAndWait();
                                                     appFabricServer = injector.getInstance(AppFabricServer.class);
                                                     metricsCollectionService = injector.getInstance(
                                                                                       MetricsCollectionService.class);
                                                     Futures.getUnchecked(Services.chainStart(zkClientService,
                                                                                              kafkaClientService,
                                                                                              metricsCollectionService,
                                                                                              appFabricServer));
                                                   }

                                                   @Override
                                                   public void follower() {
                                                     LOG.info("Becoming follower.");
                                                     Futures.getUnchecked(Services.chainStop(appFabricServer,
                                                                                             zkClientService));
                                                   }
                                                 });
    electionCancel.add(election);

  }

  /**
   * Invoked by jsvc to stop the program.
   */
  @Override
  public void stop() {
    LOG.info("Stopping App Fabric ...");
    for (Cancellable cancel : electionCancel) {
      cancel.cancel();
    }
  }

  /**
   * Invoked by jsvc for resource cleanup.
   */
  @Override
  public void destroy() {
  }
}

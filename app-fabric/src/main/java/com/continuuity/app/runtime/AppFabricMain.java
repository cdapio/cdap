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
import com.continuuity.common.service.CommandPortService;
import com.continuuity.common.service.RUOKHandler;
import com.continuuity.common.zookeeper.election.ElectionHandler;
import com.continuuity.common.zookeeper.election.LeaderElection;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.internal.app.services.AppFabricServer;
import com.continuuity.internal.kafka.client.ZKKafkaClientService;
import com.continuuity.kafka.client.KafkaClientService;
import com.continuuity.metrics.guice.MetricsClientRuntimeModule;
import com.continuuity.weave.api.WeaveRunnerService;
import com.continuuity.weave.common.Services;
import com.continuuity.weave.zookeeper.RetryStrategies;
import com.continuuity.weave.zookeeper.ZKClientService;
import com.continuuity.weave.zookeeper.ZKClientServices;
import com.continuuity.weave.zookeeper.ZKClients;
import com.google.common.util.concurrent.Futures;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * AppFabric server main. It can be started as regular Java Application
 * or started using apache commons daemon (jsvc)
 */
public final class AppFabricMain extends DaemonMain {
  private static final Logger LOG = LoggerFactory.getLogger(AppFabricMain.class);

  private ZKClientService zkClientService;
  private CommandPortService cmdService;
  private AppFabricServer appFabricServer;
  private MetricsCollectionService metricsCollectionService;
  private KafkaClientService kafkaClientService;
  private Injector injector;
  private LeaderElection leaderElection;
  private String kafkaZKNamespace;

  public static void main(final String[] args) throws Exception {
    new AppFabricMain().doMain(args);
  }

  @Override
  public void init(String[] args) {
    CConfiguration cConf = CConfiguration.create();
    Configuration hConf = HBaseConfiguration.create(new HdfsConfiguration());

    zkClientService =
      ZKClientServices.delegate(
        ZKClients.reWatchOnExpire(
          ZKClients.retryOnFailure(
            ZKClientService.Builder.of(cConf.get(Constants.Zookeeper.QUORUM))
                                  .setSessionTimeout(cConf.getInt(Constants.Zookeeper.CFG_SESSION_TIMEOUT_MILLIS,
                                                                  Constants.Zookeeper.DEFAULT_SESSION_TIMEOUT_MILLIS))
                                  .build(),
            RetryStrategies.fixDelay(2, TimeUnit.SECONDS)
          )
        )
      );
    kafkaZKNamespace = cConf.get(KafkaConstants.ConfigKeys.ZOOKEEPER_NAMESPACE_CONFIG);
    kafkaClientService = new ZKKafkaClientService(kafkaZKNamespace == null
                               ? zkClientService
                               : ZKClients.namespace(zkClientService, "/" + kafkaZKNamespace));
    injector = Guice.createInjector(
      new MetricsClientRuntimeModule(kafkaClientService).getDistributedModules(),
      new ConfigModule(cConf, hConf),
      new IOModule(),
      new LocationRuntimeModule().getDistributedModules(),
      new DiscoveryRuntimeModule(zkClientService).getDistributedModules(),
      new AppFabricServiceRuntimeModule().getDistributedModules(),
      new ProgramRunnerRuntimeModule().getDistributedModules(),
      new DataFabricModules(cConf, hConf).getDistributedModules()
    );

  }

  @Override
  public void start() {
    metricsCollectionService = injector.getInstance(MetricsCollectionService.class);

    startHealthCheckService();

    Futures.getUnchecked(Services.chainStart(zkClientService,
                                             kafkaClientService,
                                             metricsCollectionService));

    injector.getInstance(WeaveRunnerService.class).startAndWait();

    leaderElection = new LeaderElection(zkClientService,
                                        Constants.Service.APP_FABRIC_LEADER_ELECTION_PREFIX, new ElectionHandler() {
      @Override
      public void leader() {
        appFabricServer = injector.getInstance(AppFabricServer.class);
        LOG.info("Leader: Starting app fabric server.");
        Futures.getUnchecked(Services.chainStart(appFabricServer));
      }

      @Override
      public void follower() {
        if (appFabricServer != null) {
          LOG.info("Follower: Stopping app fabric server.");
          Futures.getUnchecked(Services.chainStop(appFabricServer));
        }
      }
    });
  }

  /**
   * Invoked by jsvc to stop the program.
   */
  @Override
  public void stop() {
    LOG.info("Stopping App Fabric ...");
    cmdService.stop();

    if (leaderElection != null){
      leaderElection.cancel();
    }

    if (appFabricServer != null) {
      Futures.getUnchecked(Services.chainStop(appFabricServer));
    }

    Futures.getUnchecked(Services.chainStop(metricsCollectionService,
                                            kafkaClientService, zkClientService));
  }

  /**
   * Invoked by jsvc for resource cleanup.
   */
  @Override
  public void destroy() {
  }

  private void startHealthCheckService() {
    CConfiguration conf = injector.getInstance(CConfiguration.class);
    int port = conf.getInt(Constants.AppFabric.SERVER_COMMAND_PORT, 0);
    cmdService = CommandPortService.builder("app-fabric-status")
      .setPort(port)
      .addCommandHandler(RUOKHandler.COMMAND, RUOKHandler.DESCRIPTION, new RUOKHandler())
      .build();
    cmdService.startAndWait();
  }
}

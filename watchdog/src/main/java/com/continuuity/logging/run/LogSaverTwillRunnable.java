/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.run;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.common.guice.KafkaClientModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.common.guice.ZKClientModule;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data.runtime.DataSetsModules;
import com.continuuity.gateway.auth.AuthModule;
import com.continuuity.logging.LoggingConfiguration;
import com.continuuity.logging.guice.LogSaverStatusServiceModule;
import com.continuuity.logging.save.LogSaver;
import com.continuuity.logging.service.LogSaverStatusService;
import com.continuuity.metrics.guice.MetricsClientRuntimeModule;
import com.continuuity.watchdog.election.MultiLeaderElection;
import com.continuuity.watchdog.election.PartitionChangeHandler;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillRunnableSpecification;
import org.apache.twill.common.Services;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.zookeeper.ZKClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Twill wrapper for running LogSaver through Twill.
 */
public final class LogSaverTwillRunnable extends AbstractTwillRunnable {
  private static final Logger LOG = LoggerFactory.getLogger(LogSaverTwillRunnable.class);

  private LogSaver logSaver;
  private SettableFuture<?> completion;

  private String name;
  private String hConfName;
  private String cConfName;
  private ZKClientService zkClientService;
  private KafkaClientService kafkaClientService;
  private MultiLeaderElection multiElection;
  private LogSaverStatusService logSaverStatusService;

  public LogSaverTwillRunnable(String name, String hConfName, String cConfName) {
    this.name = name;
    this.hConfName = hConfName;
    this.cConfName = cConfName;
  }

  @Override
  public TwillRunnableSpecification configure() {
    return TwillRunnableSpecification.Builder.with()
      .setName(name)
      .withConfigs(ImmutableMap.of(
        "hConf", hConfName,
        "cConf", cConfName
      ))
      .build();
  }

  @Override
  public void initialize(TwillContext context) {
    super.initialize(context);

    completion = SettableFuture.create();
    name = context.getSpecification().getName();
    Map<String, String> configs = context.getSpecification().getConfigs();

    LOG.info("Initialize runnable: " + name);
    try {
      // Load configuration
      Configuration hConf = new Configuration();
      hConf.clear();
      hConf.addResource(new File(configs.get("hConf")).toURI().toURL());

      UserGroupInformation.setConfiguration(hConf);

      CConfiguration cConf = CConfiguration.create();
      cConf.clear();
      cConf.addResource(new File(configs.get("cConf")).toURI().toURL());
      cConf.set(Constants.LogSaver.ADDRESS, context.getHost().getCanonicalHostName());

      // Initialize ZK client
      String zookeeper = cConf.get(Constants.Zookeeper.QUORUM);
      if (zookeeper == null) {
        LOG.error("No zookeeper quorum provided.");
        throw new IllegalStateException("No zookeeper quorum provided.");
      }

      Injector injector = createGuiceInjector(cConf, hConf);
      zkClientService = injector.getInstance(ZKClientService.class);
      kafkaClientService = injector.getInstance(KafkaClientService.class);
      logSaver = injector.getInstance(LogSaver.class);

      int numPartitions = Integer.parseInt(cConf.get(LoggingConfiguration.NUM_PARTITIONS,
                                                     LoggingConfiguration.DEFAULT_NUM_PARTITIONS));
      LOG.info("Num partitions = {}", numPartitions);
      multiElection = new MultiLeaderElection(zkClientService, "log-saver-partitions", numPartitions,
                                              createPartitionChangeHandler(logSaver));

      logSaverStatusService = injector.getInstance(LogSaverStatusService.class);
      LOG.info("Runnable initialized: " + name);
    } catch (Throwable t) {
      LOG.error(t.getMessage(), t);
      throw Throwables.propagate(t);
    }
  }

  @Override
  public void run() {
    LOG.info("Starting runnable " + name);

    Futures.getUnchecked(Services.chainStart(zkClientService,
                                             kafkaClientService, logSaver, multiElection, logSaverStatusService));

    LOG.info("Runnable started " + name);

    try {
      completion.get();

      LOG.info("Runnable stopped " + name);
    } catch (InterruptedException e) {
      LOG.error("Waiting on completion interrupted", e);
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) {
      // Propagate the execution exception will causes TwillRunnable terminate with error,
      // and AM would detect and restarts it.
      LOG.error("Completed with exception. Exception get propagated", e);
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void stop() {
    LOG.info("Stopping runnable " + name);

    Futures.getUnchecked(Services.chainStop(logSaverStatusService,
                                            multiElection, logSaver, kafkaClientService, zkClientService));
    completion.set(null);
  }

  private PartitionChangeHandler createPartitionChangeHandler(final PartitionChangeHandler delegate) {
    return new PartitionChangeHandler() {
      @Override
      public void partitionsChanged(Set<Integer> partitions) {
        try {
          delegate.partitionsChanged(partitions);
        } catch (Throwable t) {
          LOG.error("Exception while changing partition. Terminating.", t);
          completion.setException(t);
        }
      }
    };
  }

  private static Injector createGuiceInjector(CConfiguration cConf, Configuration hConf) {
    return Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new IOModule(),
      new ZKClientModule(),
      new KafkaClientModule(),
      new AuthModule(),
      new MetricsClientRuntimeModule().getDistributedModules(),
      new DiscoveryRuntimeModule().getDistributedModules(),
      new LocationRuntimeModule().getDistributedModules(),
      new DataFabricModules().getDistributedModules(),
      new DataSetsModules().getDistributedModule(),
      new LogSaverStatusServiceModule()
    );
  }
}

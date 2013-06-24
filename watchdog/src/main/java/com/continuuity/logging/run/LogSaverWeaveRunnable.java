/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.run;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.engine.memory.MemoryOVCTableHandle;
import com.continuuity.data.engine.memory.oracle.MemoryStrictlyMonotonicTimeOracle;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.omid.OmidTransactionalOperationExecutor;
import com.continuuity.data.operation.executor.omid.TimestampOracle;
import com.continuuity.data.operation.executor.omid.TransactionOracle;
import com.continuuity.data.operation.executor.omid.memory.MemoryOracle;
import com.continuuity.data.operation.executor.remote.RemoteOperationExecutor;
import com.continuuity.data.table.OVCTableHandle;
import com.continuuity.logging.save.LogSaver;
import com.continuuity.weave.api.AbstractWeaveRunnable;
import com.continuuity.weave.api.WeaveContext;
import com.continuuity.weave.api.WeaveRunnableSpecification;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Singleton;
import com.google.inject.name.Names;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * Weave wrapper for running LogSaver through Weave.
 */
public final class LogSaverWeaveRunnable extends AbstractWeaveRunnable {
  private static final Logger LOG = LoggerFactory.getLogger(LogSaverWeaveRunnable.class);

  private LogSaver logSaver;
  private CountDownLatch runLatch;

  private String name;
  private String hConfName;
  private String cConfName;

  private CConfiguration cConf;

  public LogSaverWeaveRunnable(String name, String hConfName, String cConfName) {
    this.name = name;
    this.hConfName = hConfName;
    this.cConfName = cConfName;
  }

  @Override
  public WeaveRunnableSpecification configure() {
    return WeaveRunnableSpecification.Builder.with()
      .setName(name)
      .withConfigs(ImmutableMap.of(
        "hConf", hConfName,
        "cConf", cConfName
      ))
      .build();
  }

  @Override
  public void initialize(WeaveContext context) {
    super.initialize(context);

    runLatch = new CountDownLatch(1);
    name = context.getSpecification().getName();
    Map<String, String> configs = context.getSpecification().getConfigs();

    LOG.info("Initialize runnable: " + name);
    try {
      // Load configuration
      Configuration hConf = new Configuration();
      hConf.clear();
      hConf.addResource(new File(configs.get("hConf")).toURI().toURL());

      cConf = CConfiguration.create();
      cConf.clear();
      cConf.addResource(new File(configs.get("cConf")).toURI().toURL());

      int instanceId = context.getInstanceId();

      Injector injector = Guice.createInjector(createModule());

      logSaver = new LogSaver(injector.getInstance(OperationExecutor.class), instanceId, hConf, cConf);

      LOG.info("Runnable initialized: " + name);
    } catch (Throwable t) {
      LOG.error(t.getMessage(), t);
      throw Throwables.propagate(t);
    }
  }

  @Override
  public void run() {
    logSaver.startAndWait();
    try {
      runLatch.await();
    } catch (InterruptedException e) {
      LOG.error("Waiting on latch interrupted");
    }
  }

  @Override
  public void stop() {
    logSaver.stopAndWait();
    runLatch.countDown();
  }

  private Module createModule() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        if (cConf.getBoolean("logging.app.run.opex.remote", true)) {
          // Bind remote operation executor
          bind(OperationExecutor.class).to(RemoteOperationExecutor.class).in(Singleton.class);
          bind(CConfiguration.class).annotatedWith(Names.named("RemoteOperationExecutorConfig")).toInstance(cConf);
        } else {
          // Bind local opex for testing purpose.
          bind(OperationExecutor.class).to(OmidTransactionalOperationExecutor.class).in(Singleton.class);
          bind(TransactionOracle.class).to(MemoryOracle.class);
          bind(TimestampOracle.class).to(MemoryStrictlyMonotonicTimeOracle.class);
          bind(OVCTableHandle.class).toInstance(MemoryOVCTableHandle.getInstance());
          bind(CConfiguration.class).annotatedWith(Names.named("DataFabricOperationExecutorConfig")).toInstance(cConf);
        }
      }
    };
  }
}

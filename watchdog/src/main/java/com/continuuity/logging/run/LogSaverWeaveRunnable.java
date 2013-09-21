/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.run;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data.DistributedDataSetAccessor;
import com.continuuity.data2.transaction.distributed.TransactionServiceClient;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.logging.LoggingConfiguration;
import com.continuuity.logging.save.LogSaver;
import com.continuuity.weave.api.AbstractWeaveRunnable;
import com.continuuity.weave.api.WeaveContext;
import com.continuuity.weave.api.WeaveRunnableSpecification;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
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

      CConfiguration cConf = CConfiguration.create();
      cConf.clear();
      cConf.addResource(new File(configs.get("cConf")).toURI().toURL());
      String baseDir = cConf.get(LoggingConfiguration.LOG_BASE_DIR);
      if (baseDir != null) {
        if (baseDir.startsWith("/")) {
          baseDir = baseDir.substring(1);
        }
        cConf.set(LoggingConfiguration.LOG_BASE_DIR, cConf.get(Constants.CFG_HDFS_NAMESPACE) + "/" + baseDir);
      }

      int instanceId = context.getInstanceId();

      DataSetAccessor dataSetAccessor = new DistributedDataSetAccessor(cConf, hConf);
      TransactionSystemClient txClient = new TransactionServiceClient(cConf);

      logSaver = new LogSaver(dataSetAccessor, txClient, instanceId, hConf, cConf);

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
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void stop() {
    logSaver.stopAndWait();
    runLatch.countDown();
  }
}

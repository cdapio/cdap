/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.run;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.logging.LoggingConfiguration;
import com.continuuity.common.logging.logback.serialize.LogSchema;
import com.continuuity.common.runtime.DaemonMain;
import com.continuuity.weave.api.ResourceSpecification;
import com.continuuity.weave.api.WeaveController;
import com.continuuity.weave.api.WeavePreparer;
import com.continuuity.weave.api.WeaveRunnerService;
import com.continuuity.weave.api.logging.PrinterLogHandler;
import com.continuuity.weave.yarn.YarnWeaveRunnerService;
import com.google.common.base.Throwables;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.PrintWriter;
import java.net.URISyntaxException;

/**
 * Wrapper class to run LogSaver as a process.
 */
public final class LogSaverMain extends DaemonMain {
  private WeaveRunnerService weaveRunnerService;
  private WeavePreparer weavePreparer;
  private WeaveController weaveController;

  public static void main(String [] args) throws Exception {
    new LogSaverMain().doMain(args);
  }

  @Override
  public void init(String[] args) {
    CConfiguration cConf = CConfiguration.create();
    weaveRunnerService = new YarnWeaveRunnerService(new YarnConfiguration(),
                                                    cConf.get(Constants.CFG_ZOOKEEPER_ENSEMBLE));

    int partitions = cConf.getInt(LoggingConfiguration.NUM_PARTITIONS,  -1);
    if (partitions < 1) {
      throw new IllegalArgumentException("log.publish.partitions should be at least 1");
    }

    ResourceSpecification spec = ResourceSpecification.Builder
      .with()
      .setCores(2)
      .setMemory(1, ResourceSpecification.SizeUnit.GIGA)
      .setInstances(partitions)
      .build();

    try {
      weavePreparer = weaveRunnerService.prepare(new LogSaverRunner(), spec)
        // TODO: write logs to file
        .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out)))
        .withResources(LogSchema.getSchemaURL().toURI());
    } catch (URISyntaxException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void start() {
    weaveController = weavePreparer.start();
  }

  @Override
  public void stop() {
    weaveController.stopAndWait();
  }

  @Override
  public void destroy() {
    weaveRunnerService.stopAndWait();
  }
}

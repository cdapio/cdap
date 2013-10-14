/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.run;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.logging.LoggingConfiguration;
import com.continuuity.weave.api.ResourceSpecification;
import com.continuuity.weave.api.WeaveApplication;
import com.continuuity.weave.api.WeaveSpecification;
import com.google.common.base.Preconditions;

import java.io.File;

/**
 * WeaveApplication wrapper to run LogSaver.
 */
public class LogSaverWeaveApplication implements WeaveApplication {
  private static final String NAME = "reactor.log.saver";
  private final CConfiguration cConf;
  private final File hConfig;
  private final File cConfig;

  public LogSaverWeaveApplication(CConfiguration cConf, File hConfig, File cConfig) {
    this.cConf = cConf;
    this.hConfig = hConfig;
    this.cConfig = cConfig;
  }

  public static String getName() {
    return NAME;
  }

  @Override
  public WeaveSpecification configure() {
    int numInstances = cConf.getInt(LoggingConfiguration.LOG_SAVER_NUM_INSTANCES,
                                    LoggingConfiguration.DEFAULT_LOG_SAVER_NUM_INSTANCES);
    Preconditions.checkArgument(numInstances > 0, "log saver num instances should be at least 1, got %s",
                                numInstances);

    int memory = cConf.getInt(LoggingConfiguration.LOG_SAVER_RUN_MEMORY_MB, 1024);
    Preconditions.checkArgument(memory > 0, "Got invalid memory value for log saver %s", memory);

    WeaveSpecification.Builder.MoreRunnable moreRunnable = WeaveSpecification.Builder.with()
      .setName(NAME)
      .withRunnable();

    ResourceSpecification spec = ResourceSpecification.Builder
      .with()
      .setVirtualCores(2)
      .setMemory(memory, ResourceSpecification.SizeUnit.MEGA)
      .setInstances(numInstances)
      .build();

    WeaveSpecification.Builder.RunnableSetter runnableSetter =
      moreRunnable.add(new LogSaverWeaveRunnable("saver", "hConf.xml", "cConf.xml"), spec)
        .withLocalFiles()
        .add("hConf.xml", hConfig.toURI())
        .add("cConf.xml", cConfig.toURI())
        .apply();

    return runnableSetter.anyOrder().build();
  }
}

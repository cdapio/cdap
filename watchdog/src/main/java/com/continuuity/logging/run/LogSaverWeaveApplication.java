/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.run;

import com.continuuity.weave.api.ResourceSpecification;
import com.continuuity.weave.api.WeaveApplication;
import com.continuuity.weave.api.WeaveSpecification;

import java.io.File;

/**
 * WeaveApplication wrapper to run LogSaver.
 */
public class LogSaverWeaveApplication implements WeaveApplication {
  private final int partitions;
  private final File hConfig;
  private final File cConfig;

  public LogSaverWeaveApplication(int partitions, File hConfig, File cConfig) {
    this.partitions = partitions;
    this.hConfig = hConfig;
    this.cConfig = cConfig;
  }

  @Override
  public WeaveSpecification configure() {
    WeaveSpecification.Builder.MoreRunnable moreRunnable = WeaveSpecification.Builder.with()
      .setName("LogSaverWeaveApplication")
      .withRunnable();

    ResourceSpecification spec = ResourceSpecification.Builder
      .with()
      .setCores(2)
      .setMemory(1, ResourceSpecification.SizeUnit.GIGA)
      .setInstances(partitions)
      .build();

    WeaveSpecification.Builder.RunnableSetter runnableSetter =
      moreRunnable.add(new LogSaverWeaveRunnable("LogSaverWeaveRunnable", "hConf.xml", "cConf.xml"), spec)
        .withLocalFiles()
        .add("hConf.xml", hConfig.toURI())
        .add("cConf.xml", cConfig.toURI())
        .apply();

    return runnableSetter.anyOrder().build();
  }
}

/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.run;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.twill.AbortOnTimeoutEventHandler;
import com.continuuity.logging.LoggingConfiguration;
import com.google.common.base.Preconditions;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillSpecification;

import java.io.File;

/**
 * TwillApplication wrapper to run LogSaver.
 */
public class LogSaverTwillApplication implements TwillApplication {
  private static final String NAME = "reactor.log.saver";
  private final CConfiguration cConf;
  private final File hConfig;
  private final File cConfig;

  public LogSaverTwillApplication(CConfiguration cConf, File hConfig, File cConfig) {
    this.cConf = cConf;
    this.hConfig = hConfig;
    this.cConfig = cConfig;
  }

  public static String getName() {
    return NAME;
  }

  @Override
  public TwillSpecification configure() {
    int numInstances = cConf.getInt(LoggingConfiguration.LOG_SAVER_NUM_INSTANCES,
                                    LoggingConfiguration.DEFAULT_LOG_SAVER_NUM_INSTANCES);
    Preconditions.checkArgument(numInstances > 0, "log saver num instances should be at least 1, got %s",
                                numInstances);

    int memory = cConf.getInt(LoggingConfiguration.LOG_SAVER_RUN_MEMORY_MB, 1024);
    Preconditions.checkArgument(memory > 0, "Got invalid memory value for log saver %s", memory);

    // It is always present in continuuity-default.xml
    long noContainerTimeout = cConf.getLong(Constants.CFG_TWILL_NO_CONTAINER_TIMEOUT, Long.MAX_VALUE);

    TwillSpecification.Builder.MoreRunnable moreRunnable = TwillSpecification.Builder.with()
      .setName(NAME)
      .withRunnable();

    ResourceSpecification spec = ResourceSpecification.Builder
      .with()
      .setVirtualCores(2)
      .setMemory(memory, ResourceSpecification.SizeUnit.MEGA)
      .setInstances(numInstances)
      .build();

    TwillSpecification.Builder.RunnableSetter runnableSetter =
      moreRunnable.add(new LogSaverTwillRunnable("saver", "hConf.xml", "cConf.xml"), spec)
        .withLocalFiles()
        .add("hConf.xml", hConfig.toURI())
        .add("cConf.xml", cConfig.toURI())
        .apply();

    return runnableSetter.anyOrder().withEventHandler(new AbortOnTimeoutEventHandler(noContainerTimeout)).build();
  }
}

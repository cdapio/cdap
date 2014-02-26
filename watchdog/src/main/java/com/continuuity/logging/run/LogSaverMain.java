/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.run;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.twill.TwillRunnerMain;
import com.continuuity.data.security.HBaseSecureStoreUpdater;
import com.continuuity.data.security.HBaseTokenUtils;
import com.continuuity.data2.util.hbase.HBaseTableUtilFactory;
import com.continuuity.logging.serialize.LogSchema;
import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.security.Credentials;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.yarn.YarnSecureStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;

/**
 * Wrapper class to run LogSaver as a process.
 */
public final class LogSaverMain extends TwillRunnerMain {
  private static final Logger LOG = LoggerFactory.getLogger(LogSaverMain.class);

  public LogSaverMain(CConfiguration cConf, Configuration hConf) {
    super(cConf, hConf);
  }

  public static void main(String[] args) throws Exception {
    new LogSaverMain(CConfiguration.create(), HBaseConfiguration.create()).doMain(args);
  }

  @Override
  protected TwillApplication createTwillApplication() {
    try {
      return new LogSaverTwillApplication(cConf, getSavedHConf(), getSavedCConf());
    } catch (IOException e) {
      LOG.error("Got exception when creating LogSaverTwillApplication", e);
      throw Throwables.propagate(e);
    }
  }

  @Override
  protected void scheduleSecureStoreUpdate(TwillRunner twillRunner) {
    if (User.isHBaseSecurityEnabled(hConf)) {
      HBaseSecureStoreUpdater updater = new HBaseSecureStoreUpdater(hConf);
      twillRunner.scheduleSecureStoreUpdate(updater, 30000L, updater.getUpdateInterval(), TimeUnit.MILLISECONDS);
    }
  }

  @Override
  protected TwillPreparer prepare(TwillPreparer preparer) {
    try {
      return preparer.withResources(LogSchema.getSchemaURL().toURI())
                     .withDependencies(new HBaseTableUtilFactory().get().getClass())
                     .addSecureStore(YarnSecureStore.create(HBaseTokenUtils.obtainToken(hConf, new Credentials())));
    } catch (URISyntaxException e) {
      LOG.error("Got exception while preparing", e);
      throw Throwables.propagate(e);
    }
  }
}

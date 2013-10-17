/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.run;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.weave.WeaveRunnerMain;
import com.continuuity.data.security.HBaseTokenUtils;
import com.continuuity.logging.serialize.LogSchema;
import com.continuuity.weave.api.WeaveApplication;
import com.continuuity.weave.api.WeavePreparer;
import com.continuuity.weave.yarn.YarnSecureStore;
import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.security.Credentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Wrapper class to run LogSaver as a process.
 */
public final class LogSaverMain extends WeaveRunnerMain {
  private static final Logger LOG = LoggerFactory.getLogger(LogSaverMain.class);

  public LogSaverMain(CConfiguration cConf, Configuration hConf) {
    super(cConf, hConf);
  }

  public static void main(String[] args) throws Exception {
    new LogSaverMain(CConfiguration.create(), HBaseConfiguration.create()).doMain(args);
  }

  @Override
  protected WeaveApplication createWeaveApplication() {
    try {
      return new LogSaverWeaveApplication(cConf, getSavedHConf(), getSavedCConf());
    } catch (IOException e) {
      LOG.error("Got exception when creating LogSaverWeaveApplication", e);
      throw Throwables.propagate(e);
    }
  }

  @Override
  protected WeavePreparer prepare(WeavePreparer preparer) {
    try {
      return preparer.withResources(LogSchema.getSchemaURL().toURI())
                     .addSecureStore(YarnSecureStore.create(HBaseTokenUtils.obtainToken(hConf, new Credentials())));
    } catch (URISyntaxException e) {
      LOG.error("Got exception while preparing", e);
      throw Throwables.propagate(e);
    }
  }
}

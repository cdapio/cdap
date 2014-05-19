/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.security;

import com.continuuity.common.conf.Constants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.twill.api.RunId;
import org.apache.twill.api.SecureStore;
import org.apache.twill.api.SecureStoreUpdater;
import org.apache.twill.yarn.YarnSecureStore;

import java.util.concurrent.TimeUnit;

/**
 * A {@link SecureStoreUpdater} that provides update to HBase secure token.
 */
public final class HBaseSecureStoreUpdater implements SecureStoreUpdater {

  private final Configuration hConf;
  private long nextUpdateTime = -1;
  private Credentials credentials;

  public HBaseSecureStoreUpdater(Configuration hConf) {
    this.hConf = hConf;
    this.credentials = HBaseTokenUtils.obtainToken(hConf, new Credentials());
  }

  /**
   * Returns the update interval for the HBase delegation token.
   * @return The update interval in milliseconds.
   */
  public long getUpdateInterval() {
    // The value contains in hbase-default.xml, so it should always there. If it is really missing, default it to 1 day.
    return hConf.getLong(Constants.HBase.AUTH_KEY_UPDATE_INTERVAL, TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS));
  }

  @Override
  public SecureStore update(String application, RunId runId) {
    long now = System.currentTimeMillis();
    if (now >= nextUpdateTime) {
      nextUpdateTime = now + getUpdateInterval();

      HBaseTokenUtils.obtainToken(hConf, credentials);
    }
    return YarnSecureStore.create(credentials);
  }
}

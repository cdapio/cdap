/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.security;

import com.continuuity.common.conf.Constants;
import com.continuuity.weave.api.RunId;
import com.continuuity.weave.api.SecureStore;
import com.continuuity.weave.api.SecureStoreUpdater;
import com.continuuity.weave.yarn.YarnSecureStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;

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

  @Override
  public SecureStore update(String application, RunId runId) {
    long now = System.currentTimeMillis();
    if (now >= nextUpdateTime) {
      // The value contains in hbase-default.xml, so it should always there.
      long renewInterval = hConf.getLong(Constants.HBase.AUTH_KEY_UPDATE_INTERVAL, 0L);
      nextUpdateTime = now + renewInterval;

      HBaseTokenUtils.obtainToken(hConf, credentials);
    }
    return YarnSecureStore.create(credentials);
  }
}

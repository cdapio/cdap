package com.continuuity.hive.client.guice;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.hive.client.DisabledHiveClient;
import com.continuuity.hive.client.HiveClient;
import com.continuuity.hive.client.HiveCommandExecutor;
import com.continuuity.hive.client.NoOpHiveClient;

import com.google.inject.AbstractModule;

/**
 * Guice module for hive client.
 */
public class HiveClientModule extends AbstractModule {

  private final boolean exploreEnabled;

  public HiveClientModule(CConfiguration conf) {
    this.exploreEnabled = conf.getBoolean(Constants.Hive.EXPLORE_ENABLED,
                                          Constants.Hive.DEFAULT_EXPLORE_ENABLED);
  }

  @Override
  protected void configure() {
    if (!exploreEnabled) {
      bind(HiveClient.class).to(DisabledHiveClient.class);
    } else {
      bind(HiveClient.class).to(HiveCommandExecutor.class);
    }
  }
}

package com.continuuity.hive.client.guice;

import com.continuuity.hive.client.HiveClient;
import com.continuuity.hive.client.HiveCommandExecutor;

import com.google.inject.AbstractModule;

/**
 * Guice module for hive client.
 */
public class HiveClientModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(HiveClient.class).to(HiveCommandExecutor.class);
  }
}

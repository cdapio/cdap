package com.continuuity.hive.guice;

import com.continuuity.hive.HiveCommandExecutor;
import com.continuuity.hive.HiveServer;
import com.continuuity.hive.distributed.DistributedHiveServer;
import com.google.inject.AbstractModule;
import com.google.inject.Scopes;

/**
 *
 */
public class DistributedHiveModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(HiveServer.class).to(DistributedHiveServer.class).in(Scopes.SINGLETON);
    bind(HiveCommandExecutor.class);
  }
}

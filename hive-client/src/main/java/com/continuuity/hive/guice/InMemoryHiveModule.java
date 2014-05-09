package com.continuuity.hive.guice;

import com.continuuity.hive.HiveCommandExecutor;
import com.continuuity.hive.HiveServer;
import com.continuuity.hive.inmemory.LocalHiveServer;
import com.google.inject.AbstractModule;
import com.google.inject.Scopes;

/**
 *
 */
public class InMemoryHiveModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(HiveServer.class).to(LocalHiveServer.class).in(Scopes.SINGLETON);
    bind(HiveCommandExecutor.class);
  }
}

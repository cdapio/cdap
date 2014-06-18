package com.continuuity.hive.client.guice;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.hive.client.HiveClient;
import com.continuuity.hive.client.HiveCommandExecutor;
import com.continuuity.hive.client.NoOpHiveClient;

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.name.Names;

/**
 * Guice module for hive client.
 */
public class HiveClientModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(HiveClient.class).annotatedWith(Names.named("runtime-client")).to(HiveCommandExecutor.class);
    bind(HiveClient.class).toProvider(HiveClientProvider.class);
  }

  private static final class HiveClientProvider implements Provider<HiveClient> {

    private final CConfiguration configuration;
    private final Injector injector;

    @Inject
    private HiveClientProvider(CConfiguration configuration, Injector injector) {
      this.configuration = configuration;
      this.injector = injector;
    }

    @Override
    public HiveClient get() {
      boolean exploreEnabled = configuration.getBoolean(Constants.Explore.CFG_EXPLORE_ENABLED);
      if (!exploreEnabled) {
        return injector.getInstance(NoOpHiveClient.class);
      } else {
        return injector.getInstance(Key.get(HiveClient.class, Names.named("runtime-client")));
      }
    }
  }
}

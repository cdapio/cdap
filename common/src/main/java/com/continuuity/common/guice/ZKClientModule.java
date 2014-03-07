/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.guice;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.google.common.base.Preconditions;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.twill.zookeeper.RetryStrategies;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.twill.zookeeper.ZKClientServices;
import org.apache.twill.zookeeper.ZKClients;

import java.util.concurrent.TimeUnit;

/**
 * Guice module for binding {@link ZKClient} and {@link ZKClientService}. Requires {@link ConfigModule}
 * bindings.
 */
public class ZKClientModule extends AbstractModule {

  @Override
  protected void configure() {
    /**
     * ZKClientService is provided by the provider method
     * {@link #provideZKClientService(com.continuuity.common.conf.CConfiguration)}.
     */
    bind(ZKClient.class).to(ZKClientService.class);
  }

  @Provides
  @Singleton
  private ZKClientService provideZKClientService(CConfiguration cConf) {
    String zookeeper = cConf.get(Constants.Zookeeper.QUORUM);
    Preconditions.checkNotNull(zookeeper, "Missing Zookeeper configuration '%s'", Constants.Zookeeper.QUORUM);

    return ZKClientServices.delegate(
      ZKClients.reWatchOnExpire(
        ZKClients.retryOnFailure(
          ZKClientService.Builder.of(cConf.get(Constants.Zookeeper.QUORUM))
            .setSessionTimeout(cConf.getInt(Constants.Zookeeper.CFG_SESSION_TIMEOUT_MILLIS,
                                            Constants.Zookeeper.DEFAULT_SESSION_TIMEOUT_MILLIS))
            .build(),
          RetryStrategies.exponentialDelay(500, 2000, TimeUnit.MILLISECONDS)
        )
      )
    );
  }
}

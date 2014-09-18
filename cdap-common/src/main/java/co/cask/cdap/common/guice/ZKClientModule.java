/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package co.cask.cdap.common.guice;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
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
     * {@link #provideZKClientService(co.cask.cdap.common.conf.CConfiguration)}.
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

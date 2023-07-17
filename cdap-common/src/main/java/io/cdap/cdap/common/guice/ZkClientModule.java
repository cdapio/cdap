/*
 * Copyright © 2014-2023 Cask Data, Inc.
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

package io.cdap.cdap.common.guice;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import java.util.concurrent.TimeUnit;
import org.apache.twill.zookeeper.RetryStrategies;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.twill.zookeeper.ZKClientServices;
import org.apache.twill.zookeeper.ZKClients;

/**
 * Guice module for binding {@link ZKClient} and {@link ZKClientService}. Requires {@link
 * ConfigModule} bindings.
 */
public class ZkClientModule extends AbstractModule {

  @Override
  protected void configure() {
    /*
     * ZKClientService is provided by the provider method
     * {@link #provideZKClientService(io.cdap.cdap.common.conf.CConfiguration)}.
     */
    bind(ZKClient.class).to(ZKClientService.class);
  }

  @Provides
  @Singleton
  private ZKClientService provideZkClientService(CConfiguration cConf) {
    return ZKClientServices.delegate(
        ZKClients.reWatchOnExpire(
            ZKClients.retryOnFailure(
                ZKClientService.Builder.of(Constants.Zookeeper.getZkQuorum(cConf))
                    .setSessionTimeout(cConf.getInt(Constants.Zookeeper.CFG_SESSION_TIMEOUT_MILLIS,
                        Constants.Zookeeper.DEFAULT_SESSION_TIMEOUT_MILLIS))
                    .build(),
                RetryStrategies.exponentialDelay(500, 2000, TimeUnit.MILLISECONDS)
            )
        )
    );
  }
}

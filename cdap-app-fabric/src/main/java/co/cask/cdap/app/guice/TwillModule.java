/*
 * Copyright © 2014-2016 Cask Data, Inc.
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
package co.cask.cdap.app.guice;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.security.Impersonator;
import com.google.inject.Inject;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.Configs;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.filesystem.LocationFactories;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.yarn.YarnTwillRunnerService;

/**
 * Guice module for providing bindings for Twill. This module requires accessible bindings to
 * {@link CConfiguration}, {@link YarnConfiguration} and {@link LocationFactory}.
 */
public class TwillModule extends PrivateModule {

  @Override
  protected void configure() {
    bind(TwillRunnerService.class).toProvider(TwillRunnerServiceProvider.class).in(Scopes.SINGLETON);
    bind(TwillRunner.class).to(TwillRunnerService.class);

    expose(TwillRunnerService.class);
    expose(TwillRunner.class);
  }

  /**
   * Provider for {@link TwillRunnerService}.
   */
  private static final class TwillRunnerServiceProvider implements Provider<TwillRunnerService> {

    private final CConfiguration cConf;
    private final YarnConfiguration yarnConf;
    private final LocationFactory locationFactory;
    private final Impersonator impersonator;

    @Inject
    TwillRunnerServiceProvider(CConfiguration cConf, YarnConfiguration yarnConf,
                               LocationFactory locationFactory, Impersonator impersonator) {
      this.cConf = cConf;
      this.yarnConf = yarnConf;
      this.locationFactory = locationFactory;
      this.impersonator = impersonator;
    }

    @Override
    public TwillRunnerService get() {
      String zkConnectStr = cConf.get(Constants.Zookeeper.QUORUM) + cConf.get(Constants.CFG_TWILL_ZK_NAMESPACE);

      // Copy the yarn config and setup twill configs
      YarnConfiguration yarnConfig = new YarnConfiguration(yarnConf);
      // Always disable the location delegation update from twill, as we always do it from CDAP side
      yarnConfig.setBoolean(Configs.Keys.SECURE_STORE_UPDATE_LOCATION_ENABLED, false);

      YarnTwillRunnerService runner = new YarnTwillRunnerService(yarnConfig,
                                                                 zkConnectStr,
                                                                 LocationFactories.namespace(locationFactory, "twill"));

      // Set JVM options based on configuration
      String jvmOpts = cConf.get(Constants.AppFabric.PROGRAM_JVM_OPTS);
      runner.setJVMOptions(jvmOpts);

      return new ImpersonatedTwillRunnerService(runner, impersonator);
    }
  }
}

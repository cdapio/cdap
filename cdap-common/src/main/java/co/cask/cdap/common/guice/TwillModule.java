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
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
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
    bind(TwillRunnerService.class).to(YarnTwillRunnerService.class).in(Scopes.SINGLETON);
    bind(TwillRunner.class).to(TwillRunnerService.class);

    expose(TwillRunnerService.class);
    expose(TwillRunner.class);
  }

  /**
   * Provider method for instantiating {@link YarnTwillRunnerService}.
   */
  @Singleton
  @Provides
  private YarnTwillRunnerService provideYarnTwillRunnerService(CConfiguration configuration,
                                                               YarnConfiguration yarnConfiguration,
                                                               LocationFactory locationFactory) {
    String zkConnectStr = configuration.get(Constants.Zookeeper.QUORUM) +
                          configuration.get(Constants.CFG_TWILL_ZK_NAMESPACE);

    // Copy the yarn config and set the max heap ratio.
    YarnConfiguration yarnConfig = new YarnConfiguration(yarnConfiguration);
    yarnConfig.set(Constants.CFG_TWILL_RESERVED_MEMORY_MB, configuration.get(Constants.CFG_TWILL_RESERVED_MEMORY_MB));
    YarnTwillRunnerService runner = new YarnTwillRunnerService(yarnConfig,
                                                               zkConnectStr,
                                                               LocationFactories.namespace(locationFactory, "twill"));

    // Set JVM options based on configuration
    runner.setJVMOptions(configuration.get(Constants.AppFabric.PROGRAM_JVM_OPTS));

    return runner;
  }
}

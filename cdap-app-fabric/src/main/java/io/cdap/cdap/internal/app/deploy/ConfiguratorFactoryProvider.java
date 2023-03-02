/*
 * Copyright © 2021 Cask Data, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package io.cdap.cdap.internal.app.deploy;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.name.Names;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;

/**
 * Provider for {@link ConfiguratorFactory}. Use the "remote" binding of {@link ConfiguratorFactory}
 * if worker pool is enabled. Use  the "local" binding of {@link ConfiguratorFactory} if worker pool
 * is disabled.
 */
public class ConfiguratorFactoryProvider implements Provider<ConfiguratorFactory> {

  private final CConfiguration cConf;
  private final Injector injector;

  @Inject
  ConfiguratorFactoryProvider(CConfiguration cConf, Injector injector) {
    this.cConf = cConf;
    this.injector = injector;
  }

  @Override
  public ConfiguratorFactory get() {
    boolean workerPoolEnabled = cConf.getBoolean(Constants.TaskWorker.POOL_ENABLE);
    if (workerPoolEnabled) {
      return injector.getInstance(Key.get(ConfiguratorFactory.class,
          Names.named(Constants.AppFabric.FACTORY_IMPLEMENTATION_REMOTE)));
    }
    return injector.getInstance(Key.get(ConfiguratorFactory.class,
        Names.named(Constants.AppFabric.FACTORY_IMPLEMENTATION_LOCAL)));
  }
}

/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.sourcecontrol;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.name.Names;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;

/**
 *
 */
public class SourceControlOperatorFactoryProvider implements Provider<SourceControlOperationRunnerFactory> {

  private final CConfiguration cConf;
  private final Injector injector;

  @Inject
  SourceControlOperatorFactoryProvider(CConfiguration cConf, Injector injector) {
    this.cConf = cConf;
    this.injector = injector;
  }

  @Override
  public SourceControlOperationRunnerFactory get() {
    boolean workerPoolEnabled = cConf.getBoolean(Constants.TaskWorker.POOL_ENABLE);
    if (workerPoolEnabled) {
      return injector.getInstance(Key.get(SourceControlOperationRunnerFactory.class,
                                          Names.named(Constants.AppFabric.FACTORY_IMPLEMENTATION_REMOTE)));
    }
    return injector.getInstance(Key.get(SourceControlOperationRunnerFactory.class,
                                        Names.named(Constants.AppFabric.FACTORY_IMPLEMENTATION_LOCAL)));
  }
}

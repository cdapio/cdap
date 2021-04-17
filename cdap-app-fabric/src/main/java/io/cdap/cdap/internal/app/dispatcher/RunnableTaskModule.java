/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.dispatcher;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import io.cdap.cdap.common.conf.CConfiguration;

/**
 * RunnableTask Module. Guice binding modules for all RunnableTasks should be installed here.
 */
public class RunnableTaskModule extends AbstractModule {

  CConfiguration cConfig;

  public RunnableTaskModule(CConfiguration cConfig) {
    this.cConfig = cConfig;
  }

  @Override
  protected void configure() {
    bind(CConfiguration.class).toInstance(this.cConfig);
    install(new ConfiguratorTaskModule());
  }

  @Provides
  public EchoRunnableTask getEchoRunnableTask() {
    return new EchoRunnableTask();
  }
}

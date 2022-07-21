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

package io.cdap.cdap.internal.app.worker;

import com.google.inject.AbstractModule;
import io.cdap.cdap.common.conf.CConfiguration;

/**
 * Module for Runnable tasks.
 */
public class RunnableTaskModule extends AbstractModule {

  private final CConfiguration cConf;

  public RunnableTaskModule(CConfiguration cConf) {
    this.cConf = cConf;
  }

  @Override
  protected void configure() {
    bind(CConfiguration.class).toInstance(cConf);
  }
}

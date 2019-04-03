/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.app;

import co.cask.cdap.api.Config;
import co.cask.cdap.api.app.ApplicationContext;

/**
 * Default Implementation of {@link ApplicationContext}.
 *
 * @param <T> config class
 */
public class DefaultApplicationContext<T extends Config> implements ApplicationContext<T> {
  private final T appConfig;

  public DefaultApplicationContext() {
    this.appConfig = null;
  }

  public DefaultApplicationContext(T appConfig) {
    this.appConfig = appConfig;
  }

  @Override
  public T getConfig() {
    return appConfig;
  }
}

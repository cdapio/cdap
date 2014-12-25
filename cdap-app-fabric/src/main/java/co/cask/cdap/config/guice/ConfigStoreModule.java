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

package co.cask.cdap.config.guice;

import co.cask.cdap.config.ConfigStore;
import co.cask.cdap.config.DefaultConfigStore;
import com.google.inject.AbstractModule;
import com.google.inject.Module;

/**
 * Configuration Store Guice Modules.
 */
public class ConfigStoreModule {

  public Module getInMemoryModule() {
    return getModule();
  }

  public Module getStandaloneModule() {
    return getModule();
  }

  public Module getDistributedModule() {
    return getModule();
  }

  private Module getModule() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(ConfigStore.class).to(DefaultConfigStore.class);
      }
    };
  }
}

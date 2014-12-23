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

package co.cask.cdap.app.guice;

import co.cask.cdap.app.config.ConfigService;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.gateway.handlers.DashboardConfigService;
import co.cask.cdap.gateway.handlers.PreferencesConfigService;
import co.cask.cdap.gateway.handlers.UserSettingsConfigService;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.name.Names;

/**
 * ConfigService Guice Modules.
 */
public class ConfigServiceModules {

  private Module getModule() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(ConfigService.class).annotatedWith(Names.named(Constants.ConfigService.USERSETTING)).
          to(UserSettingsConfigService.class).in(Scopes.SINGLETON);
        bind(ConfigService.class).annotatedWith(Names.named(Constants.ConfigService.PREFERENCE_SETTING)).
          to(PreferencesConfigService.class).in(Scopes.SINGLETON);
        bind(ConfigService.class).annotatedWith(Names.named(Constants.ConfigService.DASHBOARD)).
          to(DashboardConfigService.class).in(Scopes.SINGLETON);
      }
    };
  }

  public Module getInMemoryModule() {
    return getModule();
  }

  public Module getStandaloneModule() {
    return getModule();
  }

  public Module getDistributedModule() {
    return getModule();
  }
}

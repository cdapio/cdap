/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.app.upgrade;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import io.cdap.cdap.internal.app.upgrade.ApplicationPluginMappingFetcher;
import io.cdap.cdap.internal.app.upgrade.DefaultUpgradeManager;
import io.cdap.cdap.internal.app.upgrade.MetadataApplicationPluginMappingFetcher;

/**
 * Module that configures Upgrade related classes.
 */
public class UpgradeModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(UpgradeManager.class).to(DefaultUpgradeManager.class).in(Scopes.SINGLETON);
    bind(ApplicationPluginMappingFetcher.class).to(MetadataApplicationPluginMappingFetcher.class)
        .in(Scopes.SINGLETON);
  }
}

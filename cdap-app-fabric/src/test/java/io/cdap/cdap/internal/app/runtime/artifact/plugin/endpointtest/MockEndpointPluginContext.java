/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.artifact.plugin.endpointtest;


import co.cask.cdap.api.plugin.EndpointPluginContext;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.api.plugin.PluginSelector;

import javax.annotation.Nullable;

/**
 * mock endpoint plugin context
 */
public class MockEndpointPluginContext implements EndpointPluginContext {

  @Nullable
  @Override
  public <T> Class<T> loadPluginClass(String pluginType, String pluginName, PluginProperties pluginProperties) {
    return null;
  }

  @Nullable
  @Override
  public <T> Class<T> loadPluginClass(String pluginType, String pluginName,
                                      PluginProperties pluginProperties, PluginSelector pluginSelector) {
    return null;
  }

  @Nullable
  @Override
  public <T> Class<T> loadPluginClass(String pluginType, String pluginName) {
    return null;
  }
}

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

package co.cask.cdap.internal.api;

import co.cask.cdap.api.plugin.PluginConfigurer;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.api.plugin.PluginSelector;

import javax.annotation.Nullable;

/**
 * Helper methods to add plugins to Programs.
 * @param <T> Program's configurer that implements {@link PluginConfigurer}
 */
public abstract class AbstractPluginConfigurable<T extends PluginConfigurer>
  extends AbstractProgramDatasetConfigurable {

  protected abstract T getConfigurer();

  @Nullable
  public <M> M usePlugin(String pluginType, String pluginName, String pluginId, PluginProperties properties) {
    return getConfigurer().usePlugin(pluginType, pluginName, pluginId, properties);
  }

  @Nullable
  public <M> M usePlugin(String pluginType, String pluginName,
                         String pluginId, PluginProperties properties, PluginSelector selector) {
    return getConfigurer().usePlugin(pluginType, pluginName, pluginId, properties, selector);
  }

  @Nullable
  public <M> Class<M> usePluginClass(String pluginType, String pluginName, String pluginId,
                                     PluginProperties properties) {
    return getConfigurer().usePluginClass(pluginType, pluginName, pluginId, properties);
  }

  @Nullable
  public <M> Class<M> usePluginClass(String pluginType, String pluginName,
                                     String pluginId, PluginProperties properties, PluginSelector selector) {
    return getConfigurer().usePluginClass(pluginType, pluginName, pluginId, properties, selector);
  }
}

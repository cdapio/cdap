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

package co.cask.cdap.etl.common;

import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.etl.api.TransformContext;

/**
 * Context that scopes plugin ids by the id of the transform. This allows multiple transforms to use plugins with
 * the same id without clobbering each other.
 */
public abstract class ScopedPluginContext implements TransformContext {
  protected final String stageId;

  public ScopedPluginContext(String stageId) {
    this.stageId = stageId;
  }

  @Override
  public PluginProperties getPluginProperties(String pluginId) {
    return getScopedPluginProperties(scopePluginId(pluginId));
  }

  @Override
  public <T> T newPluginInstance(String pluginId) throws InstantiationException {
    return newScopedPluginInstance(scopePluginId(pluginId));
  }

  @Override
  public <T> Class<T> loadPluginClass(String pluginId) {
    return loadScopedPluginClass(scopePluginId(pluginId));
  }

  protected abstract <T> T newScopedPluginInstance(String scopedPluginId) throws InstantiationException;

  protected abstract <T> Class<T> loadScopedPluginClass(String scopedPluginId);

  protected abstract PluginProperties getScopedPluginProperties(String scopedPluginId);

  private String scopePluginId(String childPluginId) {
    return String.format("%s%s%s", stageId, Constants.ID_SEPARATOR, childPluginId);
  }

}

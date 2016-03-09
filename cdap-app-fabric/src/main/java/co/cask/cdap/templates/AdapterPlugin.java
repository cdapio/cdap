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

package co.cask.cdap.templates;

import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.plugin.PluginProperties;

/**
 * A container class for holding plugin information for an adapter instance. Only exists because its part of
 * AdapterDefinition, which is only around because its needed for upgrade.
 */
@Deprecated
public final class AdapterPlugin {
  private final PluginInfo pluginInfo;
  private final PluginClass pluginClass;
  private final PluginProperties properties;

  AdapterPlugin(PluginInfo pluginInfo, PluginClass pluginClass, PluginProperties properties) {
    this.pluginInfo = pluginInfo;
    this.pluginClass = pluginClass;
    this.properties = properties;
  }

  /**
   * Returns the plugin information.
   */
  public PluginInfo getPluginInfo() {
    return pluginInfo;
  }

  /**
   * Returns the plugin class information.
   */
  public PluginClass getPluginClass() {
    return pluginClass;
  }

  /**
   * Returns the set of properties available for the plugin when the adapter was created.
   */
  public PluginProperties getProperties() {
    return properties;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AdapterPlugin that = (AdapterPlugin) o;

    return pluginInfo.equals(that.pluginInfo)
      && pluginClass.equals(that.pluginClass)
      && properties.equals(that.properties);
  }

  @Override
  public int hashCode() {
    int result = pluginInfo.hashCode();
    result = 31 * result + pluginClass.hashCode();
    result = 31 * result + properties.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "AdapterPlugin{" +
      "pluginClass=" + pluginClass +
      ", pluginInfo=" + pluginInfo +
      ", properties=" + properties +
      '}';
  }
}

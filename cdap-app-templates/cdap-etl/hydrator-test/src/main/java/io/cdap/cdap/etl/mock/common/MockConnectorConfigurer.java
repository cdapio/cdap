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
 *
 */

package io.cdap.cdap.etl.mock.common;

import io.cdap.cdap.api.macro.InvalidMacroException;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.macro.MacroParserOptions;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.api.plugin.PluginSelector;
import io.cdap.cdap.etl.api.connector.ConnectorConfigurer;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Mock connector configurer
 */
public class MockConnectorConfigurer implements ConnectorConfigurer {

  private final Map<String, Object> plugins;

  public MockConnectorConfigurer() {
    this(Collections.emptyMap());
  }

  public MockConnectorConfigurer(Map<String, Object> plugins) {
    this.plugins = plugins;
  }

  @Nullable
  @Override
  public <T> T usePlugin(String pluginType, String pluginName, String pluginId,
      PluginProperties properties,
      PluginSelector selector) {
    return (T) plugins.get(pluginId);
  }

  @Nullable
  @Override
  public <T> Class<T> usePluginClass(String pluginType, String pluginName, String pluginId,
      PluginProperties properties,
      PluginSelector selector) {
    return (Class<T>) plugins.get(pluginId).getClass();
  }

  @Override
  public Map<String, String> evaluateMacros(Map<String, String> properties,
      MacroEvaluator evaluator,
      MacroParserOptions options) throws InvalidMacroException {
    return properties;
  }
}

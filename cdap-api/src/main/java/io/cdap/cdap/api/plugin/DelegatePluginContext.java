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

package io.cdap.cdap.api.plugin;

import io.cdap.cdap.api.macro.InvalidMacroException;
import io.cdap.cdap.api.macro.MacroEvaluator;

import java.util.function.Function;

public interface DelegatePluginContext extends PluginContext {
  PluginContext getPluginContextDelegate();

  @Override
  default PluginProperties getPluginProperties(String pluginId) {
    return getPluginContextDelegate().getPluginProperties(pluginId);
  }

  @Override
  default PluginProperties getPluginProperties(String pluginId, MacroEvaluator evaluator) throws InvalidMacroException {
    return getPluginContextDelegate().getPluginProperties(pluginId, evaluator);
  }

  @Override
  default <T> Class<T> loadPluginClass(String pluginId) {
    return getPluginContextDelegate().loadPluginClass(pluginId);
  }

  @Override
  default <T> T newPluginInstance(String pluginId) throws InstantiationException {
    return getPluginContextDelegate().newPluginInstance(pluginId);
  }

  @Override
  default <T> T newPluginInstance(String pluginId, MacroEvaluator evaluator)
    throws InstantiationException, InvalidMacroException {
    return getPluginContextDelegate().newPluginInstance(pluginId, evaluator);
  }

  @Override
  default <S, T> T newPluginInstance(String pluginId, MacroEvaluator evaluator, Function<S, T> pluginInitializer)
    throws InstantiationException, InvalidMacroException {
    return getPluginContextDelegate().newPluginInstance(pluginId, evaluator, pluginInitializer);
  }
}

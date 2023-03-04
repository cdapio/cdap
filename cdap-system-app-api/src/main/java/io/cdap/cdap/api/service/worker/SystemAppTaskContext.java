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

package io.cdap.cdap.api.service.worker;

import io.cdap.cdap.api.ServiceDiscoverer;
import io.cdap.cdap.api.artifact.ArtifactManager;
import io.cdap.cdap.api.feature.FeatureFlagsProvider;
import io.cdap.cdap.api.macro.InvalidMacroException;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.macro.MacroParserOptions;
import io.cdap.cdap.api.plugin.PluginConfigurer;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.api.service.http.ServicePluginConfigurer;
import java.io.IOException;
import java.util.Map;

/**
 * System App context for a remote task
 */
public interface SystemAppTaskContext extends ServiceDiscoverer, SecureStore, AutoCloseable,
    FeatureFlagsProvider {

  /**
   * Fetch preferences for the given namespace.
   *
   * @param namespace the name of the namespace to fetch preferences for.
   * @param resolved true if resolved properties are desired.
   * @return Map containing Preferences keys and values.
   * @throws IOException if the preferences for the supplied namespace could not be fetched.
   * @throws IllegalArgumentException if the namespace doesn't exist.
   */
  Map<String, String> getPreferencesForNamespace(String namespace, boolean resolved)
      throws Exception;

  /**
   * Create a {@link PluginConfigurer} that can be used to instantiate plugins at runtime.
   *
   * @param namespace the namespace for user scoped plugins
   * @return a dynamic plugin configurer that must be closed
   */
  PluginConfigurer createPluginConfigurer(String namespace) throws IOException;

  /**
   * Create a {@link ServicePluginConfigurer} that can be used to instantiate plugins with macro
   * evaluation
   *
   * @param namespace the namespace for user scoped plugins.
   * @return a plugin configurer specifically for service.
   */
  ServicePluginConfigurer createServicePluginConfigurer(String namespace);

  /**
   * Evaluates macros using provided macro evaluator with the provided parsing options.
   *
   * @param namespace namespace in which macros needs to be evaluated
   * @param macros key-value map of properties to evaluate
   * @param evaluator macro evaluator to be used to evaluate macros
   * @param options macro parsing options
   * @return map of evaluated macros
   * @throws InvalidMacroException indicates that there is an invalid macro
   */
  Map<String, String> evaluateMacros(String namespace, Map<String, String> macros,
      MacroEvaluator evaluator,
      MacroParserOptions options) throws InvalidMacroException;

  /**
   * @return {@link ArtifactManager} for artifact listing and class loading
   */
  ArtifactManager getArtifactManager();

  /**
   * @return String service name
   */
  String getServiceName();
}

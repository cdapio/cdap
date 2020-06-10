/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.api.app;

import io.cdap.cdap.api.Config;
import io.cdap.cdap.api.artifact.ArtifactId;

import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactVersionRange;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Context for updating Application configs. Provides helper methods for application to support operations like config
 * upgrade.
 */
public interface ApplicationUpdateContext {

  /**
   * @return All update actions application should perform on the config.
   */
  List<ApplicationConfigUpdateAction> getUpdateActions();

  /**
   * Get the old config as an object of the given type. The platform would perform the json deserialization based on
   * the provided type. This is for the case where an application has the same/compatible/old config class. Application
   * should decide on how they want to convert config from old to current type.
   *
   * @param configType type of the config platform should deserialize to.
   * @return application config serialized to an object of given configType.
   */
  <C extends Config> C getConfig(Type configType);

  /**
   * Get the application configuration as json string.
   */
  String getConfigAsString();

  /**
   * Returns list of available plugin artifacts based on given parameters.
   *
   * @param pluginType the plugin type.
   * @param pluginName the plugin name.
   * @param pluginRange the range of the version candidate plugins should be in.
   * @return artifact list of plugins which matches with given parameters, sorted in ascending order.
   *         Returns empty list if no artifact for the plugin found.
   */
  default List<ArtifactId> getPluginArtifacts(String pluginType, String pluginName,
                                              @Nullable ArtifactVersionRange pluginRange) throws Exception {
    return getPluginArtifacts(pluginType, pluginName, pluginRange, Integer.MAX_VALUE);
  }

  /**
   * Returns list of available plugin artifacts based on given parameters.
   *
   * @param pluginType the plugin type.
   * @param pluginName the plugin name.
   * @param pluginRange the range of the version candidate plugins should be in.
   * @param limit number of results to return at max, if null, default will be INT_MAX.
   * @return artifact list of plugins which matches with given parameters, sorted in ascending order.
   *         Returns empty list if no artifact for the plugin found.
   */
  List<ArtifactId> getPluginArtifacts(String pluginType, String pluginName,
                                      @Nullable ArtifactVersionRange pluginRange, int limit) throws Exception;

}


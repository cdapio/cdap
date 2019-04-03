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

package co.cask.cdap.etl.proto;

import javax.annotation.Nullable;

/**
 * Context for upgrading configs.
 */
public interface UpgradeContext {

  /**
   * Gets artifact information for a given plugin type and name.
   *
   * @param pluginType the plugin type
   * @param pluginName the plugin name
   * @return artifact information for the plugin type and name.
   *         returns null if no artifact for the plugin could be found.
   */
  @Nullable
  ArtifactSelectorConfig getPluginArtifact(String pluginType, String pluginName);

}

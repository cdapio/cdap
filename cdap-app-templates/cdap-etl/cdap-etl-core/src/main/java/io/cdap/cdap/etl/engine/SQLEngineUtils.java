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

package io.cdap.cdap.etl.engine;

/**
 * Utilitycalled methods for the SQL Engine.
 */
public class SQLEngineUtils {

  /**
   * Builds a new stage name for the SQL Engine implementation.
   * @param pluginName the name of the plugin to use
   * @return new name for the SQL engine stage.
   */
  public static String buildStageName(String pluginName) {
    if (pluginName == null) {
      pluginName = "";
    }

    return "sqlengine_" + pluginName.toLowerCase();
  }
}

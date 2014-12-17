/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.app.config;

import com.google.common.util.concurrent.Service;

import java.util.List;
import java.util.Map;

/**
 * Service to store/retrieve configuration settings.
 */
public interface ConfigService extends Service {

  /**
   * Write setting to a Configuration.
   * @param namespace Namespace
   * @param type Configuration Type
   * @param name Name of the Configuration
   * @param key Key
   * @param value Value
   * @throws Exception
   */
  void writeSetting(String namespace, ConfigType type, String name, String key, String value) throws Exception;

  /**
   * Write a Map of settings to a Configuration.
   * @param namespace Namespace
   * @param type Configuration Type
   * @param name Name of the Configuration
   * @param settingsMap Map of Key/Value
   * @throws Exception
   */
  void writeSetting(String namespace, ConfigType type, String name, Map<String, String> settingsMap) throws Exception;

  /**
   * Read a setting of a Configuration.
   * @param namespace Namespace
   * @param type Configuration Type
   * @param name Name of the Configuration
   * @param key Key
   * @return Value
   * @throws Exception
   */
  String readSetting(String namespace, ConfigType type, String name, String key) throws Exception;

  /**
   * Read settings of a Configuration.
   * @param namespace Namespace
   * @param type Configuration Type
   * @param name Name of the Configuration
   * @return Map of Key/Value
   * @throws Exception
   */
  Map<String, String> readSetting(String namespace, ConfigType type, String name) throws Exception;

  /**
   * Delete a setting of a Configuration.
   * @param namespace Namespace
   * @param type Configuration Type
   * @param name Name of the Configuration
   * @param key Key
   * @throws Exception
   */
  void deleteSetting(String namespace, ConfigType type, String name, String key) throws Exception;

  /**
   * Delete all the settings of a Configuration.
   * @param namespace Namespace
   * @param type Configuration Type
   * @param name Name of the Configuration
   * @throws Exception
   */
  void deleteSetting(String namespace, ConfigType type, String name) throws Exception;

  /**
   * Delete a Configuration.
   * @param namespace Namespace
   * @param type Configuration Type
   * @param accId Account Id
   * @param name Name of the Configuration
   * @throws Exception
   */
  void deleteConfig(String namespace, ConfigType type, String accId, String name) throws Exception;

  /**
   * Create a Configuration.
   * @param namespace Namespace
   * @param type Configuration Type
   * @param accId Account Id
   * @return Configuration Id
   * @throws Exception
   */
  String createConfig(String namespace, ConfigType type, String accId) throws Exception;

  /**
   * Get the list of Configurations belonging to a specific Account Id.
   * @param namespace Namespace
   * @param type Configuration Type
   * @param accId Account Id
   * @return List of Configuration Ids
   * @throws Exception
   */
  List<String> getConfig(String namespace, ConfigType type, String accId) throws Exception;

  /**
   * Get the list of Configurations.
   * @param namespace Namespace
   * @param type Configuration Type
   * @return List of Configuration Ids
   * @throws Exception
   */
  List<String> getConfig(String namespace, ConfigType type) throws Exception;

  /**
   * Check if a Configuration exists.
   * @param namespace Namespace
   * @param type Configuration Type
   * @param name Name of the Configuration
   * @return True if the configuration exists
   * @throws Exception
   */
  boolean checkConfig(String namespace, ConfigType type, String name) throws Exception;
}

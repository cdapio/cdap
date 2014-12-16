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

  void writeSetting(ConfigType type, String name, String key, String value) throws Exception;

  void writeSetting(ConfigType type, String name, Map<String, String> settingsMap) throws Exception;

  String readSetting(ConfigType type, String name, String key) throws Exception;

  Map<String, String> readSetting(ConfigType type, String name) throws Exception;

  void deleteSetting(ConfigType type, String name, String key) throws Exception;

  void deleteConfig(ConfigType type, String accId, String name) throws Exception;

  String createConfig(ConfigType type, String accId) throws Exception;

  List<String> getConfig(ConfigType type, String accId) throws Exception;

  List<String> getConfig(ConfigType type) throws Exception;

  boolean checkConfig(ConfigType type, String name) throws Exception;
}

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

package co.cask.cdap.config;

import java.util.List;

/**
 * Configuration Store.
 */
public interface ConfigStore {

  /**
   * Create a Configuration.
   * @param namespace namespace
   * @param type configuration type
   * @param config configuration object
   * @throws Exception
   */
  void create(String namespace, String type, Config config) throws Exception;

  /**
   * Delete a Configuration.
   * @param namespace namespace
   * @param type configuration type
   * @param id name of the configuration
   * @throws Exception
   */
  void delete(String namespace, String type, String id) throws Exception;

  /**
   * List all Configurations which are of a specific type.
   * @param namespace namespace
   * @param type configuration type
   * @return list of {@link Config} objects
   * @throws Exception
   */
  List<Config> list(String namespace, String type) throws Exception;

  /**
   * Read a Configuration.
   * @param namespace namespace
   * @param type configuration type
   * @param id name of the configuration
   * @return {@link Config}
   * @throws Exception
   */
  Config get(String namespace, String type, String id) throws Exception;

  /**
   * Update a Configuration.
   * @param namespace namespace
   * @param type configuration type
   * @param config {@link Config}
   * @throws Exception
   */
  void update(String namespace, String type, Config config) throws Exception;
}

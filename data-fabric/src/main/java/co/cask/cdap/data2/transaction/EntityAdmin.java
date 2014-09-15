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

package co.cask.cdap.data2.transaction;

import java.util.Properties;
import javax.annotation.Nullable;

/**
 * Common interface for managing persisted entities.
 * TODO: refactor this out once queues and streams evolve and seprate their ways.
 */
public interface EntityAdmin {
  /**
   * @param name entity name
   * @return true if entity with given name exists, otherwise false
   * @throws Exception if check fails
   */
  boolean exists(String name) throws Exception;

  /**
   * Creates entity if doesn't exist. If entity exists does nothing.
   * @param name name of the entity to create
   * @throws Exception if creation fails
   */
  void create(String name) throws Exception;

  /**
   * Creates entity if doesn't exist. If entity exists does nothing.
   * @param name name of the entity to create
   * @param props additional properties
   * @throws Exception if creation fails
   */
  void create(String name, @Nullable Properties props) throws Exception;

  /**
   * Wipes out entity data.
   * @param name entity name
   * @throws Exception if cleanup fails
   */
  void truncate(String name) throws Exception;

  /**
   * Deletes entity from the system completely.
   * @param name entity name
   * @throws Exception if deletion fails
   */
  void drop(String name) throws Exception;

  /**
   * Performs update of entity.
   *
   * @param name Name of the entity to update
   * @throws Exception if update fails
   */
  void upgrade(String name, Properties properties) throws Exception;
}

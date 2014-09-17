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

package co.cask.cdap.api.security;

/**
 * Represents groups of operations that is categorized under a permission type.
 */
public enum PermissionType {

  /**
   * Allows listing objects belonging to an entity.
   *
   * For example, with this permission on a namespace, the user may list all applications under a namespace.
   */
  LIST,

  /**
   * Allows reading data belonging to an entity.
   *
   * For example, with this permission on a dataset, the user may read the data of the dataset.
   */
  READ,

  /**
   * Allows writing data belonging to an entity.
   */
  WRITE,

  /**
   * Alows executing code belonging to an entity.
   */
  EXECUTE,

  /**
   * Allows administrative operations on an entity.
   */
  ADMIN

}

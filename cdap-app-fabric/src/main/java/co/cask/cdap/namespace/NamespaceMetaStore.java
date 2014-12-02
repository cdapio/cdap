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

package co.cask.cdap.namespace;

import java.util.List;

/**
 * API to store/retrieve/delete namespace metadata
 */
public interface NamespaceMetaStore {

  /**
   * Creates a new namespace
   * @param name name of the new namespace. This field is immutable. TODO: Should we call it id?
   * @param displayName display name of the new namespace. We may allow users to update this field later.
   * @param description description of the new namespace
   * @throws java.lang.Exception if problems occur while creating the new namespace
   */
  void create(String name, String displayName, String description) throws Exception;

  /**
   * Retrieves a namespace from the namespace metadata store
   * @param name name of the requested namespace
   * @return {@link co.cask.cdap.namespace.NamespaceMetadata} of the requested namespace
   * @throws java.lang.Exception if problems occur while retrieving the namespace
   */
  NamespaceMetadata get(String name) throws Exception;

  /**
   * Deletes a namespace from the namespace metadata store
   * @param name name of the namespace to delete
   * @throws java.lang.Exception if problems occur while deleting the namespace
   * @throws java.lang.Exception if problems occur while deleting the namespace
   */
  void delete(String name) throws Exception;

  /**
   * Lists all registered namespaces
   * @return a list of all registered namespaces
   * java.lang.Exception if problems occur while listing namespaces
   */
  List<NamespaceMetadata> list() throws Exception;

  /**
   * Check if namespace already exists
   * @param name name of the requested namespace to check for existence
   * @return true if the namespace already exists, false otherwise
   * java.lang.Exception if problems occur while checking namespace status
   */
  boolean exists(String name) throws Exception;
}

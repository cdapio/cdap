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

import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;

import java.util.List;

/**
 * API to store/retrieve/delete namespace metadata
 */
public interface NamespaceMetaStore {

  /**
   * Creates a new namespace
   * @param metadata {@link NamespaceMeta} representing the namespace metadata
   * @throws java.lang.Exception if problems occur while creating the new namespace
   */
  void create(NamespaceMeta metadata) throws Exception;

  /**
   * Retrieves a namespace from the namespace metadata store
   * @param id {@link Id.Namespace} of the requested namespace
   * @return {@link co.cask.cdap.proto.NamespaceMeta} of the requested namespace
   * @throws java.lang.Exception if problems occur while retrieving the namespace
   */
  NamespaceMeta get(Id.Namespace id) throws Exception;

  /**
   * Deletes a namespace from the namespace metadata store
   * @param id {@link Id.Namespace} of the namespace to delete
   * @throws java.lang.Exception if problems occur while deleting the namespace
   * @throws java.lang.Exception if problems occur while deleting the namespace
   */
  void delete(Id.Namespace id) throws Exception;

  /**
   * Lists all registered namespaces
   * @return a list of all registered namespaces
   * java.lang.Exception if problems occur while listing namespaces
   */
  List<NamespaceMeta> list() throws Exception;

  /**
   * Check if namespace already exists
   * @param id {@link Id.Namespace} of the requested namespace to check for existence
   * @return true if the namespace already exists, false otherwise
   * java.lang.Exception if problems occur while checking namespace status
   */
  boolean exists(Id.Namespace id) throws Exception;
}

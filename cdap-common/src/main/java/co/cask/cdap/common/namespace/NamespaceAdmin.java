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

package co.cask.cdap.common.namespace;

import co.cask.cdap.common.NamespaceAlreadyExistsException;
import co.cask.cdap.common.NamespaceCannotBeCreatedException;
import co.cask.cdap.common.NamespaceCannotBeDeletedException;
import co.cask.cdap.common.NamespaceNotFoundException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.NamespaceId;

/**
 * Admin class for managing a namespace's lifecycle
 */
public interface NamespaceAdmin extends NamespaceQueryAdmin {

  /**
   * Creates a new namespace.
   *
   * @param metadata the {@link NamespaceMeta} for the new namespace to be created
   * @throws NamespaceAlreadyExistsException if the specified namespace already exists
   * @throws NamespaceCannotBeCreatedException if the creation operation was unsuccessful
   */
  void create(NamespaceMeta metadata) throws Exception;

  /**
   * Deletes the specified namespace.
   *
   * @param namespaceId the {@link NamespaceId} of the specified namespace
   * @throws NamespaceNotFoundException if the specified namespace does not exist
   * @throws NamespaceCannotBeDeletedException if the deletion operation was unsuccessful
   */
  void delete(NamespaceId namespaceId) throws Exception;

  /**
   * Deletes all datasets in the specified namespace.
   *
   * @param namespaceId the {@link NamespaceId} of the specified namespace
   * @throws NotFoundException if the specified namespace does not exist
   * @throws NamespaceCannotBeDeletedException if the deletion operation was unsuccessful
   */
  void deleteDatasets(NamespaceId namespaceId) throws Exception;

  /**
   * Update namespace properties for a given namespace.
   *
   * @param namespaceId  the {@link NamespaceId} of the namespace to be updated
   * @param namespaceMeta namespace meta to update
   * @throws NotFoundException if the specified namespace is not found
   */
  void updateProperties(NamespaceId namespaceId, NamespaceMeta namespaceMeta) throws Exception;
}

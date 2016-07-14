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

package co.cask.cdap.internal.app.namespace;

import co.cask.cdap.explore.service.ExploreException;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.NamespaceId;

import java.io.IOException;
import java.sql.SQLException;

/**
 * Performs namespace admin operations on underlying storage (HBase, Filesystem, Hive, etc)
 */
public interface StorageProviderNamespaceAdmin {

  /**
   * Create a namespace in the storage providers.
   * Can perform operations such as creating directories, creating namespaces, etc.
   *
   * @param namespaceMeta {@link NamespaceMeta} for the namespace to create
   * @throws IOException if there are errors while creating the namespace
   */
  void create(NamespaceMeta namespaceMeta) throws IOException, ExploreException, SQLException;

  /**
   * Delete a namespace from the storage providers.
   * Can perform operations such as deleting directories, deleting namespaces, etc.
   *
   * @param namespaceId {@link NamespaceId} for the namespace to delete
   * @throws IOException if there are errors while deleting the namespace
   */
  void delete(NamespaceId namespaceId) throws IOException, ExploreException, SQLException;
}

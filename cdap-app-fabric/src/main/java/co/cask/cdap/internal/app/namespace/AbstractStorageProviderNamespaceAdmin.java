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

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.explore.client.ExploreFacade;
import co.cask.cdap.explore.service.ExploreException;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.NamespaceId;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;

/**
 * Performs common namespace admin operations on storage providers (HBase, Filesystem, Hive, etc)
 */
abstract class AbstractStorageProviderNamespaceAdmin implements StorageProviderNamespaceAdmin {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractStorageProviderNamespaceAdmin.class);

  private final CConfiguration cConf;
  private final NamespacedLocationFactory namespacedLocationFactory;
  private final ExploreFacade exploreFacade;

  AbstractStorageProviderNamespaceAdmin(CConfiguration cConf, NamespacedLocationFactory namespacedLocationFactory,
                                        ExploreFacade exploreFacade) {
    this.cConf = cConf;
    this.namespacedLocationFactory = namespacedLocationFactory;
    this.exploreFacade = exploreFacade;
  }

  /**
   * Create a namespace in the File System and Hive.
   *
   * @param namespaceMeta {@link NamespaceMeta} for the namespace to create
   * @throws IOException if there are errors while creating the namespace in the File System
   * @throws ExploreException if there are errors while deleting the namespace in Hive
   * @throws SQLException if there are errors while deleting the namespace in Hive
   */
  @Override
  public void create(NamespaceMeta namespaceMeta) throws IOException, ExploreException, SQLException {
    Location namespaceHome = namespacedLocationFactory.get(namespaceMeta.getNamespaceId().toId());
    if (namespaceHome.exists()) {
      LOG.warn("Home directory '{}' for namespace '{}' already exists. Deleting it.",
               namespaceHome, namespaceMeta.getName());
      if (!namespaceHome.delete(true)) {
        throw new IOException(String.format("Error while deleting home directory '%s' for namespace '%s'",
                                            namespaceHome, namespaceMeta.getName()));
      }
    }
    if (!namespaceHome.mkdirs()) {
      throw new IOException(String.format("Error while creating home directory '%s' for namespace '%s'",
                                          namespaceHome, namespaceMeta.getName()));
    }

    if (cConf.getBoolean(Constants.Explore.EXPLORE_ENABLED)) {
      exploreFacade.createNamespace(namespaceMeta.getNamespaceId().toId());
    }
  }

  /**
   * Deletes the namespace directory on the FileSystem and Hive.
   *
   * @param namespaceId {@link NamespaceId} for the namespace to delete
   * @throws IOException if there are errors while deleting the namespace in the File System
   * @throws ExploreException if there are errors while deleting the namespace in Hive
   * @throws SQLException if there are errors while deleting the namespace in Hive
   */
  @Override
  public void delete(NamespaceId namespaceId) throws IOException, ExploreException, SQLException {
    // TODO: CDAP-1581: Implement soft delete
    Location namespaceHome = namespacedLocationFactory.get(namespaceId.toId());
    if (namespaceHome.exists()) {
      if (!namespaceHome.delete(true)) {
        throw new IOException(String.format("Error while deleting home directory '%s' for namespace '%s'",
                                            namespaceHome, namespaceId.getNamespace()));
      }
    } else {
      // warn that namespace home was not found and skip delete step
      LOG.warn(String.format("Home directory '%s' for namespace '%s' does not exist.",
                             namespaceHome, namespaceId));
    }

    if (cConf.getBoolean(Constants.Explore.EXPLORE_ENABLED)) {
      exploreFacade.removeNamespace(namespaceId.toId());
    }
  }
}

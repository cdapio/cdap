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

package co.cask.cdap.internal.app.namespace;

import co.cask.cdap.app.store.Store;
import co.cask.cdap.app.store.StoreFactory;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.exception.AlreadyExistsException;
import co.cask.cdap.common.exception.NotFoundException;
import co.cask.cdap.config.DashboardStore;
import co.cask.cdap.config.PreferencesStore;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * {@link AbstractIdleService} for managing namespaces
 */
public final class DefaultNamespaceAdmin implements NamespaceAdmin {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultNamespaceAdmin.class);
  private static final String NAMESPACE_ELEMENT_TYPE = "Namespace";

  private final Store store;
  private final PreferencesStore preferencesStore;
  private final DashboardStore dashboardStore;
  private final LocationFactory locationFactory;

  @Inject
  public DefaultNamespaceAdmin(StoreFactory storeFactory, PreferencesStore preferencesStore,
                               DashboardStore dashboardStore, LocationFactory locationFactory) {
    this.store = storeFactory.create();
    this.preferencesStore = preferencesStore;
    this.dashboardStore = dashboardStore;
    this.locationFactory = locationFactory;
  }

  /**
   * This should be removed once we stop support for v2 APIs, since 'default' namespace is only reserved for v2 APIs.
   */
  private void createDefaultNamespace() {
    NamespaceMeta.Builder builder = new NamespaceMeta.Builder();
    NamespaceMeta defaultNamespace = builder.setId(Constants.DEFAULT_NAMESPACE)
      .setName(Constants.DEFAULT_NAMESPACE)
      .setDescription(Constants.DEFAULT_NAMESPACE)
      .build();

    try {
      createNamespace(defaultNamespace);
      LOG.info("Successfully created 'default' namespace.");
    } catch (AlreadyExistsException e) {
      LOG.info("'default' namespace already exists.");
    } catch (IOException e) {
      // propagating it as an unchecked exception because this will be gone soon,
      // don't want to change method signatures till then
      LOG.info("Error while creating default namespace");
      Throwables.propagate(e);
    }
  }

  /**
   * Lists all namespaces
   *
   * @return a list of {@link NamespaceMeta} for all namespaces
   */
  public List<NamespaceMeta> listNamespaces() {
    return store.listNamespaces();
  }

  /**
   * Gets details of a namespace
   *
   * @param namespaceId the {@link Id.Namespace} of the requested namespace
   * @return the {@link NamespaceMeta} of the requested namespace
   * @throws NotFoundException if the requested namespace is not found
   */
  public NamespaceMeta getNamespace(Id.Namespace namespaceId) throws NotFoundException {
    NamespaceMeta ns = store.getNamespace(namespaceId);
    if (ns == null) {
      throw new NotFoundException(NAMESPACE_ELEMENT_TYPE, namespaceId.getId());
    }
    return ns;
  }

  /**
   * Checks if the specified namespace exists
   *
   * @param namespaceId the {@link Id.Namespace} to check for existence
   * @return true, if the specifed namespace exists, false otherwise
   */
  public boolean hasNamespace(Id.Namespace namespaceId) {
    boolean exists = true;
    try {
      getNamespace(namespaceId);
    } catch (NotFoundException e) {
      // TODO: CDAP-1213 this will move to StandaloneMain/MasterServiceMain very soon
      if (Constants.DEFAULT_NAMESPACE.equals(namespaceId.getId())) {
        createDefaultNamespace();
      } else {
        exists = false;
      }
    }
    return exists;
  }

  /**
   * Creates a new namespace
   *
   * @param metadata the {@link NamespaceMeta} for the new namespace to be created
   * @throws AlreadyExistsException if the specified namespace already exists
   */
  public void createNamespace(NamespaceMeta metadata) throws AlreadyExistsException, IOException {
    // TODO: CDAP-1427 - This should be transactional, but we don't support transactions on files yet
    NamespaceMeta existing = store.createNamespace(metadata);
    if (existing != null) {
      throw new AlreadyExistsException(NAMESPACE_ELEMENT_TYPE, metadata.getId());
    }
    Location namespaceLocation = locationFactory.create(metadata.getId());
    String namespacePath = namespaceLocation.toURI().getPath();
    if (namespaceLocation.exists()) {
      throw new IOException(String.format("Namespace home directory %s already exists", namespacePath));
    }
    if (!namespaceLocation.mkdirs()) {
      throw new IOException(String.format("Error while creating namespace home directory %s", namespacePath));
    }
  }

  /**
   * Deletes the specified namespace
   *
   * @param namespaceId the {@link Id.Namespace} of the specified namespace
   * @throws NotFoundException if the specified namespace does not exist
   */
  public void deleteNamespace(Id.Namespace namespaceId) throws NotFoundException, IOException {
    //TODO: CDAP-870, CDAP-1427. Delete should be in a single transaction.
    // Delete Preferences associated with this namespace
    preferencesStore.deleteProperties(namespaceId.getId());

    // Delete all dashboards associated with this namespace
    dashboardStore.delete(namespaceId.getId());

    // Store#deleteNamespace already checks for existence
    NamespaceMeta deletedNamespace = store.deleteNamespace(namespaceId);
    if (deletedNamespace == null) {
      throw new NotFoundException(NAMESPACE_ELEMENT_TYPE, namespaceId.getId());
    }

    Location namespaceLocation = locationFactory.create(namespaceId.getId());
    String namespacePath = namespaceLocation.toURI().getPath();
    if (!namespaceLocation.exists()) {
      throw new IOException(String.format("Namespace home %s not found", namespacePath));
    }
    if (!namespaceLocation.delete(true)) {
      throw new IOException(String.format("Error while deleting namespace directory %s", namespacePath));
    }
  }
}

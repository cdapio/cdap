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
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * {@link AbstractIdleService} for managing namespaces
 */
public final class NamespaceService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(NamespaceService.class);
  private static final String NAMESPACE_ELEMENT_TYPE = "Namespace";

  private volatile Store store;
  private final StoreFactory storeFactory;
  private final PreferencesStore preferencesStore;
  private final DashboardStore dashboardStore;

  @Inject
  public NamespaceService(StoreFactory storeFactory, PreferencesStore preferencesStore, DashboardStore dashboardStore) {
    this.storeFactory = storeFactory;
    this.preferencesStore = preferencesStore;
    this.dashboardStore = dashboardStore;
  }

  /**
   * Initializes and starts namespace service.
   * Currently only creates the default namespace if it does not exist.
   * In future, it could potentially do other tasks as well.
   */
  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting namespace service");
    initializeStore();
    ensureDefaultNamespaceExists();
  }

  /**
   * Asynchronously initializes the #store, with retries.
   * Need a retry mechanism since {@link StoreFactory#create()} requires the dataset service to be running.
   */
  private void initializeStore() {
    RetryThread retryThread = new RetryThread(new Runnable() {
      @Override
      public void run() {
        store = storeFactory.create();
      }
    }, 1, TimeUnit.SECONDS);
    retryThread.start();
  }

  /**
   * Asynchronously ensures that the default namespace exists, with retries.
   * Need a retry mechanism since the ensure operation requires the dataset service to be running.
   * Hence this method ensures that the default namespace exists in a separate thread, with multiple retries
   */
  private void ensureDefaultNamespaceExists() {
    RetryThread retryThread = new RetryThread(new Runnable() {
      @Override
      public void run() {
        createDefaultNamespace();
      }
    }, 1, TimeUnit.SECONDS);
    retryThread.start();
  }

  private static class RetryThread extends Thread {

    private Runnable toRetry;
    private int waitBetweenRetries;
    private TimeUnit waitUnit;

    RetryThread(Runnable runnable, int wait, TimeUnit unit) {
      this.toRetry = runnable;
      this.waitBetweenRetries = wait;
      this.waitUnit = unit;
    }

    @Override
    public void run() {
      int retries = 0;
      while (true) {
        try {
          toRetry.run();
          // if run() does not throw exception, break
          break;
        } catch (Exception e) {
          retries++;
          LOG.warn("Error during retry# {} - {}", retries, e.getMessage());
          try {
            waitUnit.sleep(waitBetweenRetries);
          } catch (InterruptedException e1) {
            LOG.warn("Interrupted during retry# - {}", retries, e1.getMessage());
          }
        }
      }
    }
  }

  /**
   * Creates the default namespace at a more deterministic time.
   * It should be removed once we stop support for v2 APIs, since 'default' namespace is only reserved for v2 APIs.
   */
  private void createDefaultNamespace() {
    // make sure store is initialized. Ok to throw unchecked exception since this is called in a retry thread which
    // retries upon exception
    if (store == null) {
      throw new IllegalStateException("Error: Store is not initialized.");
    }
    NamespaceMeta.Builder builder = new NamespaceMeta.Builder();
    NamespaceMeta defaultNamespace = builder.setId(Constants.DEFAULT_NAMESPACE)
      .setName(Constants.DEFAULT_NAMESPACE)
      .setDescription(Constants.DEFAULT_NAMESPACE)
      .build();

    // Shouldn't use #createNamespace during initialization. That method should only be used after the service
    // is initialized.
    NamespaceMeta meta = store.createNamespace(defaultNamespace);
    if (meta == null) {
      LOG.info("Successfully created 'default' namespace.");
    } else {
      LOG.info("'default' namespace already exists.");
    }
  }

  private boolean isInitialized() {
    return store != null && store.getNamespace(Id.Namespace.from(Constants.DEFAULT_NAMESPACE)) != null;
  }

  private void ensureInitialized() {
    if (!isRunning()) {
      throw new IllegalStateException("Error: Namespace service is not running.");
    }
    if (!isInitialized()) {
      throw new IllegalStateException("Error: Namespace service is not initialized.");
    }
  }

  /**
   * Lists all namespaces
   *
   * @return a list of {@link NamespaceMeta} for all namespaces
   */
  public List<NamespaceMeta> listNamespaces() {
    ensureInitialized();
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
    ensureInitialized();
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
    ensureInitialized();
    try {
      getNamespace(namespaceId);
      return true;
    } catch (NotFoundException e) {
      return false;
    }
  }

  /**
   * Creates a new namespace
   *
   * @param metadata the {@link NamespaceMeta} for the new namespace to be created
   * @throws AlreadyExistsException if the specified namespace already exists
   */
  public void createNamespace(NamespaceMeta metadata) throws AlreadyExistsException {
    ensureInitialized();
    NamespaceMeta existing = store.createNamespace(metadata);
    if (existing != null) {
      throw new AlreadyExistsException(NAMESPACE_ELEMENT_TYPE, metadata.getId());
    }
  }

  /**
   * Deletes the specified namespace
   *
   * @param namespaceId the {@link Id.Namespace} of the specified namespace
   * @throws NotFoundException if the specified namespace does not exist
   */
  public void deleteNamespace(Id.Namespace namespaceId) throws NotFoundException {
    ensureInitialized();

    //TODO: CDAP-870. Delete should be in a single transaction.
    // Delete Preferences associated with this namespace
    preferencesStore.deleteProperties(namespaceId.getId());

    // Delete all dashboards associated with this namespace
    dashboardStore.delete(namespaceId.getId());

    // Store#deleteNamespace already checks for existence
    NamespaceMeta deletedNamespace = store.deleteNamespace(namespaceId);
    if (deletedNamespace == null) {
      throw new NotFoundException(NAMESPACE_ELEMENT_TYPE, namespaceId.getId());
    }
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Shutting down namespace service");
  }
}

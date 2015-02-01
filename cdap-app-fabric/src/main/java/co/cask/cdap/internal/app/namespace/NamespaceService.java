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
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.sun.istack.Nullable;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * {@link AbstractIdleService} for managing namespaces
 */
@Singleton
public final class NamespaceService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(NamespaceService.class);
  private static final String NAMESPACE_ELEMENT_TYPE = "Namespace";

  private final Store store;
  private final PreferencesStore preferencesStore;
  private final DashboardStore dashboardStore;

  @Inject
  public NamespaceService(StoreFactory storeFactory, PreferencesStore preferencesStore, DashboardStore dashboardStore) {
    this.store = storeFactory.create();
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
    ensureDefaultNamespaceExists();
  }

  /**
   * Asynchronously ensures that the default namespace exists, with retries.
   * Need a retry mechanism since the ensure operation requires the dataset service to be running.
   * Hence this method ensures that the default namespace exists in a separate thread, with multiple retries
   */
  private void ensureDefaultNamespaceExists() {
    ListenableFuture<Void> listenableFuture = asyncCreateDefaultNamespace();
    Futures.addCallback(listenableFuture, new FutureCallback<Void>() {
      @Override
      public void onSuccess(Void aVoid) {
        // No-op, since either default namespace already existed, or its creation was successful
        LOG.info("Successfully ensured existence of default namespace.");
      }

      @Override
      public void onFailure(Throwable throwable) {
        LOG.warn("Failed to ensure existence of default namespace.");
        // now keep retrying
        if (isRunning()) {
          final FutureCallback<Void> callback = this;
          // Retry in 2 seconds. Shouldn't sleep in this callback thread. Should start a new thread for the retry.
          Thread retryThread = new Thread("retrying-default-namespace-ensurer") {
            @Override
            public void run() {
              try {
                TimeUnit.SECONDS.sleep(2);
                LOG.info("Retrying to ensure that default namespace exists.");
                Futures.addCallback(asyncCreateDefaultNamespace(), callback);
              } catch (InterruptedException e) {
                LOG.warn("Default namespace ensurer retry thread interrupted.");
              }
            }
          };
          retryThread.setDaemon(true);
          retryThread.start();
        }
      }
    });
  }

  /**
   * Asynchronously calls #createDefaultNamespace to create the default namespace
   */
  private ListenableFuture<Void> asyncCreateDefaultNamespace() {
    ListeningExecutorService executorService = MoreExecutors.listeningDecorator(
      Executors.newSingleThreadExecutor(Threads.createDaemonThreadFactory("default-namespace-ensurer")));
    return executorService.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        createDefaultNamespace();
        return null;
      }
    });
  }
  
  /**
   * Creates the default namespace at a more deterministic time.
   * It should be removed once we stop support for v2 APIs, since 'default' namespace is only reserved for v2 APIs.
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
    }
  }

  /**
   * Lists all namespaces
   *
   * @return a list of {@link NamespaceMeta} for all namespaces
   */
  public List<NamespaceMeta> listNamespaces() {
    startAndWait();
    return store.listNamespaces();
  }

  /**
   * Gets details of a namespace
   *
   * @param namespaceId {@link Id.Namespace} of the requested namespace
   * @return the {@link NamespaceMeta} of the requested namespace
   * @throws NotFoundException if the requested namespace is not found
   */
  public NamespaceMeta getNamespace(Id.Namespace namespaceId) throws NotFoundException {
    startAndWait();
    NamespaceMeta ns = store.getNamespace(namespaceId);
    if (ns == null) {
      throw new NotFoundException(NAMESPACE_ELEMENT_TYPE, namespaceId.getId());
    }
    return ns;
  }

  /**
   * Creates a new namespace
   *
   * @param metadata the {@link NamespaceMeta} for the new namespace to be created
   * @throws AlreadyExistsException if the specified namespace already exists
   */
  public void createNamespace(NamespaceMeta metadata) throws AlreadyExistsException {
    startAndWait();
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
    startAndWait();
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

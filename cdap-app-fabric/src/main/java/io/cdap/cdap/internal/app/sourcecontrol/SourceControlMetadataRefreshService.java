/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.sourcecontrol;

import com.google.common.util.concurrent.AbstractIdleService;
import io.cdap.cdap.api.feature.FeatureFlagsProvider;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.feature.DefaultFeatureFlagsProvider;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.features.Feature;
import io.cdap.cdap.internal.app.services.NamespaceSourceControlMetadataRefreshService;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.sourcecontrol.RepositoryMeta;
import io.cdap.cdap.sourcecontrol.operationrunner.SourceControlOperationRunner;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.TableNotFoundException;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.store.RepositoryTable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class manages a mapping of namespaces with repository configurations to their respective
 * NamespaceSourceControlMetadataRefreshService scheduler instances.
 */
public class SourceControlMetadataRefreshService extends AbstractIdleService {

  private ConcurrentMap<NamespaceId, NamespaceSourceControlMetadataRefreshService>
      refreshSchedulers = new ConcurrentHashMap<>();
  private final TransactionRunner transactionRunner;
  private final NamespaceAdmin namespaceAdmin;
  private final CConfiguration cConf;
  private final SourceControlOperationRunner sourceControlOperationRunner;
  private static final Logger LOG = LoggerFactory.getLogger(
      SourceControlMetadataRefreshService.class);
  private final FeatureFlagsProvider featureFlagsProvider;

  /**
   * Constructs a SourceControlMetadataRefreshService instance.
   */
  @Inject
  public SourceControlMetadataRefreshService(TransactionRunner transactionRunner,
      NamespaceAdmin namespaceAdmin, CConfiguration cConf,
      SourceControlOperationRunner sourceControlOperationRunner,
      Store store) {
    this.transactionRunner = transactionRunner;
    this.namespaceAdmin = namespaceAdmin;
    this.cConf = cConf;
    this.sourceControlOperationRunner = sourceControlOperationRunner;
    this.featureFlagsProvider = new DefaultFeatureFlagsProvider(cConf);
  }

  @Override
  protected void startUp() throws Exception {
    if (!isSourceControlMetadataPeriodicRefreshFlagEnabled()) {
      return;
    }
    LOG.info("Starting SourceControlMetadataRefreshService");

    namespaceAdmin.list().stream()
        .map(NamespaceMeta::getNamespaceId)
        .filter(namespaceId -> getRepositoryMeta(namespaceId) != null)
        .forEach(this::addRefreshService);
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Shutting down SourceControlMetadataRefreshService");
    refreshSchedulers.values().forEach(NamespaceSourceControlMetadataRefreshService::stopAndWait);
  }

  private NamespaceSourceControlMetadataRefreshService createRefreshService(NamespaceId namespace) {
    return new NamespaceSourceControlMetadataRefreshService(cConf, transactionRunner,
        sourceControlOperationRunner, namespace, namespaceAdmin);
  }

  /**
   * Adds {@link NamespaceSourceControlMetadataRefreshService} for the specified namespace. If not
   * created before, it starts the service or else, triggers a forced manual refresh
   *
   * @param namespaceId The ID of the namespace for which to add the refresh service.
   */
  public void addRefreshService(NamespaceId namespaceId) {
    refreshSchedulers.putIfAbsent(namespaceId, createRefreshService(namespaceId));
    if (isSourceControlMetadataPeriodicRefreshFlagEnabled()) {
      refreshSchedulers.get(namespaceId).start();
      LOG.debug("Added Scheduled NamespaceSourceControlMetadataRefreshService for "
          + namespaceId.getNamespace());
    }
  }

  /**
   * Stops and removes the refresh service associated with the specified namespace.
   *
   * @param namespaceId The ID of the namespace for which to remove the refresh service.
   */
  public void removeRefreshService(NamespaceId namespaceId) {
    refreshSchedulers.computeIfPresent(namespaceId, (id, service) -> {
      service.stop();
      return null;
    });
  }

  /**
   * Retrieves the timestamp of the last refresh for the specified namespace.
   *
   * @param namespaceId The ID of the namespace for which to retrieve the last refresh time.
   * @return The timestamp of the last refresh
   */
  public Long getLastRefreshTime(NamespaceId namespaceId) {
    NamespaceSourceControlMetadataRefreshService service = refreshSchedulers.get(namespaceId);
    if (service != null) {
      return service.getLastRefreshTime();
    }
    return null;
  }

  /**
   * Initiates a manual refresh for the specified namespace. If a refresh service exists for the
   * namespace, triggers an unforced manual refresh. If no refresh service exists, adds a new
   * refresh service and starts it. This is triggered when the user calls the list API for namespace
   * and repository source control metadata.
   *
   * @param namespaceId The ID of the namespace for which to run the refresh service.
   */
  public void runRefreshService(NamespaceId namespaceId) {
    NamespaceSourceControlMetadataRefreshService service = refreshSchedulers.get(namespaceId);
    if (service != null) {
      refreshSchedulers.get(namespaceId).triggerManualRefresh(false);
    } else {
      addRefreshService(namespaceId);
    }
  }

  private RepositoryMeta getRepositoryMeta(NamespaceId namespace) {
    return TransactionRunners.run(transactionRunner, context -> {
      RepositoryTable table = getRepositoryTable(context);
      RepositoryMeta repoMeta = table.get(namespace);
      return repoMeta;
    });
  }

  private RepositoryTable getRepositoryTable(StructuredTableContext context)
      throws TableNotFoundException {
    return new RepositoryTable(context);
  }

  private boolean isSourceControlMetadataPeriodicRefreshFlagEnabled() {
    return Feature.SOURCE_CONTROL_METADATA_PERIODIC_REFRESH.isEnabled(featureFlagsProvider);
  }

}

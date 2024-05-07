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

package io.cdap.cdap.internal.app.services;

import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.app.store.ScanSourceControlMetadataRequest;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.RepositoryNotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants.AppFabric;
import io.cdap.cdap.common.conf.Constants.SourceControlManagement;
import io.cdap.cdap.internal.app.store.NamespaceSourceControlMetadataStore;
import io.cdap.cdap.internal.app.store.RepositorySourceControlMetadataStore;
import io.cdap.cdap.proto.SourceControlMetadataRecord;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.ApplicationReference;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.sourcecontrol.RemoteRepositoryValidationException;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;
import io.cdap.cdap.proto.sourcecontrol.RepositoryMeta;
import io.cdap.cdap.proto.sourcecontrol.SourceControlMeta;
import io.cdap.cdap.sourcecontrol.operationrunner.NamespaceRepository;
import io.cdap.cdap.sourcecontrol.operationrunner.RepositoryApp;
import io.cdap.cdap.sourcecontrol.operationrunner.RepositoryAppsResponse;
import io.cdap.cdap.sourcecontrol.operationrunner.SourceControlOperationRunner;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.TableNotFoundException;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.store.RepositoryTable;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is for refreshing namespace and repository source control metadata of a namespace.
 */
public class NamespaceSourceControlMetadataRefresher {

  private static final Logger LOG = LoggerFactory.getLogger(
      NamespaceSourceControlMetadataRefresher.class);

  private final TransactionRunner transactionRunner;
  private final SourceControlOperationRunner sourceControlOperationRunner;
  private final NamespaceId namespaceId;
  private final AtomicLong lastRefreshTime;

  /**
   * Constructs a {@link NamespaceSourceControlMetadataRefresher} instance.
   */
  public NamespaceSourceControlMetadataRefresher(TransactionRunner transactionRunner,
      SourceControlOperationRunner sourceControlOperationRunner,
      NamespaceId namespaceId) {
    this.transactionRunner = transactionRunner;
    this.sourceControlOperationRunner = sourceControlOperationRunner;
    this.namespaceId = namespaceId;
    this.lastRefreshTime = new AtomicLong(0);
  }

  private RepositorySourceControlMetadataStore getRepoSourceControlMetadataStore(
      StructuredTableContext context) {
    return RepositorySourceControlMetadataStore.create(context);
  }

  private NamespaceSourceControlMetadataStore getNamespaceSourceControlMetadataStore(
      StructuredTableContext context) {
    return NamespaceSourceControlMetadataStore.create(context);
  }

  private RepositoryTable getRepositoryTable(StructuredTableContext context)
      throws TableNotFoundException {
    return new RepositoryTable(context);
  }


  // TODO(CDAP-21020): Refactor this function
  private RepositoryMeta getRepositoryMeta(NamespaceId namespace)
      throws RepositoryNotFoundException {
    return TransactionRunners.run(transactionRunner, context -> {
      RepositoryTable table = getRepositoryTable(context);
      RepositoryMeta repoMeta = table.get(namespace);
      if (repoMeta == null) {
        throw new RepositoryNotFoundException(namespace);
      }

      return repoMeta;
    }, RepositoryNotFoundException.class);
  }

  public long refreshMetadata() throws NotFoundException {
    long refreshStartTime = System.currentTimeMillis();

    // Getting repository config for a specific namespace
    RepositoryConfig repoConfig = getRepositoryMeta(namespaceId).getConfig();
    // Listing the apps from remote repo
    RepositoryAppsResponse repositoryAppsResponse = sourceControlOperationRunner.list(
        new NamespaceRepository(namespaceId, repoConfig));

    Set<String> repoFileNames = repositoryAppsResponse.getApps().stream()
        .map(RepositoryApp::getName)
        .collect(Collectors.toCollection(HashSet::new));

    TransactionRunners.run(transactionRunner, context -> {
      RepositorySourceControlMetadataStore repoMetadataStore = getRepoSourceControlMetadataStore(
          context);

      // Cleaning up the repo source control metadata table
      cleanupRepoSourceControlMeta(namespaceId.getNamespace(),
          repoFileNames, repoMetadataStore);

      // Updating the namespace and repo source control metadata table
      NamespaceSourceControlMetadataStore namespaceMetadataStore =
          getNamespaceSourceControlMetadataStore(context);
      for (RepositoryApp repoApp : repositoryAppsResponse.getApps()) {
        updateSourceControlMeta(
            new ApplicationReference(namespaceId.getNamespace(), repoApp.getName()),
            repoApp.getFileHash(),
            refreshStartTime, repoMetadataStore, namespaceMetadataStore);
      }
    });
    lastRefreshTime.set(refreshStartTime);
    return this.lastRefreshTime.get();
  }

  private void updateSourceControlMeta(ApplicationReference appRef,
      String repoFileHash,
      long refreshStartTime, RepositorySourceControlMetadataStore repoMetadataStore,
      NamespaceSourceControlMetadataStore namespaceMetadataStore) throws IOException {

    SourceControlMeta namespaceSourceControlMeta = namespaceMetadataStore.get(appRef);

    if (namespaceSourceControlMeta == null) {
      repoMetadataStore.write(appRef, false, 0L);
      return;
    }

    if (namespaceSourceControlMeta.getLastSyncedAt() != null
        && namespaceSourceControlMeta.getLastSyncedAt().isAfter(
        Instant.ofEpochMilli(refreshStartTime))) {
      repoMetadataStore.write(appRef,
          Boolean.TRUE.equals(namespaceSourceControlMeta.getSyncStatus()),
          namespaceSourceControlMeta.getLastSyncedAt().toEpochMilli());
      return;
    }
    boolean isSynced = repoFileHash.equals(namespaceSourceControlMeta.getFileHash());
    namespaceMetadataStore.write(appRef,
        SourceControlMeta.builder(namespaceSourceControlMeta).setSyncStatus(isSynced).build());
    repoMetadataStore.write(appRef,
        isSynced, namespaceSourceControlMeta.getLastSyncedAt() == null ? 0L :
            namespaceSourceControlMeta.getLastSyncedAt().toEpochMilli());
  }

  private void cleanupRepoSourceControlMeta(String namespace,
      Set<String> repoFiles,
      RepositorySourceControlMetadataStore store) throws IOException {
    ScanSourceControlMetadataRequest request = ScanSourceControlMetadataRequest
        .builder()
        .setNamespace(namespace)
        .setLimit(Integer.MAX_VALUE)
        .build();
    List<SourceControlMetadataRecord> records = new ArrayList<>();
    String type = EntityType.APPLICATION.toString();

    store.scan(request, type, records::add);

    for (SourceControlMetadataRecord record : records) {
      if (!repoFiles.contains(record.getName())) {
        store.delete(new ApplicationReference(record.getNamespace(), record.getName()));
      }
    }
  }
}

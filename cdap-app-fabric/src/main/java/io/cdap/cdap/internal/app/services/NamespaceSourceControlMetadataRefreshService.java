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

import com.google.common.util.concurrent.AbstractScheduledService;
import io.cdap.cdap.app.store.ScanSourceControlMetadataRequest;
import io.cdap.cdap.common.RepositoryNotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants.AppFabric;
import io.cdap.cdap.common.conf.Constants.SourceControlManagement;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.internal.app.store.NamespaceSourceControlMetadataStore;
import io.cdap.cdap.internal.app.store.RepositorySourceControlMetadataStore;
import io.cdap.cdap.proto.SourceControlMetadataRecord;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.ApplicationReference;
import io.cdap.cdap.proto.id.NamespaceId;
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
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service for refreshing namespace and repository source control metadata of a namespace at
 * scheduled intervals and manually.
 */
public class NamespaceSourceControlMetadataRefreshService extends AbstractScheduledService {

  private static final Logger LOG = LoggerFactory.getLogger(
      NamespaceSourceControlMetadataRefreshService.class);
  private final long runInterval;
  private final long bufferTime;
  private final TransactionRunner transactionRunner;
  private final SourceControlOperationRunner sourceControlOperationRunner;
  private final NamespaceId namespaceId;
  private final NamespaceAdmin namespaceAdmin;
  private final AtomicLong lastRefreshTime;
  private final AtomicBoolean running;
  private static final Integer MAX_JITTER_SECONDS = 10;
  private ScheduledExecutorService executor;
  private final int batchSize;


  /**
   * Constructs a {@link NamespaceSourceControlMetadataRefreshService} instance.
   */
  @Inject
  public NamespaceSourceControlMetadataRefreshService(CConfiguration cConf,
      TransactionRunner transactionRunner,
      SourceControlOperationRunner sourceControlOperationRunner,
      NamespaceId namespaceId, NamespaceAdmin namespaceAdmin) {
    this.transactionRunner = transactionRunner;
    this.sourceControlOperationRunner = sourceControlOperationRunner;
    this.bufferTime = cConf.getLong(SourceControlManagement.METADATA_REFRESH_BUFFER_SECONDS);
    this.namespaceId = namespaceId;
    this.namespaceAdmin = namespaceAdmin;
    this.lastRefreshTime = new AtomicLong(0);
    this.running = new AtomicBoolean(false);
    this.batchSize = cConf.getInt(AppFabric.STREAMING_BATCH_SIZE);

    // Adding a jitter (a random value between 0 and 10) to the runInterval because if services for all
    // the namespaces ran at the same time, it may create availability issues in task workers.
    long interval =
        cConf.getLong(SourceControlManagement.METADATA_REFRESH_INTERVAL_SECONDS);
    if (interval > 0) {
      Random random = new Random();
      this.runInterval = interval + random.nextInt(
          MAX_JITTER_SECONDS + 1);
    } else {
      this.runInterval = interval;
    }
  }

  private RepositorySourceControlMetadataStore getRepoSourceControlMetadataStore(
      StructuredTableContext context) {
    return RepositorySourceControlMetadataStore.create(context);
  }

  private NamespaceSourceControlMetadataStore getNamespaceSourceControlMetadataStore(
      StructuredTableContext context) {
    return NamespaceSourceControlMetadataStore.create(context);
  }

  @Override
  protected final ScheduledExecutorService executor() {
    executor = Executors.newSingleThreadScheduledExecutor(
        Threads.createDaemonThreadFactory("source-control-metadata-refresh-service-for-namespace-"
            + namespaceId.getNamespace()));
    return executor;
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

  // TODO(CDAP-21017): Optimize periodic refresh of source control metadata
  @Override
  public void runOneIteration() {
    if (!running.compareAndSet(false, true)) {
      return;
    }
    runRefreshService(false);
  }

  /**
   * Triggers a manual refresh of source control metadata for the associated namespace.
   *
   * @param forced If true, forces the refresh by skipping the check whether the time elapsed since
   *               the last refresh is within the buffer time.
   */
  public void triggerManualRefresh(boolean forced) {
    if (!running.compareAndSet(false, true)) {
      return;
    }
    executor().execute(() -> runRefreshService(forced));
  }

  private void runRefreshService(boolean forced) {
    running.set(true);
    Long refreshStartTime = System.currentTimeMillis();

    try {

      if (!namespaceAdmin.exists(namespaceId)) {
        this.stop();
      }

      // Check if the time elapsed since the last refresh is within the buffer time.
      // This is done  to prevent a high number of refresh jobs from being queued up
      // unnecessarily.
      if (!forced && System.currentTimeMillis() - lastRefreshTime.get() < bufferTime * 1000) {
        return;
      }

      // Getting repository config for a specific namespace
      RepositoryConfig repoConfig = getRepositoryMeta(namespaceId).getConfig();

      // Listing the apps from remote repo
      RepositoryAppsResponse repositoryAppsResponse = sourceControlOperationRunner.list(
          new NamespaceRepository(namespaceId, repoConfig));

      // Cleaning up the repo source control metadata table
      HashSet<String> repoFileNames = repositoryAppsResponse.getApps().stream()
          .map(RepositoryApp::getName)
          .collect(Collectors.toCollection(HashSet::new));

      cleanupRepoSourceControlMetaInBatches(namespaceId.getNamespace(), repoFileNames);

      // Updating the namespace and repo source control metadata table
      for (RepositoryApp repoApp : repositoryAppsResponse.getApps()) {
        updateSourceControlMeta(
            new ApplicationReference(namespaceId.getNamespace(), repoApp.getName()),
            repoApp.getFileHash(),
            refreshStartTime);
      }

      lastRefreshTime.set(refreshStartTime);

    } catch (Exception e) {
      LOG.error("Failed to refresh source control metadata for namespace "
          + namespaceId.getNamespace(), e);
    } finally {
      running.set(false);
    }
  }

  public Long getLastRefreshTime() {
    return this.lastRefreshTime.get();
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting SourceControlMetadataRefreshService for namespace "
        + namespaceId.getNamespace() + " with interval " + runInterval + " seconds");
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(1, runInterval, TimeUnit.SECONDS);
  }

  @Override
  protected void shutDown() throws Exception {
    if (executor != null) {
      executor.shutdownNow();
    }
  }

  private void updateSourceControlMeta(ApplicationReference appRef, String repoFileHash,
      Long refreshStartTime) {
    TransactionRunners.run(transactionRunner, context -> {
      RepositorySourceControlMetadataStore repoMetadataStore = getRepoSourceControlMetadataStore(
          context);
      NamespaceSourceControlMetadataStore namespaceMetadataStore =
          getNamespaceSourceControlMetadataStore(context);
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
    });
  }

  private void cleanupRepoSourceControlMetaInBatches(String namespace, HashSet<String> repoFiles) {
    TransactionRunners.run(transactionRunner, context -> {
      RepositorySourceControlMetadataStore store = getRepoSourceControlMetadataStore(context);
      ScanSourceControlMetadataRequest request = ScanSourceControlMetadataRequest
          .builder()
          .setNamespace(namespace)
          .setLimit(Integer.MAX_VALUE)
          .build();
      ArrayList<SourceControlMetadataRecord> records = new ArrayList<>();
      String type = EntityType.APPLICATION.toString();

      store.scan(request, type, record -> {
        records.add(record);
        if (records.size() == batchSize) {
          try {
            cleanUpRecords(store, repoFiles, records);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          records.clear();
        }
      });

      if (!records.isEmpty()) {
        cleanUpRecords(store, repoFiles, records);
      }
    });
  }


  private void cleanUpRecords(RepositorySourceControlMetadataStore store, HashSet<String> repoFiles,
      List<SourceControlMetadataRecord> records)
      throws IOException {
    for (SourceControlMetadataRecord record : records) {
      if (!repoFiles.contains(record.getName())) {
        store.delete(new ApplicationReference(record.getNamespace(), record.getName()));
      }
    }
  }

}

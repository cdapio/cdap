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

import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.inject.Inject;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.app.store.AppMetadataStore;
import io.cdap.cdap.internal.app.store.NamespaceSourceControlMetadataStore;
import io.cdap.cdap.proto.id.ApplicationReference;
import io.cdap.cdap.proto.sourcecontrol.SourceControlMeta;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service for migrating source control metadata from app_spec table to source control table.
 * Starting from 6.10.1 we have introduced a separate table for storing scm metadata. As this
 * information was previously stored in app_spec table we need to run this migration
 */
public class SourceControlMetadataMigrationService extends AbstractScheduledService {

  private ScheduledExecutorService executor;
  private final long runInterval;
  private final TransactionRunner transactionRunner;
  private final Store store;

  private static final Logger LOG = LoggerFactory.getLogger(SourceControlMetadataMigrationService.class);

  @Inject
  SourceControlMetadataMigrationService(CConfiguration cConf, Store store,
      TransactionRunner transactionRunner) {
    this.runInterval = cConf.getLong(
        Constants.SourceControlManagement.METADATA_MIGRATION_INTERVAL_SECONDS);
    this.transactionRunner = transactionRunner;
    this.store = store;
  }

  @Override
  protected final ScheduledExecutorService executor() {
    executor = Executors.newSingleThreadScheduledExecutor(
        Threads.createDaemonThreadFactory("scm-metadata-migration"));
    return executor;
  }

  @Override
  protected void runOneIteration() {
    try {
      migrateMetadata();
      // stop if the migration is successfull otherwise retry after delay
      this.stop();
    } catch (Exception e) {
      // Log and retry on exception
      LOG.error("Failed to migrate source control metadata", e);
    }
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedDelaySchedule(0, runInterval, TimeUnit.SECONDS);
  }

  @Override
  protected void shutDown() throws Exception {
    if (executor != null) {
      executor.shutdownNow();
    }
  }

  private void migrateMetadata() {
    LOG.info("Starting source control metadata migration at {}", Instant.now());
    // Get all current metadata from app_spec table.
    // This is to avoid running recursive transaction while trying to update scm table
    Map<ApplicationReference, SourceControlMeta> metaMap = new HashMap<>();
    TransactionRunners.run(transactionRunner, context -> {
      metaMap.putAll(AppMetadataStore.create(context).getAllSourceControlMeta());
    });

    // For each metadata Update source control table with the metadata
    // only if there is no current metadata in source control table
    metaMap.forEach((appRef, scmMeta) -> {
      TransactionRunners.run(transactionRunner, context -> {
        NamespaceSourceControlMetadataStore metadataStore = NamespaceSourceControlMetadataStore.create(
            context);
        SourceControlMeta currentMeta = metadataStore.get(appRef);
        if (currentMeta == null) {
          LOG.debug("Migrating scm metadata {} for application {}", appRef, scmMeta);
          if (scmMeta == null) {
            // If the app_spec_table entry does not have any scm meta that means the app
            // was never pushed/pulled
            metadataStore.write(appRef, SourceControlMeta.createDefaultMeta());
          } else {
            store.setAppSourceControlMeta(appRef, scmMeta);
          }
        }
      });
    });
  }
}

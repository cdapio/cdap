/*
/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.metadata;

import co.cask.cdap.common.service.Retries;
import co.cask.cdap.common.service.RetryStrategies;
import co.cask.cdap.common.service.RetryStrategy;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.metadata.store.MetadataStore;
import co.cask.cdap.proto.metadata.MetadataScope;
import com.google.inject.Inject;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Runs when Metadata Service starts to execute Metatdata Spec Update and Data Migration
 */
public class MetadataUpgrader {
  private static final Logger LOG = LoggerFactory.getLogger(MetadataUpgrader.class);
  private static final int RETRY_DELAY = 2; // seconds
  private static final RetryStrategy retryStrategy = RetryStrategies.fixDelay(RETRY_DELAY, TimeUnit.SECONDS);

  private final ExistingEntitySystemMetadataWriter existingEntitySystemMetadataWriter;
  private final MetadataStore metadataStore;
  private final DatasetFramework datasetFramework;

  @Inject
  MetadataUpgrader(ExistingEntitySystemMetadataWriter existingEntitySystemMetadataWriter,
                   MetadataStore metadataStore, DatasetFramework datasetFramework) {
    this.existingEntitySystemMetadataWriter = existingEntitySystemMetadataWriter;
    this.metadataStore = metadataStore;
    this.datasetFramework = datasetFramework;
  }

  public void createOrUpgradeIfNecessary() {
    // Upgrade the Specs inline
    upgradeMetadataDatasetSpecs(MetadataScope.SYSTEM);
    upgradeMetadataDatasetSpecs(MetadataScope.USER);

    // Create a single thread executor using daemon threads which runs data-migration process.
    ExecutorService executor =
      Executors.newSingleThreadExecutor(Threads.createDaemonThreadFactory("metadata-upgrader"));
    executor.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        runSystemUpgrade();
        runUserUpgrade();
        return null;
      }
    });
  }

  private boolean isUpgradeRequired(final MetadataScope scope) throws Exception {
    boolean upgradeRequired;
    try {
      upgradeRequired = Retries.callWithRetries(new Retries.Callable<Boolean, Exception>() {
        @Override
        public Boolean call() throws Exception {
          return metadataStore.isUpgradeRequired(scope);
        }
      }, retryStrategy);
    } catch (Exception e) {
      LOG.error("Failed to check Metadata Dataset Upgrade status for scope {}.", scope, e);
      throw e;
    }
    return upgradeRequired;
  }

  private void saveUpgradeState(final MetadataScope scope) throws Exception {
    LOG.info("Save Metadata Dataset Upgrade status for scope {}", scope);
    try {
      Retries.callWithRetries(new Retries.Callable<Void, Exception>() {
        @Override
        public Void call() throws Exception {
          metadataStore.markUpgradeComplete(scope);
          return null;
        }
      }, retryStrategy);
    } catch (Exception e) {
      LOG.error("Failed to save upgrade state for scope {}.", scope, e);
      throw e;
    }
  }

  private void runSystemUpgrade() throws Exception {
    if (!isUpgradeRequired(MetadataScope.SYSTEM)) {
      LOG.info("{} Metadata Dataset Upgrade is not required.", MetadataScope.SYSTEM);
      return;
    }

    LOG.info("Writing system metadata for existing entities.");
    try {
      Retries.callWithRetries(new Retries.Callable<Void, Exception>() {
        @Override
        public Void call() throws Exception {
          existingEntitySystemMetadataWriter.write(datasetFramework);
          return null;
        }
      }, retryStrategy);
    } catch (Exception e) {
      LOG.error("Failed write Existing System Metadata.", e);
      throw e;
    }

    saveUpgradeState(MetadataScope.SYSTEM);
  }

  private void runUserUpgrade() throws Exception {
    if (!isUpgradeRequired(MetadataScope.USER)) {
      LOG.info("{} Metadata Dataset Upgrade is not required.", MetadataScope.USER);
      return;
    }

    LOG.info("Re-building metadata indexes for scope {}.", MetadataScope.USER);
    metadataStore.rebuildIndexes(MetadataScope.USER, retryStrategy); // Retry per batch

    saveUpgradeState(MetadataScope.USER);
  }

  private void upgradeMetadataDatasetSpecs(final MetadataScope scope) {
    LOG.debug("Upgrading the Metadata Dataset Specs now");
    try {
      Retries.callWithRetries(new Retries.Callable<Void, Exception>() {
        @Override
        public Void call() throws Exception {
          metadataStore.createOrUpgrade(scope);
          return null;
        }
      }, retryStrategy);
    } catch (Exception e) {
      LOG.error("Metadata Dataset createOrUpgrade Failed for scope {}", scope, e);
      throw new RuntimeException(e);
    }
  }
}

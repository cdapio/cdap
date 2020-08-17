/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.preview;

import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.inject.Inject;
import io.cdap.cdap.app.store.preview.PreviewStore;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import org.apache.twill.common.Threads;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The clean up service that cleans up preview data periodically.
 */
public class PreviewDataCleanupService extends AbstractScheduledService {
  private final PreviewStore previewStore;
  private final long cleanUpInterval;
  private final long ttl;
  private ScheduledExecutorService executor;

  @Inject
  PreviewDataCleanupService(CConfiguration cConf, PreviewStore previewStore) {
    this.previewStore = previewStore;
    this.cleanUpInterval = cConf.getLong(Constants.Preview.DATA_CLEANUP_INTERVAL_SECONDS);
    this.ttl = cConf.getLong(Constants.Preview.DATA_TTL_SECONDS);
  }

  @Override
  protected final ScheduledExecutorService executor() {
    executor = Executors.newSingleThreadScheduledExecutor(Threads.createDaemonThreadFactory("preview-cleanup"));
    return executor;
  }

  @Override
  protected void runOneIteration() {
    previewStore.deleteExpiredData(ttl);
  }

  @Override
  protected Scheduler scheduler() {
    // Try right away if there's anything to cleanup, we will then schedule based on the minimum retention interval
    return Scheduler.newFixedRateSchedule(1, cleanUpInterval, TimeUnit.SECONDS);
  }

  @Override
  protected void shutDown() throws Exception {
    if (executor != null) {
      executor.shutdownNow();
    }
  }
}

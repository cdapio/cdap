/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.metrics.store;

import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.inject.Inject;
import io.cdap.cdap.api.metrics.MetricStore;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import org.apache.twill.common.Threads;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The metrics clean up service that will clean up resolution metrics table periodically based on their retention time.
 */
public class MetricsCleanUpService extends AbstractScheduledService {
  private final MetricStore metricStore;
  private final long cleanUpInterval;
  private ScheduledExecutorService executor;

  @Inject
  MetricsCleanUpService(MetricStore metricStore, CConfiguration cConf) {
    this.metricStore = metricStore;
    this.cleanUpInterval = cConf.getLong(Constants.Metrics.MINIMUM_RESOLUTION_RETENTION_SECONDS);
  }

  @Override
  protected final ScheduledExecutorService executor() {
    executor = Executors.newSingleThreadScheduledExecutor(Threads.createDaemonThreadFactory("metrics-cleanup"));
    return executor;
  }

  @Override
  protected void runOneIteration() {
    // delete metrics from resolution table
    metricStore.deleteTTLExpired();
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

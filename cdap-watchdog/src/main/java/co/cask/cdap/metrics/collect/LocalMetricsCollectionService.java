/*
 * Copyright © 2014 Cask Data, Inc.
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
package co.cask.cdap.metrics.collect;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.metrics.MetricsConstants;
import co.cask.cdap.metrics.store.MetricStore;
import co.cask.cdap.metrics.transport.MetricValue;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A {@link co.cask.cdap.common.metrics.MetricsCollectionService} that writes to MetricsTable directly.
 * It also has a scheduling job that clean up old metrics periodically.
 */
@Singleton
public final class LocalMetricsCollectionService extends AggregatedMetricsCollectionService {

  private static final Logger LOG = LoggerFactory.getLogger(LocalMetricsCollectionService.class);

  private final CConfiguration cConf;
  private final MetricStore metricStore;
  private ScheduledExecutorService scheduler;

  @Inject
  public LocalMetricsCollectionService(CConfiguration cConf, MetricStore metricStore) {
    this.cConf = cConf;
    this.metricStore = metricStore;
  }

  @Override
  protected void publish(Iterator<MetricValue> metrics) throws Exception {
    List<MetricValue> records = ImmutableList.copyOf(metrics);

    for (MetricValue value : records) {
      // todo: change method signature to allow adding multiple at once?
      try {
        metricStore.add(value);
      } catch (Exception e) {
        throw new RuntimeException("Failed to add metric data to a store", e);
      }
    }
  }

  @Override
  protected void startUp() throws Exception {
    super.startUp();

    // It will only do cleanup if the underlying table doesn't supports TTL.
    scheduler = Executors.newSingleThreadScheduledExecutor(Threads.createDaemonThreadFactory("metrics-cleanup"));
    long retention = cConf.getLong(MetricsConstants.ConfigKeys.RETENTION_SECONDS + ".1.seconds",
                                   MetricsConstants.DEFAULT_RETENTION_HOURS);

    // Try right away if there's anything to cleanup, then we'll schedule to do that periodically
    scheduler.schedule(createCleanupTask(retention), 1, TimeUnit.SECONDS);
  }

  @Override
  protected void shutDown() throws Exception {
    if (scheduler != null) {
      scheduler.shutdownNow();
    }
    super.shutDown();
  }

  /**
   * Creates a task for cleanup.
   * @param retention Retention in seconds.
   */
  private Runnable createCleanupTask(final long retention) {
    return new Runnable() {
      @Override
      public void run() {
        // Only do cleanup if the underlying table doesn't supports TTL.
        long currentTime = TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        long deleteBefore = currentTime - retention;

        metricStore.deleteBefore(deleteBefore);
        scheduler.schedule(this, 1, TimeUnit.HOURS);
      }
    };
  }
}

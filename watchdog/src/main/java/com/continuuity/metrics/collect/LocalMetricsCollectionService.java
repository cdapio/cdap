/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.collect;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.metrics.MetricsScope;
import com.continuuity.data2.OperationException;
import com.continuuity.metrics.MetricsConstants;
import com.continuuity.metrics.data.MetricsTableFactory;
import com.continuuity.metrics.data.TimeSeriesTable;
import com.continuuity.metrics.process.MetricsProcessor;
import com.continuuity.metrics.transport.MetricsRecord;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A {@link com.continuuity.common.metrics.MetricsCollectionService} that writes to MetricsTable directly.
 * It also has a scheduling job that clean up old metrics periodically.
 */
@Singleton
public final class LocalMetricsCollectionService extends AggregatedMetricsCollectionService {

  private static final Logger LOG = LoggerFactory.getLogger(LocalMetricsCollectionService.class);

  private final CConfiguration cConf;
  private final Set<MetricsProcessor> processors;
  private final MetricsTableFactory tableFactory;
  private ScheduledExecutorService scheduler;

  @Inject
  public LocalMetricsCollectionService(CConfiguration cConf, MetricsTableFactory tableFactory,
                                       Set<MetricsProcessor> processors) {
    this.cConf = cConf;
    this.processors = processors;
    this.tableFactory = tableFactory;
  }

  @Override
  protected void publish(MetricsScope scope, Iterator<MetricsRecord> metrics) throws Exception {
    List<MetricsRecord> records = ImmutableList.copyOf(metrics);
    for (MetricsProcessor processor : processors) {
      processor.process(scope, records.iterator());
    }
  }

  @Override
  protected void startUp() throws Exception {
    super.startUp();

    // Only do cleanup if the underlying table doesn't supports TTL.
    if (!tableFactory.isTTLSupported()) {
      scheduler = Executors.newSingleThreadScheduledExecutor(Threads.createDaemonThreadFactory("metrics-cleanup"));
      long retention = cConf.getLong(MetricsConstants.ConfigKeys.RETENTION_SECONDS + ".1.seconds",
                                     MetricsConstants.DEFAULT_RETENTION_HOURS);


      scheduler.scheduleAtFixedRate(createCleanupTask(retention), 0, 1, TimeUnit.HOURS);
    }
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
        long currentTime = TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        long deleteBefore = currentTime - retention;

        for (MetricsScope scope : MetricsScope.values()) {
          TimeSeriesTable timeSeriesTable = tableFactory.createTimeSeries(scope.name(), 1);
          try {
            timeSeriesTable.deleteBefore(deleteBefore);
          } catch (OperationException e) {
            LOG.error("Failed in cleaning up metrics table: {}", e.getMessage(), e);
          }
        }
      }
    };
  }
}

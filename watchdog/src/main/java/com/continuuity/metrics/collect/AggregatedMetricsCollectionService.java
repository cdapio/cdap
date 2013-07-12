/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.collect;

import com.continuuity.metrics.transport.MetricsRecord;
import com.continuuity.weave.common.Threads;
import com.google.common.base.Objects;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.AbstractIterator;
import com.google.common.util.concurrent.AbstractScheduledService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Base class for {@link MetricsCollectionService} which collect metrics through a set of cached
 * {@link AggregatedMetricsEmitter}.
 */
public abstract class AggregatedMetricsCollectionService extends AbstractScheduledService
                                                         implements MetricsCollectionService {

  private static final Logger LOG = LoggerFactory.getLogger(AggregatedMetricsCollectionService.class);
  private static final long CACHE_EXPIRE_MINUTES = 1;
  private static final long DEFAULT_FREQUENCY_SECONDS = 1;

  private final LoadingCache<MetricKey, AggregatedMetricsEmitter> collectors;

  public AggregatedMetricsCollectionService() {
    this.collectors = CacheBuilder.newBuilder()
      .expireAfterAccess(CACHE_EXPIRE_MINUTES, TimeUnit.MINUTES)
      .build(new CacheLoader<MetricKey, AggregatedMetricsEmitter>() {
        @Override
        public AggregatedMetricsEmitter load(MetricKey key) throws Exception {
          return new AggregatedMetricsEmitter(key.getContext(), key.getRunId(), key.getMetric());
        }
      });
  }

  /**
   * Publishes the given collection of {@link com.continuuity.metrics.transport.MetricsRecord}. When this method returns, the
   * given {@link Iterator} will no longer be valid. This method should process the input
   * iterator and returns quickly. Any long operations should be run in a separated thread.
   * This method is guaranteed not to get concurrent calls.
   *
   * @param metrics collection of {@link com.continuuity.metrics.transport.MetricsRecord} to publish.
   * @throws Exception if there is error raised during publish.
   */
  protected abstract void publish(Iterator<MetricsRecord> metrics) throws Exception;

  @Override
  protected final void runOneIteration() throws Exception {
    final long timestamp = TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    LOG.trace("Start log collection for timestamp {}", timestamp);
    Iterator<MetricsRecord> metricsItor = getMetrics(timestamp);

    try {
      publish(metricsItor);
    } catch (Throwable t) {
      LOG.error("Failed in publishing metrics for timestamp {}.", timestamp, t);
    }

    // Consume the whole iterator if it is not yet consumed inside publish. This is to make sure metrics are reset.
    while (metricsItor.hasNext()) {
      metricsItor.next();
    }
    LOG.trace("Completed log collection for timestamp {}", timestamp);
  }

  @Override
  protected ScheduledExecutorService executor() {
    return Executors.newSingleThreadScheduledExecutor(Threads.createDaemonThreadFactory("metrics-collection"));
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(DEFAULT_FREQUENCY_SECONDS, DEFAULT_FREQUENCY_SECONDS, TimeUnit.SECONDS);
  }

  @Override
  public final MetricsCollector getCollector(String context, String runId, String metricName) {
    return collectors.getUnchecked(new MetricKey(context, runId, metricName));
  }

  private Iterator<MetricsRecord> getMetrics(final long timestamp) {
    final Iterator<? extends MetricsEmitter> iterator = collectors.asMap().values().iterator();
    return new AbstractIterator<MetricsRecord>() {
      @Override
      protected MetricsRecord computeNext() {
        while (iterator.hasNext()) {
          MetricsRecord metricsRecord = iterator.next().emit(timestamp);
          if (metricsRecord.getValue() != 0) {
            LOG.trace("Emit metric {}", metricsRecord);
            return metricsRecord;
          }
        }
        return endOfData();
      }
    };
  }

  /**
   * Inner class for the cache key for looking up {@link AggregatedMetricsEmitter}.
   */
  private static final class MetricKey {
    private final String context;
    private final String runId;
    private final String metric;

    private MetricKey(String context, String runId, String metric) {
      this.context = context;
      this.runId = runId;
      this.metric = metric;
    }

    String getContext() {
      return context;
    }

    private String getRunId() {
      return runId;
    }

    String getMetric() {
      return metric;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      MetricKey other = (MetricKey) o;
      return Objects.equal(context, other.context)
        && Objects.equal(metric, other.metric)
        && Objects.equal(runId, other.runId);
    }

    @Override
    public int hashCode() {
      int result = context.hashCode();
      result = 31 * result + (runId != null ? runId.hashCode() : 0);
      result = 31 * result + metric.hashCode();
      return result;
    }
  }
}

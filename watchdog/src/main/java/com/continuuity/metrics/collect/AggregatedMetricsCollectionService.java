/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.collect;

import com.continuuity.metrics.transport.MetricRecord;
import com.continuuity.weave.common.Threads;
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
          return new AggregatedMetricsEmitter(key.getContext(), key.getMetric());
        }
      });
  }

  /**
   * Publishes the given collection of {@link MetricRecord}. When this method returns, the
   * given {@link Iterator} will no longer be valid. This method should process the input
   * iterator and returns quickly. Any long operations should be run in a separated thread.
   * This method is guaranteed not to get concurrent calls.
   *
   * @param metrics collection of {@link MetricRecord} to publish.
   * @throws Exception if there is error raised during publish.
   */
  protected abstract void publish(Iterator<MetricRecord> metrics) throws Exception;

  @Override
  protected final void runOneIteration() throws Exception {
    final long timestamp = TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    LOG.debug("Start log collection for timestamp {}", timestamp);
    Iterator<MetricRecord> metricsItor = getMetrics(timestamp);

    try {
      publish(metricsItor);
    } catch (Throwable t) {
      LOG.error("Failed in publishing metrics for timestamp {}.", timestamp, t);
    }

    // Consume the whole iterator if it is not yet consumed inside publish. This is to make sure metrics are reset.
    while (metricsItor.hasNext()) {
      metricsItor.next();
    }
    LOG.debug("Completed log collection for timestamp {}", timestamp);
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
  public final MetricsCollector getCollector(String context, String metricName) {
    return collectors.getUnchecked(new MetricKey(context, metricName));
  }

  private Iterator<MetricRecord> getMetrics(final long timestamp) {
    final Iterator<? extends MetricsEmitter> iterator = collectors.asMap().values().iterator();
    return new AbstractIterator<MetricRecord>() {
      @Override
      protected MetricRecord computeNext() {
        while (iterator.hasNext()) {
          MetricRecord metricRecord = iterator.next().emit(timestamp);
          if (metricRecord.getValue() != 0) {
            LOG.debug("Emit metric {}", metricRecord);
            return metricRecord;
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
    private final String metric;

    private MetricKey(String context, String metric) {
      this.context = context;
      this.metric = metric;
    }

    String getContext() {
      return context;
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
      return context.equals(other.context) && metric.equals(other.metric);
    }

    @Override
    public int hashCode() {
      int result = context.hashCode();
      result = 31 * result + metric.hashCode();
      return result;
    }
  }
}

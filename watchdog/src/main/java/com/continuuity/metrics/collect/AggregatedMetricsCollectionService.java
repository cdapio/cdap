/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.collect;

import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.common.metrics.MetricsCollector;
import com.continuuity.common.metrics.MetricsScope;
import com.continuuity.metrics.transport.MetricsRecord;
import com.google.common.base.Objects;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.AbstractIterator;
import com.google.common.util.concurrent.AbstractScheduledService;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
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

  private final LoadingCache<CollectorKey, MetricsCollector> collectors;
  private final LoadingCache<EmitterKey, AggregatedMetricsEmitter> emitters;

  public AggregatedMetricsCollectionService() {
    this.collectors = CacheBuilder.newBuilder()
      .expireAfterAccess(CACHE_EXPIRE_MINUTES, TimeUnit.MINUTES)
      .build(createCollectorLoader());

    this.emitters = CacheBuilder.newBuilder()
      .expireAfterAccess(CACHE_EXPIRE_MINUTES, TimeUnit.MINUTES)
      .build(new CacheLoader<EmitterKey, AggregatedMetricsEmitter>() {
        @Override
        public AggregatedMetricsEmitter load(EmitterKey key) throws Exception {
          return new AggregatedMetricsEmitter(key.getCollectorKey().getContext(),
                                              key.getCollectorKey().getRunId(),
                                              key.getMetric());
        }
      });
  }

  /**
   * Publishes the given collection of {@link MetricsRecord}. When this method returns, the
   * given {@link Iterator} will no longer be valid. This method should process the input
   * iterator and returns quickly. Any long operations should be run in a separated thread.
   * This method is guaranteed not to get concurrent calls.
   *
   * @param scope The scope of metrics to be published.
   * @param metrics collection of {@link com.continuuity.metrics.transport.MetricsRecord} to publish.
   * @throws Exception if there is error raised during publish.
   */
  protected abstract void publish(MetricsScope scope, Iterator<MetricsRecord> metrics) throws Exception;

  @Override
  protected final void runOneIteration() throws Exception {
    final long timestamp = TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    LOG.trace("Start log collection for timestamp {}", timestamp);
    for (MetricsScope scope : MetricsScope.values()) {
      Iterator<MetricsRecord> metricsItor = getMetrics(scope, timestamp);

      try {
        publish(scope, metricsItor);
      } catch (Throwable t) {
        LOG.error("Failed in publishing metrics for timestamp {}.", timestamp, t);
      }

      // Consume the whole iterator if it is not yet consumed inside publish. This is to make sure metrics are reset.
      while (metricsItor.hasNext()) {
        metricsItor.next();
      }
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
  public final MetricsCollector getCollector(final MetricsScope scope, final String context, final String runId) {
    return collectors.getUnchecked(new CollectorKey(scope, context, runId));
  }

  @Override
  protected void shutDown() throws Exception {
    // Flush the metrics when shutting down.
    runOneIteration();
  }

  private Iterator<MetricsRecord> getMetrics(final MetricsScope scope, final long timestamp) {
    final Iterator<Map.Entry<EmitterKey, AggregatedMetricsEmitter>> iterator = emitters.asMap().entrySet().iterator();
    return new AbstractIterator<MetricsRecord>() {
      @Override
      protected MetricsRecord computeNext() {
        while (iterator.hasNext()) {
          Map.Entry<EmitterKey, AggregatedMetricsEmitter> entry = iterator.next();
          if (entry.getKey().getCollectorKey().getScope() != scope) {
            continue;
          }

          MetricsRecord metricsRecord = entry.getValue().emit(timestamp);
          if (metricsRecord.getValue() != 0) {
            LOG.trace("Emit metric {}", metricsRecord);
            return metricsRecord;
          }
        }
        return endOfData();
      }
    };
  }

  private CacheLoader<CollectorKey, MetricsCollector> createCollectorLoader() {
    return new CacheLoader<CollectorKey, MetricsCollector>() {
      @Override
      public MetricsCollector load(final CollectorKey collectorKey) throws Exception {
        return new MetricsCollector() {

          // Cache for minimizing creating new MetricKey object.
          private final LoadingCache<String, EmitterKey> keys =
            CacheBuilder.newBuilder()
              .expireAfterAccess(CACHE_EXPIRE_MINUTES, TimeUnit.MINUTES)
              .build(new CacheLoader<String, EmitterKey>() {
                @Override
                public EmitterKey load(String metric) throws Exception {
                  return new EmitterKey(collectorKey, metric);
                }
              });

          @Override
          public void gauge(String metricName, int value, String... tags) {
            emitters.getUnchecked(keys.getUnchecked(metricName)).gauge(value, tags);
          }
        };
      }
    };
  }

  /**
   * Inner class for cache key for looking up {@link MetricsCollector}.
   */
  private static final class CollectorKey {
    private final MetricsScope scope;
    private final String context;
    private final String runId;

    private CollectorKey(MetricsScope scope, String context, String runId) {
      this.scope = scope;
      this.context = context;
      this.runId = runId;
    }

    private MetricsScope getScope() {
      return scope;
    }

    private String getContext() {
      return context;
    }

    private String getRunId() {
      return runId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      CollectorKey other = (CollectorKey) o;

      return scope == other.scope
        && Objects.equal(context, other.context)
        && Objects.equal(runId, other.runId);
    }

    @Override
    public int hashCode() {
      int result = scope.hashCode();
      result = 31 * result + context.hashCode();
      result = 31 * result + runId.hashCode();
      return result;
    }
  }

  /**
   * Inner class for the cache key for looking up {@link AggregatedMetricsEmitter}.
   */
  private static final class EmitterKey {
    private final CollectorKey collectorKey;
    private final String metric;

    private EmitterKey(CollectorKey collectorKey, String metric) {
      this.collectorKey = collectorKey;
      this.metric = metric;
    }

    private CollectorKey getCollectorKey() {
      return collectorKey;
    }

    private String getMetric() {
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

      EmitterKey other = (EmitterKey) o;
      return Objects.equal(collectorKey, other.collectorKey)
        && Objects.equal(metric, other.metric);
    }

    @Override
    public int hashCode() {
      int result = collectorKey.hashCode();
      result = 31 * result + metric.hashCode();
      return result;
    }
  }
}

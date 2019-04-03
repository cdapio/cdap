/*
 * Copyright © 2014-2019 Cask Data, Inc.
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
package io.cdap.cdap.metrics.collect;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.Uninterruptibles;
import io.cdap.cdap.api.metrics.MetricType;
import io.cdap.cdap.api.metrics.MetricValue;
import io.cdap.cdap.api.metrics.MetricValues;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.common.conf.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * Base class for {@link MetricsCollectionService} which collect metrics through a set of cached
 * {@link AggregatedMetricsEmitter}.
 */
public abstract class AggregatedMetricsCollectionService extends AbstractExecutionThreadService
                                                         implements MetricsCollectionService {

  private static final Logger LOG = LoggerFactory.getLogger(AggregatedMetricsCollectionService.class);
  private static final long CACHE_EXPIRE_MINUTES = 1;

  private final LoadingCache<Map<String, String>, MetricsContext> collectors;
  private final LoadingCache<Map<String, String>, LoadingCache<String, AggregatedMetricsEmitter>> emitters;
  // maximum number of milliseconds to sleep between each publish
  private final long publishIntervalInMillis;
  private final CountDownLatch shutdownLatch;

  public AggregatedMetricsCollectionService(long publishIntervalInMillis) {
    // the longest sleep time will be 1 min
    this.publishIntervalInMillis = Math.min(publishIntervalInMillis, Constants.Metrics.PROCESS_INTERVAL_MILLIS);
    this.shutdownLatch = new CountDownLatch(1);
    this.collectors = CacheBuilder.newBuilder()
      .expireAfterAccess(CACHE_EXPIRE_MINUTES, TimeUnit.MINUTES)
      .build(createCollectorLoader());

    this.emitters = CacheBuilder.newBuilder()
      // NOTE : we don't need to have removalListener to  emit metrics, as we have expireAfterAccess set for a minute,
      // emitters.get() is used to increment/gauge and that would reset the access time,
      // and since runOneIteration() emits all the metrics for the scheduled duration (every 1 second)
      // there wont be any loss of emitter entries.
      .expireAfterAccess(CACHE_EXPIRE_MINUTES, TimeUnit.MINUTES)
      .build(new CacheLoader<Map<String, String>, LoadingCache<String, AggregatedMetricsEmitter>>() {
        @Override
        public LoadingCache<String, AggregatedMetricsEmitter> load(Map<String, String> tags) throws Exception {
          return CacheBuilder.newBuilder().expireAfterAccess(CACHE_EXPIRE_MINUTES, TimeUnit.MINUTES).
            build(new CacheLoader<String, AggregatedMetricsEmitter>() {
              @Override
              public AggregatedMetricsEmitter load(String metricName) throws Exception {
                return new AggregatedMetricsEmitter(metricName);
              }
            });
        }
      });
  }

  /**
   * Publishes the given collection of {@link io.cdap.cdap.api.metrics.MetricValues}. When this method returns, the
   * given {@link Iterator} will no longer be valid. This method should process the input
   * iterator and returns quickly. Any long operations should be run in a separated thread.
   * This method is guaranteed not to get concurrent calls.
   *
   * @param metrics collection of {@link io.cdap.cdap.api.metrics.MetricValues} to publish.
   * @throws Exception if there is error raised during publish.
   */
  protected abstract void publish(Iterator<MetricValues> metrics) throws Exception;

  /**
   * Returns the initial delay in milliseconds for the first metrics to be published. By default this will be the
   * publish intervals
   */
  protected long getInitialDelayMillis() {
    return publishIntervalInMillis;
  }

  @Override
  protected void startUp() throws Exception {
    // No-op
  }

  @Override
  protected final void run() {
    long sleepMillis = getInitialDelayMillis();
    while (isRunning()) {
      if (Uninterruptibles.awaitUninterruptibly(shutdownLatch, sleepMillis, TimeUnit.MILLISECONDS)) {
        // If the shutdown latch is triggered, it means triggerShutdown has been called, hence break the loop
        break;
      }

      long startTime = System.currentTimeMillis();
      publishMetrics(startTime);
      sleepMillis = Math.max(0, publishIntervalInMillis - (System.currentTimeMillis() - startTime));
    }
  }

  private void publishMetrics(long currentTimeMillis) {
    long timestamp = TimeUnit.MILLISECONDS.toSeconds(currentTimeMillis);

    LOG.trace("Start log collection for timestamp {}", timestamp);

    Iterator<MetricValues> metrics = getMetrics(timestamp);
    try {
      publish(metrics);
    } catch (Throwable t) {
      LOG.error("Failed in publishing metrics for timestamp {}.", timestamp, t);
    }

    // Consume the whole iterator if it is not yet consumed inside publish. This is to make sure metrics are reset.
    while (metrics.hasNext()) {
      metrics.next();
    }
    LOG.trace("Completed log collection for timestamp {}", timestamp);
  }

  @Override
  protected Executor executor() {
    final String name = getClass().getSimpleName();
    return new Executor() {
      @Override
      public void execute(Runnable command) {
        Thread thread = new Thread(command, name);
        thread.setDaemon(true);
        thread.start();
      }
    };
  }

  @Override
  public final MetricsContext getContext(final Map<String, String> tags) {
    return collectors.getUnchecked(tags);
  }

  @Override
  protected void shutDown() throws Exception {
    // Flush the metrics when shutting down.
    publishMetrics(System.currentTimeMillis());
  }

  @Override
  protected void triggerShutdown() {
    shutdownLatch.countDown();
  }

  private Iterator<MetricValues> getMetrics(final long timestamp) {
    // NOTE : emitters.asMap does not reset the access time in cache,
    // so it's the preferred way to access the cache entries. as we access and emit metrics every second.
    final Iterator<Map.Entry<Map<String, String>, LoadingCache<String, AggregatedMetricsEmitter>>> iterator =
      emitters.asMap().entrySet().iterator();
    return new AbstractIterator<MetricValues>() {
      @Override
      protected MetricValues computeNext() {
        while (iterator.hasNext()) {
          Map.Entry<Map<String, String>, LoadingCache<String, AggregatedMetricsEmitter>> entry = iterator.next();
          Map<String, AggregatedMetricsEmitter> metricEmitters = entry.getValue().asMap();
          // +1 because we add extra metric about how many metric values did we emit in this context (see below)
          List<MetricValue> metricValues = Lists.newArrayListWithCapacity(metricEmitters.size() + 1);
          for (Map.Entry<String, AggregatedMetricsEmitter> emitterEntry : metricEmitters.entrySet()) {
            MetricValue metricValue = emitterEntry.getValue().emit();
            // skip increment by 0
            if (metricValue.getType() == MetricType.COUNTER && metricValue.getValue() == 0) {
              continue;
            }
            metricValues.add(metricValue);
          }

          if (metricValues.isEmpty()) {
            // skip if there are no metric values to send
            continue;
          }

          // number of emitted metrics
          metricValues.add(new MetricValue("metrics.emitted.count", MetricType.COUNTER, metricValues.size() + 1));

          LOG.trace("Emit metric {}", metricValues);
          return new MetricValues(entry.getKey(), timestamp, metricValues);
        }
        return endOfData();
      }
    };
  }

  private CacheLoader<Map<String, String>, MetricsContext> createCollectorLoader() {
    return new CacheLoader<Map<String, String>, MetricsContext>() {
      @Override
      public MetricsContext load(final Map<String, String> collectorKey) throws Exception {
        return new MetricsContextImpl(collectorKey);
      }
    };
  }

  private final class MetricsContextImpl implements MetricsContext {

    private final Map<String, String> tags;

    private MetricsContextImpl(final Map<String, String> tags) {
      this.tags = ImmutableMap.copyOf(tags);
    }

    @Override
    public void increment(String metricName, long value) {
      emitters.getUnchecked(tags).getUnchecked(metricName).increment(value);
    }

    @Override
    public void gauge(String metricName, long value) {
      emitters.getUnchecked(tags).getUnchecked(metricName).gauge(value);
    }

    @Override
    public MetricsContext childContext(String tagName, String tagValue) {
      ImmutableMap<String, String> allTags = ImmutableMap.<String, String>builder()
        .putAll(tags).put(tagName, tagValue).build();
      return collectors.getUnchecked(allTags);
    }

    @Override
    public Map<String, String> getTags() {
      return tags;
    }

    @Override
    public MetricsContext childContext(Map<String, String> tags) {
      if (tags.isEmpty()) {
        return this;
      }
      // todo: may be warn when duplicate tag is provided? for now ok
      Map<String, String> allTags = Maps.newHashMap();
      allTags.putAll(this.tags);
      allTags.putAll(tags);
      return collectors.getUnchecked(allTags);
    }
  }
}

/*
 * Copyright © 2014-2018 Cask Data, Inc.
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

import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.metrics.MetricValues;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.id.Id;
import co.cask.cdap.metrics.process.MessagingMetricsProcessorService;
import co.cask.cdap.metrics.process.MessagingMetricsProcessorServiceFactory;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A {@link co.cask.cdap.api.metrics.MetricsCollectionService} that writes to MetricsTable directly.
 * It also has a scheduling job that clean up old metrics periodically.
 */
@Singleton
public final class LocalMetricsCollectionService extends AggregatedMetricsCollectionService {
  private static final Logger LOG = LoggerFactory.getLogger(LocalMetricsCollectionService.class);

  private static final ImmutableMap<String, String> METRICS_PROCESSOR_CONTEXT =
    ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, Id.Namespace.SYSTEM.getId(),
                    Constants.Metrics.Tag.COMPONENT, Constants.Service.METRICS_PROCESSOR);

  private final CConfiguration cConf;
  private final MetricStore metricStore;
  private ScheduledExecutorService scheduler;
  private MessagingMetricsProcessorServiceFactory messagingMetricsProcessorFactory;
  private MessagingMetricsProcessorService messagingMetricsProcessor;
  private long numPublish = 0L;
  private long avgTime = 0L;
  private long userMetricsCount = 0L;
  private long systemMetricsCount = 0L;

  @Inject
  LocalMetricsCollectionService(CConfiguration cConf, MetricStore metricStore) {
    this.cConf = cConf;
    this.metricStore = metricStore;
    metricStore.setMetricsContext(this.getContext(METRICS_PROCESSOR_CONTEXT));
  }

  /**
   * Setter method for the optional binding on the {@link MessagingMetricsProcessorServiceFactory}.
   */
  @Inject (optional = true)
  public void setMessagingMetricsProcessorFactory(MessagingMetricsProcessorServiceFactory factory) {
    this.messagingMetricsProcessorFactory = factory;
  }

  @Override
  public void publish(Iterator<MetricValues> metrics) {
    Map<Integer, Long> oldWriteTime = getWriteTime();
    Map<Integer, Long> oldMetricsCount = getMetricsCount();
    long startTime = System.currentTimeMillis();
    long count = 0;
    List<MetricValues> metricValues = new ArrayList<>();
    while (metrics.hasNext()) {
      count++;
      MetricValues next = metrics.next();
      if (next.getTags().containsKey(Constants.Metrics.Tag.SCOPE)) {
        userMetricsCount++;
      } else {
        systemMetricsCount++;
      }
      metricValues.add(next);
    }
    metricStore.add(metricValues);
    long duration = System.currentTimeMillis() - startTime;
    if (count > 50) {
      numPublish++;
      long delta = duration - avgTime;
      avgTime += delta / numPublish;
      Map<Integer, Long> newWriteTime = getWriteTime();
      Map<Integer, Long> newMetricsCount = getMetricsCount();
      Map<Integer, Long> timeElapsed = new HashMap<>();
      Map<Integer, Long> writeHappened = new HashMap<>();
      for (Map.Entry<Integer, Long> entry : oldWriteTime.entrySet()) {
        timeElapsed.put(entry.getKey(), newWriteTime.get(entry.getKey()) - entry.getValue());
      }

      for (Map.Entry<Integer, Long> entry : oldMetricsCount.entrySet()) {
        writeHappened.put(entry.getKey(), newMetricsCount.get(entry.getKey()) - entry.getValue());
      }
      LOG.info("Added {} metrics in one publish for {} milliseconds, the time happened in leveldb write" +
                 "is: {}, the number of level db writes is: {}", count, duration, timeElapsed, writeHappened);
      if (numPublish % 100 == 0) {
        LOG.info("Avg time in publish is {}", avgTime);
      }
    }
  }

  public long getAvgTime() {
    return avgTime;
  }

  public long getUserMetricsCount() {
    return userMetricsCount;
  }

  public long getSystemMetricsCount() {
    return systemMetricsCount;
  }

  public Map<Integer, Long> getMetricsCount() {
    return metricStore.getCounts();
  }

  public Map<Integer, Long> getWriteTime() {
    return metricStore.getWriteTime();
  }

  @Override
  protected void startUp() throws Exception {
    super.startUp();

    // If there is a metrics processor for TMS, start it.
    if (messagingMetricsProcessorFactory != null) {
      messagingMetricsProcessor = messagingMetricsProcessorFactory.create(
        IntStream.range(0, cConf.getInt(Constants.Metrics.MESSAGING_TOPIC_NUM)).boxed().collect(Collectors.toSet()),
        getContext(METRICS_PROCESSOR_CONTEXT), 0);
      messagingMetricsProcessor.startAndWait();
    }

    // It will only do cleanup if the underlying table doesn't supports TTL.
    scheduler = Executors.newSingleThreadScheduledExecutor(Threads.createDaemonThreadFactory("metrics-cleanup"));
    long secRetentionSecs = cConf.getLong(Constants.Metrics.RETENTION_SECONDS + Constants.Metrics.SECOND_RESOLUTION +
                                            Constants.Metrics.RETENTION_SECONDS_SUFFIX);
    // Try right away if there's anything to cleanup, we will then schedule based on the min retention interval
    scheduler.schedule(createCleanupTask(secRetentionSecs), 1, TimeUnit.SECONDS);
  }

  @Override
  protected void shutDown() throws Exception {
    if (scheduler != null) {
      scheduler.shutdownNow();
    }

    // Shutdown the TMS metrics processor if present. This will flush all buffered metrics that were read from TMS
    Exception failure = null;
    try {
      if (messagingMetricsProcessor != null) {
        messagingMetricsProcessor.stopAndWait();
      }
    } catch (Exception e) {
      failure = e;
    }

    // Shutdown the local metrics process. This will flush all in memory metrics.
    try {
      super.shutDown();
    } catch (Exception e) {
      if (failure != null) {
        failure.addSuppressed(e);
      } else {
        failure = e;
      }
    }

    if (failure != null) {
      throw failure;
    }
  }

  /**
   * Creates a task for cleanup.
   *
   * @param nextScheduleDelaySecs The next schedule delay in seconds for the clean up task.
   */
  private Runnable createCleanupTask(long nextScheduleDelaySecs) {
    return new Runnable() {
      @Override
      public void run() {
        // We perform CleanUp only in LocalMetricsCollectionService , where TTL is NOT supported
        // by underlying data store.
        long currentTime = TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        try {
          // delete metrics from metrics resolution table
          metricStore.deleteTTLExpired();
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
        // delete based on the min retention interval
        scheduler.schedule(this, nextScheduleDelaySecs, TimeUnit.SECONDS);
      }
    };
  }
}

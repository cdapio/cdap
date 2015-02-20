/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.data.stream.service;

import co.cask.cdap.api.metrics.MetricDataQuery;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.metrics.MetricTimeSeries;
import co.cask.cdap.api.metrics.MetricType;
import co.cask.cdap.api.metrics.TimeValue;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.stream.StreamCoordinatorClient;
import co.cask.cdap.proto.Id;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.Service;
import org.apache.twill.common.Threads;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Stream service meant to run in an HTTP service.
 */
public abstract class AbstractStreamService extends AbstractScheduledService implements StreamService {

  private final StreamCoordinatorClient streamCoordinatorClient;
  private final StreamFileJanitorService janitorService;
  private final StreamWriterSizeCollector sizeCollector;
  private final MetricStore metricStore;

  private ScheduledExecutorService executor;

  /**
   * Children classes should implement this method to add logic to the start of this {@link Service}.
   *
   * @throws Exception in case of any error while initializing
   */
  protected abstract void initialize() throws Exception;

  /**
   * Children classes should implement this method to add logic to the shutdown of this {@link Service}.
   *
   * @throws Exception in case of any error while shutting down
   */
  protected abstract void doShutdown() throws Exception;

  /**
   * @return The {@link StreamCoordinatorClient} used by this {@link StreamService}.
   */
  protected StreamCoordinatorClient getStreamCoordinatorClient() {
    return streamCoordinatorClient;
  }

  protected AbstractStreamService(StreamCoordinatorClient streamCoordinatorClient,
                                  StreamFileJanitorService janitorService,
                                  StreamWriterSizeCollector sizeCollector,
                                  MetricStore metricStore) {
    this.streamCoordinatorClient = streamCoordinatorClient;
    this.janitorService = janitorService;
    this.sizeCollector = sizeCollector;
    this.metricStore = metricStore;
  }

  @Override
  protected final void startUp() throws Exception {
    streamCoordinatorClient.startAndWait();
    janitorService.startAndWait();
    sizeCollector.startAndWait();
    initialize();
  }

  @Override
  protected final void shutDown() throws Exception {
    doShutdown();

    if (executor != null) {
      executor.shutdownNow();
    }

    sizeCollector.stopAndWait();
    janitorService.stopAndWait();
    streamCoordinatorClient.stopAndWait();
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(0, Constants.Stream.HEARTBEAT_INTERVAL, TimeUnit.SECONDS);
  }

  @Override
  protected ScheduledExecutorService executor() {
    executor = Executors.newSingleThreadScheduledExecutor(Threads.createDaemonThreadFactory("heartbeats-scheduler"));
    return executor;
  }

  /**
   * Get the size of events ingested by a stream since its creation, in bytes.
   * @param streamId id of the stream
   * @return Size of events ingested by a stream since its creation
   * @throws IOException when getting an error retrieving the metric
   */
  protected long getStreamEventsSize(Id.Stream streamId) throws IOException {
    MetricDataQuery metricDataQuery = new MetricDataQuery(
      0L, TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()),
      Integer.MAX_VALUE, "system.collect.bytes",
      MetricType.COUNTER,
      ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, streamId.getNamespaceId(),
                      Constants.Metrics.Tag.STREAM, streamId.getName()),
      ImmutableList.<String>of()
    );

    try {
      Collection<MetricTimeSeries> metrics = metricStore.query(metricDataQuery);
      if (metrics == null || metrics.isEmpty()) {
        // Data is not yet available, which means no data has been ingested by the stream yet
        return 0L;
      }

      MetricTimeSeries metric = metrics.iterator().next();
      List<TimeValue> timeValues = metric.getTimeValues();
      if (timeValues == null || timeValues.size() != 1) {
        throw new IOException("Should only collect one time value");
      }
      return timeValues.get(0).getValue();
    } catch (Exception e) {
      Throwables.propagateIfInstanceOf(e, IOException.class);
      throw new IOException(e);
    }
  }
}

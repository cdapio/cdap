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

import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.stream.StreamCoordinatorClient;
import co.cask.cdap.data.stream.service.heartbeat.StreamWriterHeartbeat;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.Service;
import org.apache.twill.common.Threads;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Stream service meant to run in an HTTP service.
 */
public abstract class AbstractStreamService extends AbstractScheduledService implements StreamService {

  private final StreamAdmin streamAdmin;
  private final StreamCoordinatorClient streamCoordinatorClient;
  private final StreamFileJanitorService janitorService;
  private final StreamMetaStore streamMetaStore;
  private final StreamWriterSizeCollector streamWriterSizeCollector;
  private final StreamWriterSizeFetcher streamWriterSizeFetcher;
  private final int writerID;
  private final Map<String, Long> streamsBaseSizes;

  private volatile ScheduledExecutorService executor;

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

  protected AbstractStreamService(StreamAdmin streamAdmin,
                                  StreamCoordinatorClient streamCoordinatorClient,
                                  StreamFileJanitorService janitorService,
                                  StreamMetaStore streamMetaStore,
                                  StreamWriterSizeCollector streamWriterSizeCollector,
                                  StreamWriterSizeFetcher streamWriterSizeFetcher,
                                  int writerID) {
    this.streamAdmin = streamAdmin;
    this.streamCoordinatorClient = streamCoordinatorClient;
    this.janitorService = janitorService;
    this.streamMetaStore = streamMetaStore;
    this.streamWriterSizeCollector = streamWriterSizeCollector;
    this.streamWriterSizeFetcher = streamWriterSizeFetcher;
    this.writerID = writerID;
    this.streamsBaseSizes = Maps.newHashMap();
  }

  @Override
  protected final void startUp() throws Exception {
    streamCoordinatorClient.startAndWait();
    janitorService.startAndWait();
    initialize();
    // TODO we should also run one iteration, and send an init for all streams at this step.
    // Use a boolean to know we're in init mod, and runOneIteration will set type to init
  }

  @Override
  protected final void shutDown() throws Exception {
    doShutdown();
    if (executor != null) {
      executor.shutdownNow();
    }
    janitorService.stopAndWait();
    streamCoordinatorClient.stopAndWait();
  }

  @Override
  protected void runOneIteration() throws Exception {
    StreamWriterHeartbeat.Builder builder = new StreamWriterHeartbeat.Builder();
    for (StreamSpecification streamSpec : streamMetaStore.listStreams()) {
      Long baseSize = streamsBaseSizes.get(streamSpec.getName());
      if (baseSize == null) {
        // First time that this stream is called in this method
        baseSize = streamWriterSizeFetcher.fetchSize(streamAdmin.getConfig(streamSpec.getName()));
        streamsBaseSizes.put(streamSpec.getName(), baseSize);
      }

      long absoluteSize = baseSize + streamWriterSizeCollector.getTotalCollected(streamSpec.getName());
      builder.addStreamSize(streamSpec.getName(), absoluteSize, StreamWriterHeartbeat.StreamSizeType.REGULAR);
    }
    builder.setWriterID(writerID);
    builder.setTimestamp(System.currentTimeMillis());

    // TODO use a heartbeatPublisher here to publish heartbeat
  }

  @Override
  protected ScheduledExecutorService executor() {
    executor = Executors.newSingleThreadScheduledExecutor(Threads.createDaemonThreadFactory("heartbeats-scheduler"));
    return executor;
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(Constants.Stream.HEARTBEAT_DELAY, Constants.Stream.HEARTBEAT_DELAY,
                                          TimeUnit.SECONDS);
  }
}

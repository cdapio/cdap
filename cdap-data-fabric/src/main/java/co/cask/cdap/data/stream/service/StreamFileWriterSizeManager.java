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
import co.cask.cdap.data.stream.StreamFileType;
import co.cask.cdap.data.stream.StreamUtils;
import co.cask.cdap.data.stream.service.heartbeat.HeartbeatPublisher;
import co.cask.cdap.data.stream.service.heartbeat.StreamWriterHeartbeat;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.twill.common.Threads;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * {@link StreamWriterSizeManager} implementation for Streams written to files.
 */
public class StreamFileWriterSizeManager extends AbstractIdleService implements StreamWriterSizeManager {
  private static final Logger LOG = LoggerFactory.getLogger(StreamFileWriterSizeManager.class);

  private static final int EXECUTOR_POOL_SIZE = 10;
  private static final int HEARTBEAT_DELAY = 2;

  private final StreamMetaStore streamMetaStore;
  private final HeartbeatPublisher heartbeatPublisher;
  private final StreamAdmin streamAdmin;
  private final int instanceId;

  // Note: Stores stream name to absolute size in bytes.
  private final ConcurrentMap<String, Long> absoluteSizes;

  private ListeningScheduledExecutorService scheduledExecutor;

  @Inject
  public StreamFileWriterSizeManager(StreamMetaStore streamMetaStore, HeartbeatPublisher heartbeatPublisher,
                                     StreamAdmin streamAdmin,
                                     @Named(Constants.Stream.CONTAINER_INSTANCE_ID) int instanceId) {
    this.streamMetaStore = streamMetaStore;
    this.heartbeatPublisher = heartbeatPublisher;
    this.streamAdmin = streamAdmin;
    this.instanceId = instanceId;
    this.absoluteSizes = Maps.newConcurrentMap();
  }

  @Override
  protected void startUp() throws Exception {
    heartbeatPublisher.startAndWait();

    scheduledExecutor = MoreExecutors.listeningDecorator(
      Executors.newScheduledThreadPool(EXECUTOR_POOL_SIZE,
                                       Threads.createDaemonThreadFactory("stream-writer-size-manager")));

    List<ListenableFuture<StreamWriterHeartbeat>> futures = Lists.newArrayList();
    for (StreamSpecification streamSpecification : streamMetaStore.listStreams()) {
      final StreamConfig config = streamAdmin.getConfig(streamSpecification.getName());
      futures.add(initStreamSizeManagement(config));
      scheduleHeartbeats(config.getName());
    }

    Futures.allAsList(futures).get();
  }

  @Override
  protected void shutDown() throws Exception {
    heartbeatPublisher.stopAndWait();
    scheduledExecutor.shutdownNow();
  }

  @Override
  public void received(String streamName, long dataSize) {
    boolean success;
    do {
      Long currentSize = absoluteSizes.get(streamName);
      long newSize = currentSize + dataSize;
      success = absoluteSizes.replace(streamName, currentSize, newSize);
    } while (!success);
  }

  private int convertToMB(long byteSize) {
    return (int) (byteSize / 1000000);
  }

  /**
   * Start counting the sizes of the files owned by this stream handler, related to the stream
   * which {@code config} is in parameter. The first step is to read the file system
   * to get the sizes of those files, and to send an initial heartbeat with the computed size.
   */
  private ListenableFuture<StreamWriterHeartbeat> initStreamSizeManagement(final StreamConfig config) {
    return scheduledExecutor.submit(new Callable<StreamWriterHeartbeat>() {
      @Override
      public StreamWriterHeartbeat call() throws Exception {
        long size = getStreamWriterFilesSize(config);
        absoluteSizes.putIfAbsent(config.getName(), size);

        LOG.debug("Sending initial heartbeat for Stream handler {} with base size {}B", instanceId, size);
        return heartbeatPublisher.sendHeartbeat(
          new StreamWriterHeartbeat(System.currentTimeMillis(), convertToMB(size),
                                    instanceId, StreamWriterHeartbeat.Type.INIT))
          .get();
      }
    });
  }

  /**
   * Schedule publishing heartbeats for the {@code streamName}. At fixed rate, a heartbeat will be send
   * with containing the absolute size of the files own by this stream handler and concerning the stream
   * {@code streamName}.
   */
  private void scheduleHeartbeats(final String streamName) {
    scheduledExecutor.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        Long size = absoluteSizes.get(streamName);

        // We don't want to block this executor, or make it fail if the get method on the future fails,
        // hence we don't call the get method
        heartbeatPublisher.sendHeartbeat(
          new StreamWriterHeartbeat(System.currentTimeMillis(), convertToMB(size),
                                    instanceId, StreamWriterHeartbeat.Type.REGULAR));
      }
    }, HEARTBEAT_DELAY, HEARTBEAT_DELAY, TimeUnit.SECONDS);
  }

  /**
   * Get the size of the files written by one stream handler for one stream.
   * @param streamConfig configuration of the stream to get partial size of.
   * @return the size of the files written by the stream handler executing this, for the stream
   * given by the {@code streamConfig}.
   * @throws IOException in case of issues accessing the stream files.
   */
  private long getStreamWriterFilesSize(StreamConfig streamConfig) throws IOException {
    Location streamPath = StreamUtils.createGenerationLocation(streamConfig.getLocation(),
                                                               StreamUtils.getGeneration(streamConfig));
    long size = 0;
    List<Location> locations = streamPath.list();
    // All directories are partition directories
    for (Location location : locations) {
      if (!location.isDirectory()) {
        continue;
      }

      // TODO add a note about TTL here
      List<Location> partitionFiles = location.list();
      for (Location partitionFile : partitionFiles) {
        if (!partitionFile.isDirectory()
          && StreamFileType.EVENT.isMatched(partitionFile.getName())
          && StreamUtils.getWriterInstanceId(partitionFile.getName()) == instanceId) {
          size += partitionFile.length();
        }
      }
    }
    return size;
  }
}

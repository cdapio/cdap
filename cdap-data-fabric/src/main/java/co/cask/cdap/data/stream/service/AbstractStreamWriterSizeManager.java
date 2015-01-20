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

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.stream.service.heartbeat.HeartbeatPublisher;
import co.cask.cdap.data.stream.service.heartbeat.StreamWriterHeartbeat;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.name.Named;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Common implementation of a {@link StreamWriterSizeManager} that keeps the size of data ingested by one stream writer
 * for all streams in memory.
 */
public abstract class AbstractStreamWriterSizeManager
  extends AbstractIdleService
  implements StreamWriterSizeManager, StreamWriterSizeCollector {
  private static final Logger LOG = LoggerFactory.getLogger(ConcurrentStreamWriter.class);
  private static final int EXECUTOR_POOL_SIZE = 10;

  private final HeartbeatPublisher heartbeatPublisher;
  private final int instanceId;

  // Note: Stores stream name to absolute size in bytes.
  private final ConcurrentMap<String, Long> absoluteSizes;
  private ListeningScheduledExecutorService scheduledExecutor;

  protected AbstractStreamWriterSizeManager(HeartbeatPublisher heartbeatPublisher,
                                            @Named(Constants.Stream.CONTAINER_INSTANCE_ID) int instanceId) {
    this.heartbeatPublisher = heartbeatPublisher;
    this.instanceId = instanceId;
    this.absoluteSizes = Maps.newConcurrentMap();
  }

  @Override
  protected void startUp() throws Exception {
    heartbeatPublisher.startAndWait();
    scheduledExecutor = MoreExecutors.listeningDecorator(
      Executors.newScheduledThreadPool(EXECUTOR_POOL_SIZE,
                                       Threads.createDaemonThreadFactory("stream-writer-size-manager")));
  }

  @Override
  protected void shutDown() throws Exception {
    scheduledExecutor.shutdownNow();

    // Send a last heartbeat before the Stream writer dies
    for (Map.Entry<String, Long> entry : absoluteSizes.entrySet()) {
      try {
        heartbeatPublisher.sendHeartbeat(entry.getKey(),
                                         new StreamWriterHeartbeat(System.currentTimeMillis(), entry.getValue(),
                                                                   instanceId, StreamWriterHeartbeat.Type.REGULAR))
          .get();
      } catch (Throwable t) {
        LOG.error("Could not publish final heartbeat for stream {} on writer instance {}",
                  entry.getKey(), instanceId, t);
      }
    }

    heartbeatPublisher.stopAndWait();
  }

  @Override
  public void received(String streamName, long dataSize) {
    if (dataSize <= 0) {
      return;
    }

    boolean success;
    do {
      Long currentSize = absoluteSizes.get(streamName);
      if (currentSize == null) {
        success = absoluteSizes.putIfAbsent(streamName, dataSize) == null;
        if (success) {
          // This thread successfully put the name of a new Stream in the map,
          // it can schedule the heartbeats for this stream too.
          scheduleHeartbeats(streamName);
        }
      } else {
        long newSize = currentSize + dataSize;
        success = absoluteSizes.replace(streamName, currentSize, newSize);
      }
    } while (!success);
  }

  protected HeartbeatPublisher getHeartbeatPublisher() {
    return heartbeatPublisher;
  }

  protected int getInstanceId() {
    return instanceId;
  }

  protected ConcurrentMap<String, Long> getAbsoluteSizes() {
    return absoluteSizes;
  }

  protected ListeningScheduledExecutorService getScheduledExecutor() {
    return scheduledExecutor;
  }

  /**
   * Schedule publishing heartbeats for the {@code streamName}. At fixed rate, a heartbeat will be sent
   * containing the absolute size of the files own by this stream handler and concerning the stream {@code streamName}.
   */
  protected void scheduleHeartbeats(final String streamName) {
    scheduledExecutor.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        try {
          Long size = absoluteSizes.get(streamName);

          // We don't want to block this executor, or make it fail if the get method on the future fails,
          // hence we don't call the get method
          heartbeatPublisher.sendHeartbeat(
            streamName,
            new StreamWriterHeartbeat(System.currentTimeMillis(), size,
                                      instanceId, StreamWriterHeartbeat.Type.REGULAR));
        } catch (Throwable t) {
          LOG.error("Could not send heartbeat for stream {}", streamName, t);
        }
      }
    }, Constants.Stream.HEARTBEAT_DELAY, Constants.Stream.HEARTBEAT_DELAY, TimeUnit.SECONDS);
  }


}

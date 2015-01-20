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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * {@link StreamWriterSizeManager} implementation for Streams written to files.
 */
public class StreamFileWriterSizeManager extends AbstractStreamWriterSizeManager {
  private static final Logger LOG = LoggerFactory.getLogger(StreamFileWriterSizeManager.class);

  private final StreamMetaStore streamMetaStore;
  private final StreamAdmin streamAdmin;

  @Inject
  public StreamFileWriterSizeManager(StreamMetaStore streamMetaStore, HeartbeatPublisher heartbeatPublisher,
                                     StreamAdmin streamAdmin,
                                     @Named(Constants.Stream.CONTAINER_INSTANCE_ID) int instanceId) {
    super(heartbeatPublisher, instanceId);
    this.streamMetaStore = streamMetaStore;
    this.streamAdmin = streamAdmin;
  }

  @Override
  public void initialize() throws Exception {
    List<ListenableFuture<StreamWriterHeartbeat>> futures = Lists.newArrayList();
    for (StreamSpecification streamSpecification : streamMetaStore.listStreams()) {
      final StreamConfig config = streamAdmin.getConfig(streamSpecification.getName());
      futures.add(initStreamSizeManagement(config));
      scheduleHeartbeats(config.getName());
    }

    Futures.allAsList(futures).get();
  }

  /**
   * Start counting the sizes of the files owned by this stream handler, related to the stream
   * which {@code config} is in parameter. The first step is to read the file system
   * to get the sizes of those files, and to send an initial heartbeat with the computed size.
   */
  private ListenableFuture<StreamWriterHeartbeat> initStreamSizeManagement(final StreamConfig config) {
    return getScheduledExecutor().submit(new Callable<StreamWriterHeartbeat>() {
      @Override
      public StreamWriterHeartbeat call() throws Exception {
        long size = getStreamWriterFilesSize(config);
        getAbsoluteSizes().putIfAbsent(config.getName(), size);

        LOG.debug("Sending initial heartbeat for Stream handler {} with base size {}B", getInstanceId(), size);
        return getHeartbeatPublisher().sendHeartbeat(
          config.getName(), new StreamWriterHeartbeat(System.currentTimeMillis(), size, getInstanceId(),
                                                      StreamWriterHeartbeat.Type.INIT))
          .get();
      }
    });
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

      // Note: by analyzing the size of all the files written to the FS by one stream writer,
      // we may also include data that has expired already, due to TTL. That is okay though,
      // because the point of the system is to keep track of increments of data. This initial
      // count is not meant to be accurate.

      List<Location> partitionFiles = location.list();
      for (Location partitionFile : partitionFiles) {
        if (!partitionFile.isDirectory()
          && StreamFileType.EVENT.isMatched(partitionFile.getName())
          && StreamUtils.getWriterInstanceId(partitionFile.getName()) == getInstanceId()) {
          size += partitionFile.length();
        }
      }
    }
    return size;
  }
}

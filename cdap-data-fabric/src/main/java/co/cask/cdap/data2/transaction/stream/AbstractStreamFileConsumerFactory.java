/*
 * Copyright © 2014-2015 Cask Data, Inc.
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
package co.cask.cdap.data2.transaction.stream;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.file.FileReader;
import co.cask.cdap.data.file.ReadFilter;
import co.cask.cdap.data.file.filter.TTLReadFilter;
import co.cask.cdap.data.stream.MultiLiveStreamFileReader;
import co.cask.cdap.data.stream.StreamEventOffset;
import co.cask.cdap.data.stream.StreamFileOffset;
import co.cask.cdap.data.stream.StreamFileType;
import co.cask.cdap.data.stream.StreamUtils;
import co.cask.cdap.data2.queue.ConsumerConfig;
import co.cask.cdap.data2.transaction.queue.QueueConstants;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.proto.Id;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Abstract base class for implementing {@link StreamConsumerFactory} using
 * {@link MultiLiveStreamFileReader}.
 */
public abstract class AbstractStreamFileConsumerFactory implements StreamConsumerFactory {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractStreamFileConsumerFactory.class);

  private final CConfiguration cConf;
  private final StreamAdmin streamAdmin;
  private final StreamConsumerStateStoreFactory stateStoreFactory;
  private final String tablePrefix;

  protected AbstractStreamFileConsumerFactory(CConfiguration cConf, StreamAdmin streamAdmin,
                                              StreamConsumerStateStoreFactory stateStoreFactory) {
    this.cConf = cConf;
    this.streamAdmin = streamAdmin;
    this.stateStoreFactory = stateStoreFactory;
    this.tablePrefix = String.format("%s.%s", Constants.SYSTEM_NAMESPACE, QueueConstants.QueueType.STREAM.toString());
  }

  /**
   * Creates a {@link StreamConsumer}.
   *
   * @param tableId Id of the table for storing process states
   * @param streamConfig configuration of the stream to consume from
   * @param consumerConfig configuration of the consumer
   * @param stateStore The {@link StreamConsumerStateStore} for recording consumer state
   * @param reader The {@link FileReader} to read stream events from
   * @return A new instance of {@link StreamConsumer}
   */
  protected abstract StreamConsumer create(
    TableId tableId, StreamConfig streamConfig, ConsumerConfig consumerConfig,
    StreamConsumerStateStore stateStore, StreamConsumerState beginConsumerState,
    FileReader<StreamEventOffset, Iterable<StreamFileOffset>> reader,
    @Nullable ReadFilter extraFilter) throws IOException;

  /**
   * Deletes process states table.
   *
   * @param tableId Id of the process states table.
   */
  protected abstract void dropTable(TableId tableId) throws IOException;

  protected void getFileOffsets(Location partitionLocation,
                                Collection<? super StreamFileOffset> fileOffsets,
                                int generation) throws IOException {
    // TODO: Support dynamic writer instances discovery
    // Current assume it won't change and is based on cConf
    int instances = cConf.getInt(Constants.Stream.CONTAINER_INSTANCES);
    String filePrefix = cConf.get(Constants.Stream.FILE_PREFIX);
    for (int i = 0; i < instances; i++) {
      // The actual file prefix is formed by file prefix in cConf + writer instance id
      String streamFilePrefix = filePrefix + '.' + i;
      Location eventLocation = StreamUtils.createStreamLocation(partitionLocation, streamFilePrefix,
                                                                0, StreamFileType.EVENT);
      fileOffsets.add(new StreamFileOffset(eventLocation, 0, generation));
    }
  }

  @Override
  public final StreamConsumer create(Id.Stream streamId, String namespace,
                                     ConsumerConfig consumerConfig) throws IOException {

    StreamConfig streamConfig = StreamUtils.ensureExists(streamAdmin, streamId);

    TableId tableId = getTableId(streamId, namespace);
    StreamConsumerStateStore stateStore = stateStoreFactory.create(streamConfig);
    StreamConsumerState consumerState = stateStore.get(consumerConfig.getGroupId(), consumerConfig.getInstanceId());

    return create(tableId, streamConfig, consumerConfig,
                  stateStore, consumerState, createReader(streamConfig, consumerState),
                  new TTLReadFilter(streamConfig.getTTL()));
  }

  @Override
  public void dropAll(Id.Stream streamId, String namespace, Iterable<Long> groupIds) throws IOException {
    // Delete the entry table
    dropTable(getTableId(streamId, namespace));

    // Cleanup state store
    Map<Long, Integer> groupInfo = Maps.newHashMap();
    for (Long groupId : groupIds) {
      groupInfo.put(groupId, 0);
    }
    try {
      streamAdmin.configureGroups(streamId, groupInfo);
    } catch (Exception e) {
      Throwables.propagateIfPossible(e, IOException.class);
      throw new IOException(e);
    }

  }

  private TableId getTableId(Id.Stream streamId, String namespace) {
    return TableId.from(streamId.getNamespace(), String.format("%s.%s.%s", tablePrefix, streamId.getName(), namespace));
  }

  private MultiLiveStreamFileReader createReader(final StreamConfig streamConfig,
                                                 StreamConsumerState consumerState) throws IOException {
    Location streamLocation = streamConfig.getLocation();
    Preconditions.checkNotNull(streamLocation, "Stream location is null for %s", streamConfig.getStreamId());

    // Look for the latest stream generation
    final int generation = StreamUtils.getGeneration(streamConfig);
    streamLocation = StreamUtils.createGenerationLocation(streamLocation, generation);

    final long currentTime = System.currentTimeMillis();

    if (!Iterables.isEmpty(consumerState.getState())) {
      // See if any offset has a different generation or is expired. If so, don't use the old states.
      boolean useStoredStates = Iterables.all(consumerState.getState(), new Predicate<StreamFileOffset>() {
        @Override
        public boolean apply(StreamFileOffset input) {
          boolean isExpired = input.getPartitionEnd() < currentTime - streamConfig.getTTL();
          boolean sameGeneration = generation == input.getGeneration();
          return !isExpired && sameGeneration;
        }
      });

      if (useStoredStates) {
        LOG.info("Create file reader with consumer state: {}", consumerState);
        // Has existing offsets, just resume from there.
        MultiLiveStreamFileReader reader = new MultiLiveStreamFileReader(streamConfig, consumerState.getState());
        reader.initialize();
        return reader;
      }
    }

    // TODO: Support starting from some time rather then from beginning.
    // Otherwise, search for files with the smallest partition start time
    // If no partition exists for the stream, start with one partition earlier than current time to make sure
    // no event will be lost if events start flowing in about the same time.
    long startTime = StreamUtils.getPartitionStartTime(currentTime - streamConfig.getPartitionDuration(),
                                                       streamConfig.getPartitionDuration());
    long earliestNonExpiredTime = StreamUtils.getPartitionStartTime(currentTime - streamConfig.getTTL(),
                                                                    streamConfig.getPartitionDuration());

    for (Location partitionLocation : streamLocation.list()) {
      if (!partitionLocation.isDirectory() || !StreamUtils.isPartition(partitionLocation.getName())) {
        // Partition should be a directory
        continue;
      }

      long partitionStartTime = StreamUtils.getPartitionStartTime(partitionLocation.getName());
      boolean isPartitionExpired = partitionStartTime < earliestNonExpiredTime;
      if (!isPartitionExpired && partitionStartTime < startTime) {
        startTime = partitionStartTime;
      }
    }

    // Create file offsets
    // TODO: Be able to support dynamic name of stream writer instances.
    // Maybe it's done through MultiLiveStreamHandler to alter list of file offsets dynamically
    Location partitionLocation = StreamUtils.createPartitionLocation(streamLocation,
                                                                     startTime, streamConfig.getPartitionDuration());
    List<StreamFileOffset> fileOffsets = Lists.newArrayList();
    getFileOffsets(partitionLocation, fileOffsets, generation);

    LOG.info("Empty consumer state. Create file reader with file offsets: groupId={}, instanceId={} states={}",
             consumerState.getGroupId(), consumerState.getInstanceId(), fileOffsets);

    MultiLiveStreamFileReader reader = new MultiLiveStreamFileReader(streamConfig, fileOffsets);
    reader.initialize();
    return reader;
  }
}

/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.stream;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.io.Locations;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data.stream.StreamCoordinator;
import com.continuuity.data.stream.StreamFileOffset;
import com.continuuity.data.stream.StreamUtils;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.CharStreams;
import com.google.common.io.Closeables;
import com.google.gson.Gson;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * An abstract base {@link StreamAdmin} for File based stream.
 */
public abstract class AbstractStreamFileAdmin implements StreamAdmin {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractStreamFileAdmin.class);
  private static final String CONFIG_FILE_NAME = "config.json";
  private static final Gson GSON = new Gson();

  private final Location streamBaseLocation;
  private final StreamCoordinator streamCoordinator;
  private final CConfiguration cConf;
  private final StreamConsumerStateStoreFactory stateStoreFactory;

  // This is just for compatibility upgrade from pre 2.2.0 to 2.2.0.
  // TODO: Remove usage of this when no longer needed
  private final StreamAdmin oldStreamAdmin;

  protected AbstractStreamFileAdmin(LocationFactory locationFactory, CConfiguration cConf,
                                    StreamCoordinator streamCoordinator,
                                    StreamConsumerStateStoreFactory stateStoreFactory,
                                    StreamAdmin oldStreamAdmin) {
    this.cConf = cConf;
    this.streamBaseLocation = locationFactory.create(cConf.get(Constants.Stream.BASE_DIR));
    this.streamCoordinator = streamCoordinator;
    this.stateStoreFactory = stateStoreFactory;
    this.oldStreamAdmin = oldStreamAdmin;
  }

  @Override
  public void dropAll() throws Exception {
    // TODO: How to support it properly with opened stream writer?
  }

  @Override
  public void configureInstances(QueueName name, long groupId, int instances) throws Exception {
    Preconditions.checkArgument(name.isStream(), "The {} is not stream.", name);
    Preconditions.checkArgument(instances > 0, "Number of consumer instances must be > 0.");

    LOG.info("Configure instances: {} {}", groupId, instances);

    StreamConfig config = StreamUtils.ensureExists(this, name.getSimpleName());
    StreamConsumerStateStore stateStore = stateStoreFactory.create(config);
    try {
      Set<StreamConsumerState> states = Sets.newHashSet();
      stateStore.getByGroup(groupId, states);

      Set<StreamConsumerState> newStates = Sets.newHashSet();
      Set<StreamConsumerState> removeStates = Sets.newHashSet();
      mutateStates(groupId, instances, states, newStates, removeStates);

      // Save the states back
      if (!newStates.isEmpty()) {
        stateStore.save(newStates);
        LOG.info("Configure instances new states: {} {} {}", groupId, instances, newStates);
      }
      if (!removeStates.isEmpty()) {
        stateStore.remove(removeStates);
        LOG.info("Configure instances remove states: {} {} {}", groupId, instances, removeStates);
      }

    } finally {
      stateStore.close();
    }

    // Also configure the old stream if it exists
    if (oldStreamAdmin.exists(name.toURI().toString())) {
      oldStreamAdmin.configureInstances(name, groupId, instances);
    }
  }

  @Override
  public void configureGroups(QueueName name, Map<Long, Integer> groupInfo) throws Exception {
    Preconditions.checkArgument(name.isStream(), "The {} is not stream.", name);
    Preconditions.checkArgument(!groupInfo.isEmpty(), "Consumer group information must not be empty.");

    LOG.info("Configure groups for {}: {}", name, groupInfo);

    StreamConfig config = StreamUtils.ensureExists(this, name.getSimpleName());
    StreamConsumerStateStore stateStore = stateStoreFactory.create(config);
    try {
      Set<StreamConsumerState> states = Sets.newHashSet();
      stateStore.getAll(states);

      // Remove all groups that are no longer exists. The offset information in that group can be discarded.
      Set<StreamConsumerState> removeStates = Sets.newHashSet();
      for (StreamConsumerState state : states) {
        if (!groupInfo.containsKey(state.getGroupId())) {
          removeStates.add(state);
        }
      }

      // For each groups, compute the new file offsets if needed
      Set<StreamConsumerState> newStates = Sets.newHashSet();
      for (Map.Entry<Long, Integer> entry : groupInfo.entrySet()) {
        final long groupId = entry.getKey();

        // Create a view of old states which match with the current groupId only.
        mutateStates(groupId, entry.getValue(), Sets.filter(states, new Predicate<StreamConsumerState>() {
          @Override
          public boolean apply(StreamConsumerState state) {
            return state.getGroupId() == groupId;
          }
        }), newStates, removeStates);
      }

      // Save the states back
      if (!newStates.isEmpty()) {
        stateStore.save(newStates);
        LOG.info("Configure groups new states: {} {}", groupInfo, newStates);
      }
      if (!removeStates.isEmpty()) {
        stateStore.remove(removeStates);
        LOG.info("Configure groups remove states: {} {}", groupInfo, removeStates);
      }

    } finally {
      stateStore.close();
    }

    // Also configure the old stream if it exists
    if (oldStreamAdmin.exists(name.toURI().toString())) {
      oldStreamAdmin.configureGroups(name, groupInfo);
    }
  }

  @Override
  public void upgrade() throws Exception {
    // No-op
    oldStreamAdmin.upgrade();
  }

  @Override
  public StreamConfig getConfig(String streamName) throws IOException {
    Location streamLocation = streamBaseLocation.append(streamName);
    Preconditions.checkArgument(streamLocation.isDirectory(), "Stream '%s' not exists.", streamName);

    Location configLocation = streamLocation.append(CONFIG_FILE_NAME);
    Reader reader = new InputStreamReader(configLocation.getInputStream(), Charsets.UTF_8);
    try {
      StreamConfig config = GSON.fromJson(reader, StreamConfig.class);
      return new StreamConfig(streamName, config.getPartitionDuration(), config.getIndexInterval(),
                              config.getTTL(), streamLocation);
    } finally {
      Closeables.closeQuietly(reader);
    }
  }

  @Override
  public void updateConfig(String streamName, StreamConfig config) throws IOException {
    Location streamLocation = streamBaseLocation.append(streamName);
    Preconditions.checkArgument(streamLocation.isDirectory(), "Stream '{}' not exists.", streamName);

    StreamConfig originalConfig = getConfig(streamName);
    Preconditions.checkArgument(isValidConfigUpdate(originalConfig, config),
                                "Configuration update for stream '{}' was not valid (can only update ttl)", streamName);

    Location configLocation = streamLocation.append(CONFIG_FILE_NAME);
    Location tempLocation = configLocation.getTempFile("tmp");
    try {
      CharStreams.write(GSON.toJson(config), CharStreams.newWriterSupplier(
        Locations.newOutputSupplier(tempLocation), Charsets.UTF_8));

      Preconditions.checkState(tempLocation.renameTo(configLocation) != null,
                               "Rename {} to {} failed", tempLocation, configLocation);
    } finally {
      if (tempLocation.exists()) {
        tempLocation.delete();
      }
    }
  }

  @Override
  public boolean exists(String name) throws Exception {
    try {
      return streamBaseLocation.append(name).append(CONFIG_FILE_NAME).exists()
        || oldStreamAdmin.exists(QueueName.fromStream(name).toURI().toString());
    } catch (IOException e) {
      LOG.error("Exception when check for stream exist.", e);
      return false;
    }
  }

  @Override
  public void create(String name) throws Exception {
    create(name, null);
  }

  @Override
  public void create(String name, @Nullable Properties props) throws Exception {
    Location streamLocation = streamBaseLocation.append(name);
    Locations.mkdirsIfNotExists(streamLocation);

    Location configLocation = streamBaseLocation.append(name).append(CONFIG_FILE_NAME);
    if (!configLocation.createNew()) {
      // Stream already exists
      return;
    }

    Properties properties = (props == null) ? new Properties() : props;
    long partitionDuration = Long.parseLong(properties.getProperty(Constants.Stream.PARTITION_DURATION,
                                            cConf.get(Constants.Stream.PARTITION_DURATION)));
    long indexInterval = Long.parseLong(properties.getProperty(Constants.Stream.INDEX_INTERVAL,
                                                               cConf.get(Constants.Stream.INDEX_INTERVAL)));
    long ttl = Long.parseLong(properties.getProperty(Constants.Stream.TTL,
                                                     cConf.get(Constants.Stream.TTL)));

    Location tmpConfigLocation = configLocation.getTempFile(null);
    StreamConfig config = new StreamConfig(name, partitionDuration, indexInterval, ttl, streamLocation);
    CharStreams.write(GSON.toJson(config), CharStreams.newWriterSupplier(
      Locations.newOutputSupplier(tmpConfigLocation), Charsets.UTF_8));
    
    try {
      tmpConfigLocation.renameTo(configLocation);
    } finally {
      if (tmpConfigLocation.exists()) {
        tmpConfigLocation.delete();
      }
    }
  }

  @Override
  public void truncate(String name) throws Exception {
    String streamName = QueueName.fromStream(name).toURI().toString();
    if (oldStreamAdmin.exists(streamName)) {
      oldStreamAdmin.truncate(streamName);
    }

    StreamConfig config = getConfig(name);
    streamCoordinator.nextGeneration(config, StreamUtils.getGeneration(config)).get();
  }

  @Override
  public void drop(String name) throws Exception {
  // TODO: How to support it properly with opened stream writer?
    String streamName = QueueName.fromStream(name).toURI().toString();
    if (oldStreamAdmin.exists(streamName)) {
      oldStreamAdmin.drop(streamName);
    }
  }

  @Override
  public void upgrade(String name, Properties properties) throws Exception {
    String streamName = QueueName.fromStream(name).toURI().toString();
    if (oldStreamAdmin.exists(streamName)) {
      oldStreamAdmin.upgrade(streamName, properties);
    }
  }

  private boolean isValidConfigUpdate(StreamConfig originalConfig, StreamConfig newConfig) {
    return originalConfig.getIndexInterval() == newConfig.getIndexInterval()
      && originalConfig.getPartitionDuration() == newConfig.getPartitionDuration();
  }

  private void mutateStates(long groupId, int instances, Set<StreamConsumerState> states,
                            Set<StreamConsumerState> newStates, Set<StreamConsumerState> removeStates) {
    int oldInstances = states.size();
    if (oldInstances == instances) {
      // If number of instances doesn't changed, no need to mutate any states
      return;
    }

    // Collects smallest offsets across all existing consumers
    // Map from event file location to file offset.
    // Use tree map to maintain ordering consistency in the offsets.
    // Not required by any logic, just easier to look at when logged.
    Map<Location, StreamFileOffset> fileOffsets = Maps.newTreeMap(Locations.LOCATION_COMPARATOR);

    for (StreamConsumerState state : states) {
      for (StreamFileOffset fileOffset : state.getState()) {
        StreamFileOffset smallestOffset = fileOffsets.get(fileOffset.getEventLocation());
        if (smallestOffset == null || fileOffset.getOffset() < smallestOffset.getOffset()) {
          fileOffsets.put(fileOffset.getEventLocation(), new StreamFileOffset(fileOffset));
        }
      }
    }

    // Constructs smallest offsets
    Collection<StreamFileOffset> smallestOffsets = fileOffsets.values();

    // When group size changed, reset all existing instances states to have smallest files offsets constructed above.
    for (StreamConsumerState state : states) {
      if (state.getInstanceId() < instances) {
        // Only keep valid instances
        newStates.add(new StreamConsumerState(groupId, state.getInstanceId(), smallestOffsets));
      } else {
        removeStates.add(state);
      }
    }

    // For all new instances, set files offsets to smallest one constructed above.
    for (int i = oldInstances; i < instances; i++) {
      newStates.add(new StreamConsumerState(groupId, i, smallestOffsets));
    }
  }
}

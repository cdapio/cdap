/*
 * Copyright © 2014 Cask Data, Inc.
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

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.common.utils.OSDetector;
import co.cask.cdap.data.stream.StreamCoordinatorClient;
import co.cask.cdap.data.stream.StreamFileOffset;
import co.cask.cdap.data.stream.StreamUtils;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.cdap.notifications.feeds.NotificationFeed;
import co.cask.cdap.notifications.feeds.NotificationFeedException;
import co.cask.cdap.notifications.feeds.NotificationFeedManager;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.CharStreams;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * An abstract base {@link StreamAdmin} for File based stream.
 */
public abstract class AbstractStreamFileAdmin implements StreamAdmin {

  public static final String CONFIG_FILE_NAME = "config.json";

  private static final Logger LOG = LoggerFactory.getLogger(AbstractStreamFileAdmin.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();

  private final Location streamBaseLocation;
  private final StreamCoordinatorClient streamCoordinatorClient;
  private final CConfiguration cConf;
  private final StreamConsumerStateStoreFactory stateStoreFactory;
  private final NotificationFeedManager notificationFeedManager;

  // This is just for compatibility upgrade from pre 2.2.0 to 2.2.0.
  // TODO: Remove usage of this when no longer needed
  private final StreamAdmin oldStreamAdmin;

  protected AbstractStreamFileAdmin(LocationFactory locationFactory, CConfiguration cConf,
                                    StreamCoordinatorClient streamCoordinatorClient,
                                    StreamConsumerStateStoreFactory stateStoreFactory,
                                    NotificationFeedManager notificationFeedManager,
                                    StreamAdmin oldStreamAdmin) {
    this.cConf = cConf;
    this.notificationFeedManager = notificationFeedManager;
    this.streamBaseLocation = locationFactory.create(cConf.get(Constants.Stream.BASE_DIR));
    this.streamCoordinatorClient = streamCoordinatorClient;
    this.stateStoreFactory = stateStoreFactory;
    this.oldStreamAdmin = oldStreamAdmin;
  }

  @Override
  public void dropAll() throws Exception {
    try {
      oldStreamAdmin.dropAll();
    } catch (Exception e) {
      LOG.error("Failed to to truncate old stream.", e);
    }

    // Simply increment the generation of all streams. The actual deletion of file, just like truncate case,
    // is done external to this class.
    List<Location> locations;
    try {
      locations = streamBaseLocation.list();
    } catch (FileNotFoundException e) {
      // If the stream base doesn't exists, nothing need to be deleted
      locations = ImmutableList.of();
    }

    for (Location streamLocation : locations) {
      try {
        StreamConfig streamConfig = loadConfig(streamLocation);
        streamCoordinatorClient.nextGeneration(streamConfig, StreamUtils.getGeneration(streamConfig)).get();
      } catch (Exception e) {
        LOG.error("Failed to truncate stream {}", streamLocation.getName(), e);
      }
    }

    // Also drop the state table
    stateStoreFactory.dropAll();
  }

  @Override
  public void configureInstances(QueueName name, long groupId, int instances) throws Exception {
    Preconditions.checkArgument(name.isStream(), "%s is not a stream.", name);
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
    Preconditions.checkArgument(name.isStream(), "%s is not a stream.", name);
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
    Preconditions.checkArgument(streamLocation.isDirectory(), "Stream '%s' does not exist.", streamName);
    return loadConfig(streamLocation);
  }

  @Override
  public void updateConfig(StreamConfig config) throws IOException {
    Location streamLocation = config.getLocation();
    Preconditions.checkArgument(streamLocation.isDirectory(), "Stream '%s' does not exist.", config.getName());

    // Check only TTL or format is changed, as only TTL or format changes are supported.
    StreamConfig originalConfig = loadConfig(streamLocation);
    Preconditions.checkArgument(isValidConfigUpdate(originalConfig, config),
                                "Configuration update for stream '%s' was not valid (can only update ttl or format)",
                                config.getName());

    if (originalConfig.getTTL() != config.getTTL()) {
      streamCoordinatorClient.changeTTL(originalConfig, config.getTTL());
    }
    if (!originalConfig.getFormat().equals(config.getFormat())) {
      saveConfig(config);
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

    StreamConfig config = new StreamConfig(name, partitionDuration, indexInterval, ttl, streamLocation, null);
    saveConfig(config);

    // Create the notification feeds linked to that stream
    createStreamFeeds(name);

    streamCoordinatorClient.streamCreated(name);
  }

  private void createStreamFeeds(String stream) {
    // TODO use accountID as namespace?
    try {
      NotificationFeed streamFeed = new NotificationFeed.Builder()
        .setNamespace(Constants.DEFAULT_NAMESPACE)
        .setCategory(Constants.Notification.Stream.STREAM_FEED_CATEGORY)
        .setName(stream)
        .setDescription(String.format("Size updates feed for Stream %s every %dMB",
                                      stream, Constants.Notification.Stream.DEFAULT_DATA_THRESHOLD))
        .build();
      notificationFeedManager.createFeed(streamFeed);
    } catch (NotificationFeedException e) {
      LOG.error("Cannot create feed for Stream {}", stream, e);
    }
  }

  @Override
  public void truncate(String name) throws Exception {
    String streamName = QueueName.fromStream(name).toURI().toString();
    if (oldStreamAdmin.exists(streamName)) {
      oldStreamAdmin.truncate(streamName);
    }

    StreamConfig config = getConfig(name);
    streamCoordinatorClient.nextGeneration(config, StreamUtils.getGeneration(config)).get();
  }

  @Override
  public void drop(String name) throws Exception {
    // Same as truncate
    truncate(name);
  }

  @Override
  public void upgrade(String name, Properties properties) throws Exception {
    String streamName = QueueName.fromStream(name).toURI().toString();
    if (oldStreamAdmin.exists(streamName)) {
      oldStreamAdmin.upgrade(streamName, properties);
    }
  }

  private void saveConfig(StreamConfig config) throws IOException {
    Location configLocation = streamBaseLocation.append(config.getName()).append(CONFIG_FILE_NAME);
    Location tmpConfigLocation = configLocation.getTempFile(null);
    CharStreams.write(GSON.toJson(config), CharStreams.newWriterSupplier(
      Locations.newOutputSupplier(tmpConfigLocation), Charsets.UTF_8));

    try {
      // Windows does not allow renaming if the destination file exists so we must delete the configLocation
      if (OSDetector.isWindows()) {
        configLocation.delete();
      }
      tmpConfigLocation.renameTo(streamBaseLocation.append(config.getName()).append(CONFIG_FILE_NAME));
    } finally {
      if (tmpConfigLocation.exists()) {
        tmpConfigLocation.delete();
      }
    }
  }

  private StreamConfig loadConfig(Location streamLocation) throws IOException {
    Location configLocation = streamLocation.append(CONFIG_FILE_NAME);

    StreamConfig config = GSON.fromJson(
      CharStreams.toString(CharStreams.newReaderSupplier(Locations.newInputSupplier(configLocation), Charsets.UTF_8)),
      StreamConfig.class);

    return new StreamConfig(streamLocation.getName(), config.getPartitionDuration(), config.getIndexInterval(),
                            config.getTTL(), streamLocation, config.getFormat());
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

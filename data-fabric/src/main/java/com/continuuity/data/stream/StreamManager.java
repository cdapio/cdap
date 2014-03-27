/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.google.common.base.Charsets;
import com.google.common.io.Closeables;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;

/**
 *
 */
public final class StreamManager {

  private static final Logger LOG = LoggerFactory.getLogger(StreamManager.class);
  private static final String CONFIG_FILE_NAME = "config.json";
  private static final Gson GSON = new Gson();

  private final Location streamBaseLocation;
  private final CConfiguration cConf;

  @Inject
  public StreamManager(LocationFactory locationFactory, CConfiguration cConf) {
    this.cConf = cConf;
    this.streamBaseLocation = locationFactory.create(cConf.get(Constants.Stream.BASE_DIR));
  }

  /**
   * Creates a stream with the given name. If the stream already exists, it won't change the existing stream.
   *
   * @param streamName Name of the stream.
   * @return A {@link StreamConfigurator} for setting up configurations of the new stream.
   * @throws IOException if failed to create the stream.
   */
  public StreamConfigurator create(String streamName) throws IOException {
    streamBaseLocation.append(streamName).mkdirs();
    return new StreamConfigurator(createDefaultStreamConfig(streamName), true);
  }

  /**
   * Tells if a stream exists.
   *
   * @param streamName Name of the stream.
   * @return {@code true} if the stream exists, {@code false} otherwise.
   */
  public boolean exists(String streamName) {
    try {
      return streamBaseLocation.append(streamName).isDirectory();
    } catch (IOException e) {
      LOG.info("Exception when check for stream exist.", e);
      return false;
    }
  }

  /**
   * Returns the configuration of the given stream
   *
   * @param streamName Name of the stream.
   * @return A {@link StreamConfig} instance.
   * @throws IOException if the stream doesn't exists.
   */
  public StreamConfig getConfig(String streamName) throws IOException {
    Location streamLocation = streamBaseLocation.append(streamName);
    if (!streamLocation.isDirectory()) {
      throw new IOException("Stream " + streamName + " not exist.");
    }
    try {
      Location configLocation = streamLocation.append(CONFIG_FILE_NAME);
      Reader reader = new InputStreamReader(configLocation.getInputStream(), Charsets.UTF_8);
      try {
        return GSON.fromJson(reader, StreamConfig.class);
      } finally {
        Closeables.closeQuietly(reader);
      }
    } catch (IOException e) {
      LOG.warn("Failed to load configuration for stream {}.", streamName, e);
      return createDefaultStreamConfig(streamName);
    }
  }

  /**
   * Changes the configuration of the given stream.
   *
   * @param streamName Name of the stream.
   * @return A {@link StreamConfigurator} for changing configurations.
   * @throws IOException if the stream doesn't exists.
   */
  public StreamConfigurator configure(String streamName) throws IOException {
    return new StreamConfigurator(getConfig(streamName), false);
  }


  /**
   * Deletes all data and configurations associated with the given stream.
   *
   * @param streamName Name of the stream.
   * @throws IOException If failed to delete the stream.
   */
  public void drop(String streamName) throws IOException {
    streamBaseLocation.append(streamName).delete(true);
  }

  /**
   * Deletes stream data for all partitions that falls in the given time range. This means for partitions that has
   *
   * <pre>
   * partitionStartTime >= startTime && (partitionStartTime + partitionDuration) <= endTime
   * </pre>
   *
   * will get removed.
   *
   * @param streamName Name of the stream.
   * @param startTime Start timestamp in milliseconds.
   * @param endTime End timestamp in milliseconds.
   * @throws IOException If failed to delete partitions.
   */
  public void deletePartitions(String streamName, long startTime, long endTime) throws IOException {
    Location streamLocation = streamBaseLocation.append(streamName);

    for (Location partitionLocation : streamLocation.list()) {
      if (!partitionLocation.isDirectory()) {
        // Partition has to be a directory
        continue;
      }

      long partitionStartTime = StreamUtils.getPartitionStartTime(partitionLocation.getName());
      long partitionEndTime = StreamUtils.getPartitionEndTime(partitionLocation.getName());

      if (partitionStartTime >= startTime && partitionEndTime <= endTime) {
        partitionLocation.delete(true);
      }
    }
  }

  private StreamConfig createDefaultStreamConfig(String streamName) {
    return new StreamConfig(streamName,
                            cConf.getLong(Constants.Stream.PARTITION_DURATION,
                                          Constants.Stream.DEFAULT_PARTITION_DURATION),
                            cConf.getLong(Constants.Stream.INDEX_INTERVAL,
                                          Constants.Stream.DEFAULT_INDEX_INTERVAL));
  }


  /**
   * This class is for changing stream configurations.
   */
  public final class StreamConfigurator {

    private final String streamName;
    private long partitionDuration;
    private long indexInterval;
    private boolean createOnly;

    /**
     * Constructor only called by {@link StreamManager}.
     */
    private StreamConfigurator(StreamConfig config, boolean createOnly) {
      this.streamName = config.getName();
      this.partitionDuration = config.getPartitionDuration();
      this.indexInterval = config.getIndexInterval();
      this.createOnly = createOnly;
    }

    /**
     * Sets the partition duration.
     *
     * @param duration New partition duration in milliseconds.
     * @return this {@link StreamConfigurator}.
     */
    public StreamConfigurator setPartitionDuration(long duration) {
      this.partitionDuration = duration;
      return this;
    }

    /**
     * Sets the index interval.
     *
     * @param interval New index interval in milliseconds.
     * @return this {@link StreamConfigurator}.
     */
    public StreamConfigurator setIndexInterval(long interval) {
      this.indexInterval = interval;
      return this;
    }

    /**
     * Applies the changes.
     *
     * @throws IOException If failed to apply the changes.
     */
    public void apply() throws IOException {
      Location configLocation = streamBaseLocation.append(streamName).append(CONFIG_FILE_NAME);
      if (createOnly && !configLocation.createNew()) {
        LOG.info("Ignore configuration for stream {} already exists while request for create only.", streamName);
        return;
      }

      Location tmpConfigLocation = configLocation.getTempFile(null);
      StreamConfig config = new StreamConfig(streamName, partitionDuration, indexInterval);

      Writer writer = new OutputStreamWriter(tmpConfigLocation.getOutputStream(), Charsets.UTF_8);
      try {
        GSON.toJson(config, writer);
      } finally {
        writer.close();
      }
      tmpConfigLocation.renameTo(configLocation);
    }
  }
}

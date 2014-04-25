/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.stream;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.queue.QueueName;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
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
import java.util.Map;
import java.util.Properties;
import javax.annotation.Nullable;

/**
 * An abstract base {@link StreamAdmin} for File based stream.
 */
public abstract class AbstractFileStreamAdmin implements StreamAdmin {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractFileStreamAdmin.class);
  private static final String CONFIG_FILE_NAME = "config.json";
  private static final Gson GSON = new Gson();

  private final Location streamBaseLocation;
  private final CConfiguration cConf;

  protected AbstractFileStreamAdmin(LocationFactory locationFactory, CConfiguration cConf) {
    this.cConf = cConf;
    this.streamBaseLocation = locationFactory.create(cConf.get(Constants.Stream.BASE_DIR));
  }

  @Override
  public void dropAll() throws Exception {
    // TODO: How to support it properly with opened stream writer?
  }

  @Override
  public void configureInstances(QueueName streamName, long groupId, int instances) throws Exception {
    // TODO:
  }

  @Override
  public void configureGroups(QueueName streamName, Map<Long, Integer> groupInfo) throws Exception {
    // TODO:
  }

  @Override
  public void upgrade() throws Exception {
    // No-op
  }

  @Override
  public StreamConfig getConfig(String streamName) throws IOException {
    Location streamLocation = streamBaseLocation.append(streamName);
    Preconditions.checkArgument(streamLocation.isDirectory(), "Stream '{}' not exists.", streamName);

    Location configLocation = streamLocation.append(CONFIG_FILE_NAME);
    Reader reader = new InputStreamReader(configLocation.getInputStream(), Charsets.UTF_8);
    try {
      StreamConfig config = GSON.fromJson(reader, StreamConfig.class);
      return new StreamConfig(streamName, config.getPartitionDuration(), config.getIndexInterval(), streamLocation);
    } finally {
      Closeables.closeQuietly(reader);
    }
  }

  @Override
  public boolean exists(String name) throws Exception {
    try {
      return streamBaseLocation.append(name).append(CONFIG_FILE_NAME).exists();
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
    if (!streamLocation.mkdirs() && !streamLocation.isDirectory()) {
      throw new IllegalStateException("Failed to create stream '" + name + "' at " + streamLocation.toURI());
    }

    Location configLocation = streamBaseLocation.append(name).append(CONFIG_FILE_NAME);
    if (!configLocation.createNew()) {
      throw new IllegalStateException("Failed to create config for stream '" + name + "' at " + configLocation.toURI());
    }

    Properties properties = (props == null) ? new Properties() : props;
    long partitionDuration = Long.parseLong(properties.getProperty(Constants.Stream.PARTITION_DURATION,
                                            cConf.get(Constants.Stream.PARTITION_DURATION)));
    long indexInterval = Long.parseLong(properties.getProperty(Constants.Stream.INDEX_INTERVAL,
                                                               cConf.get(Constants.Stream.INDEX_INTERVAL)));

    Location tmpConfigLocation = configLocation.getTempFile(null);
    StreamConfig config = new StreamConfig(name, partitionDuration, indexInterval, streamLocation);
    Writer writer = new OutputStreamWriter(tmpConfigLocation.getOutputStream(), Charsets.UTF_8);
    try {
      GSON.toJson(config, writer);
    } finally {
      writer.close();
    }
    tmpConfigLocation.renameTo(configLocation);
  }

  @Override
  public void truncate(String name) throws Exception {
    // TODO: How to support it properly with opened stream writer?
  }

  @Override
  public void drop(String name) throws Exception {
  // TODO: How to support it properly with opened stream writer?
  }

  @Override
  public void upgrade(String name, Properties properties) throws Exception {
    // No-op
  }
}

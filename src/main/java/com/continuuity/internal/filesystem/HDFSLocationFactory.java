/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.filesystem;

import com.continuuity.filesystem.Location;
import com.continuuity.filesystem.LocationFactory;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.net.URI;

/**
 * A concrete implementation of {@link LocalLocationFactory} for HDFS.
 */
public final class HDFSLocationFactory implements LocationFactory {
  /**
   * Hadoop configuration.
   */
  private final Configuration configuration;

  /**
   * Hadoop filesystem.
   */
  private final FileSystem filesystem;

  /**
   * Factory constructor for HDFS.
   *
   * @param configuration
   * @throws IOException
   */
  @Inject
  public HDFSLocationFactory(Configuration configuration) throws IOException {
    this.configuration = configuration;
    this.filesystem = FileSystem.get(configuration);
  }

  /**
   * Creates an instance of {@link Location} based on string <code>path</code>
   *
   * @param path to the resource on the hdfs filesystem.
   * @return A instance of {@link Location}
   */
  @Override
  public Location create(String path) {
    return new HDFSLocation(filesystem, path);
  }

  /**
   * Creates an instance of {@link Location} based on {@link URI} <code>uri</code>
   *
   * @param uri to the resource on the hdfs filesystem.
   * @return A instance of {@link Location}
   */
  @Override
  public Location create(URI uri) {
    return new HDFSLocation(filesystem, uri);
  }
}

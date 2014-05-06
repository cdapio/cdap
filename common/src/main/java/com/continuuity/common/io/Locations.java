/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.io;

import com.google.common.io.InputSupplier;
import com.google.common.io.OutputSupplier;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.twill.filesystem.Location;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Comparator;

/**
 * Utility class to help interaction with {@link Location}.
 */
public final class Locations {

  public static final Comparator<Location> LOCATION_COMPARATOR = new Comparator<Location>() {
    @Override
    public int compare(Location o1, Location o2) {
      return o1.toURI().compareTo(o2.toURI());
    }
  };

  /**
   * Creates a new {@link InputSupplier} that can provides {@link SeekableInputStream} of the given path.
   *
   * @param fs The {@link org.apache.hadoop.fs.FileSystem} for the given path.
   * @param path The path to create {@link com.continuuity.common.io.SeekableInputStream} when requested.
   * @return A {@link InputSupplier}.
   */
  public static InputSupplier<? extends SeekableInputStream> newInputSupplier(final FileSystem fs, final Path path) {
    return new InputSupplier<SeekableInputStream>() {
      @Override
      public SeekableInputStream getInput() throws IOException {
        return SeekableInputStream.create(fs.open(path));
      }
    };
  }

  /**
   * Creates a new {@link InputSupplier} that can provides {@link SeekableInputStream} from the given location.
   *
   * @param location Location for the input stream.
   * @return A {@link InputSupplier}.
   */
  public static InputSupplier<? extends SeekableInputStream> newInputSupplier(final Location location) {
    return new InputSupplier<SeekableInputStream>() {
      @Override
      public SeekableInputStream getInput() throws IOException {
        return SeekableInputStream.create(location.getInputStream());
      }
    };
  }

  /**
   * Creates a new {@link OutputSupplier} that can provides {@link OutputStream} for the given location.
   *
   * @param location Location for the output.
   * @return A {@link OutputSupplier}.
   */
  public static OutputSupplier<? extends OutputStream> newOutputSupplier(final Location location) {
    return new OutputSupplier<OutputStream>() {
      @Override
      public OutputStream getOutput() throws IOException {
        return location.getOutputStream();
      }
    };
  }

  private Locations() {
  }
}

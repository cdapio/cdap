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
package co.cask.cdap.common.io;

import com.google.common.io.InputSupplier;
import com.google.common.io.OutputSupplier;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Comparator;
import javax.annotation.Nullable;

/**
 * Utility class to help interaction with {@link Location}.
 */
public final class Locations {

  private static final Logger LOG = LoggerFactory.getLogger(Locations.class);

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
   * @param path The path to create {@link co.cask.cdap.common.io.SeekableInputStream} when requested.
   * @return A {@link InputSupplier}.
   */
  public static InputSupplier<? extends SeekableInputStream> newInputSupplier(final FileSystem fs, final Path path) {
    return new InputSupplier<SeekableInputStream>() {
      @Override
      public SeekableInputStream getInput() throws IOException {
        long size = fs.getFileStatus(path).getLen();
        return SeekableInputStream.create(fs.open(path), size);
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
        long size = location.length();
        return SeekableInputStream.create(location.getInputStream(), size);
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

  /**
   * Creates a {@link Location} instance which represents the parent of the given location.
   *
   * @param location location to extra parent from.
   * @return an instance representing the parent location or {@code null} if there is no parent.
   */
  @Nullable
  public static Location getParent(Location location) {
    URI source = location.toURI();

    // If it is root, return null
    if ("/".equals(source.getPath())) {
      return null;
    }

    URI resolvedParent = URI.create(source.toString() + "/..").normalize();
    return location.getLocationFactory().create(resolvedParent);
  }

  /**
   * Create the directory represented by the location if not exists.
   *
   * @param location the location for the directory.
   * @throws IOException If the location cannot be created
   */
  public static void mkdirsIfNotExists(Location location) throws IOException {
    // Need to check && mkdir && check to deal with race condition
    if (!location.isDirectory() && !location.mkdirs() && !location.isDirectory()) {
      throw new IOException("Failed to create directory at " + location.toURI());
    }
  }

  public static void deleteQuietly(Location location) {
    deleteQuietly(location, false);
  }

  public static void deleteQuietly(Location location, boolean recursive) {
    try {
      location.delete(recursive);
    } catch (IOException e) {
      LOG.error("IOException while deleting location {}", location.toURI(), e);
    }
  }

  private Locations() {
  }
}

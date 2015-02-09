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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.google.common.io.InputSupplier;
import com.google.common.io.OutputSupplier;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.twill.filesystem.HDFSLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
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
        FSDataInputStream input = fs.open(path);
        try {
          return new DFSSeekableInputStream(input, createDFSStreamSizeProvider(fs, path, input));
        } catch (Throwable t) {
          Closeables.closeQuietly(input);
          Throwables.propagateIfInstanceOf(t, IOException.class);
          throw new IOException(t);
        }
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
        InputStream input = location.getInputStream();
        try {
          if (input instanceof FileInputStream) {
            return new FileSeekableInputStream((FileInputStream) input);
          }
          if (input instanceof FSDataInputStream) {
            FSDataInputStream dataInput = (FSDataInputStream) input;
            LocationFactory locationFactory = location.getLocationFactory();

            // It should be HDFSLocationFactory
            if (locationFactory instanceof HDFSLocationFactory) {
              FileSystem fs = ((HDFSLocationFactory) locationFactory).getFileSystem();
              return new DFSSeekableInputStream(dataInput,
                                                createDFSStreamSizeProvider(fs, new Path(location.toURI()), dataInput));
            } else {
              // This shouldn't happen
              return new DFSSeekableInputStream(dataInput, new StreamSizeProvider() {
                @Override
                public long size() throws IOException {
                  // Assumption is if the FS is not a HDFS fs, the location length tells the stream size
                  return location.length();
                }
              });
            }
          }

          throw new IOException("Failed to create SeekableInputStream from location " + location.toURI());
        } catch (Throwable t) {
          Closeables.closeQuietly(input);
          Throwables.propagateIfInstanceOf(t, IOException.class);
          throw new IOException(t);
        }
      }
    };
  }

  /**
   * Do some processing on the locations contained in the {@code startLocation}, using the {@code processor}. If this
   * location is a directory, all the locations contained in it will also be processed. If the {@code recursive} tag
   * is set to true, those locations that are directories will also be processed recursively. If the
   * {@code startLocation} is not a directory, this method will return the result of the processing of that location.
   *
   * @param startLocation location to start the processing from
   * @param recursive true if this method should be called on the directory {@link Location}s found from
   *                  {@code startLocation}
   * @param processor used to process locations. If the {@link Processor#process} method returns false on any
   *                  {@link Location} object processed, this method will return the current result of the processor.
   * @param <R> Type of the return value
   * @throws IOException if the locations could not be read
   */
  public static <R> R processLocations(Location startLocation, boolean recursive,
                                       Processor<LocationStatus, R> processor) throws IOException {
    boolean process = processor.process(new LocationStatus(startLocation.toURI(), startLocation.length(),
                                                           startLocation.isDirectory()));
    if (!process || !startLocation.isDirectory()) {
      return processor.getResult();
    }

    LocationFactory locationFactory = startLocation.getLocationFactory();
    if (locationFactory instanceof HDFSLocationFactory) {
      // Treat the HDFS case
      FileSystem fs = ((HDFSLocationFactory) locationFactory).getFileSystem();
      Deque<Path> pathStack = Lists.newLinkedList();
      pathStack.push(new Path(startLocation.toURI()));
      while (!pathStack.isEmpty()) {
        Path currentPath = pathStack.poll();
        FileStatus[] statuses = fs.listStatus(currentPath);

        for (FileStatus status : statuses) {
          process = processor.process(new LocationStatus(status.getPath().toUri(), status.getLen(),
                                                         status.isDirectory()));
          if (!process) {
            return processor.getResult();
          }
          if (recursive && status.isDirectory()) {
            pathStack.push(status.getPath());
          }
        }
      }
    } else {
      // Treat the local FS case, we can directly use the Location class APIs
      Deque<Location> locationStack = Lists.newLinkedList();
      locationStack.push(startLocation);
      while (!locationStack.isEmpty()) {
        Location currentLocation = locationStack.poll();
        List<Location> locations = currentLocation.list();
        for (Location location : locations) {
          process = processor.process(new LocationStatus(location.toURI(), location.length(), location.isDirectory()));
          if (!process) {
            return processor.getResult();
          }
          if (recursive && location.isDirectory()) {
            locationStack.push(location);
          }
        }
      }
    }
    return processor.getResult();
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

  /**
   * Creates a {@link StreamSizeProvider} for determining the size of the given {@link FSDataInputStream}.
   */
  private static StreamSizeProvider createDFSStreamSizeProvider(final FileSystem fs,
                                                                final Path path, FSDataInputStream input) {
    // This is the default provider to use. It will try to determine if the file is closed and return the size of it.
    final StreamSizeProvider defaultSizeProvider = new StreamSizeProvider() {
      @Override
      public long size() throws IOException {
        if (fs instanceof DistributedFileSystem) {
          if (((DistributedFileSystem) fs).isFileClosed(path)) {
            return fs.getFileStatus(path).getLen();
          } else {
            return -1L;
          }
        }
        // If the the underlying file system is not DistributedFileSystem, just assume the file length tells the size
        return fs.getFileStatus(path).getLen();
      }
    };

    // This supplier is to abstract out the logic for getting the DFSInputStream#getFileLength method using reflection
    // Reflection is used to avoid ClassLoading error if the DFSInputStream class is moved or method get renamed
    final InputStream wrappedStream = input.getWrappedStream();
    final Supplier<Method> getFileLengthMethodSupplier = Suppliers.memoize(new Supplier<Method>() {
      @Override
      public Method get() {
        try {
          // This is a hack to get to the underlying DFSInputStream
          // Need to revisit it when need to support different distributed file system
          Class<? extends InputStream> cls = wrappedStream.getClass();
          String expectedName = "org.apache.hadoop.hdfs.DFSInputStream";
          if (!cls.getName().equals(expectedName)) {
            throw new Exception("Expected wrapper class be " + expectedName + ", but got " + cls.getName());
          }

          Method getFileLengthMethod = cls.getMethod("getFileLength");
          if (!getFileLengthMethod.isAccessible()) {
            getFileLengthMethod.setAccessible(true);
          }
          return getFileLengthMethod;
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    });

    return new StreamSizeProvider() {
      @Override
      public long size() throws IOException {
        // Try to determine the size using default provider
        long size = defaultSizeProvider.size();
        if (size >= 0) {
          return size;
        }
        try {
          // If not able to get length from the default provider, use the DFSInputStream#getFileLength method
          return (Long) getFileLengthMethodSupplier.get().invoke(wrappedStream);
        } catch (Throwable t) {
          LOG.warn("Unable to get actual file length from DFS input.", t);
          return size;
        }
      }
    };
  }

  private Locations() {
  }

}

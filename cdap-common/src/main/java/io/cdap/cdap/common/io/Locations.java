/*
 * Copyright © 2014-2017 Cask Data, Inc.
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
package io.cdap.cdap.common.io;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.common.io.InputSupplier;
import com.google.common.io.OutputSupplier;
import io.cdap.cdap.common.lang.FunctionWithException;
import io.cdap.cdap.common.lang.jar.BundleJarUtil;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.common.utils.FileUtils;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.twill.filesystem.FileContextLocationFactory;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.PrivilegedExceptionAction;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.zip.GZIPInputStream;
import javax.annotation.Nullable;

/**
 * Utility class to help interaction with {@link Location}.
 */
public final class Locations {

  private static final Logger LOG = LoggerFactory.getLogger(Locations.class);

  // For converting local file into Location.
  private static final LocalLocationFactory LOCAL_LOCATION_FACTORY = new LocalLocationFactory();
  // For converting FileStatus to LocationStatus
  private static final FunctionWithException<FileStatus, LocationStatus, IOException> FILE_STATUS_TO_LOCATION_STATUS =
    new FunctionWithException<FileStatus, LocationStatus, IOException>() {
      @Override
      public LocationStatus apply(FileStatus status) throws IOException {
        return new LocationStatus(status.getPath().toUri(), status.getLen(),
                                  status.isDirectory(), status.getModificationTime());
      }
    };
  // For converting Location to LocationStatus
  private static final FunctionWithException<Location, LocationStatus, IOException> LOCATION_TO_LOCATION_STATUS =
    new FunctionWithException<Location, LocationStatus, IOException>() {
      @Override
      public LocationStatus apply(Location location) throws IOException {
        return new LocationStatus(location.toURI(), location.length(), location.isDirectory(),
                                  location.lastModified());
      }
    };

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
   * @param path The path to create {@link io.cdap.cdap.common.io.SeekableInputStream} when requested.
   * @return A {@link InputSupplier}.
   */
  public static InputSupplier<? extends SeekableInputStream> newInputSupplier(final FileSystem fs, final Path path) {
    return new InputSupplier<SeekableInputStream>() {
      @Override
      public SeekableInputStream getInput() throws IOException {
        FSDataInputStream input = fs.open(path);
        try {
          return new DFSSeekableInputStream(input, createDFSStreamSizeProvider(fs, false, path, input));
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
            final FSDataInputStream dataInput = (FSDataInputStream) input;
            LocationFactory locationFactory = location.getLocationFactory();

            if (locationFactory instanceof FileContextLocationFactory) {
              final FileContextLocationFactory lf = (FileContextLocationFactory) locationFactory;
              return lf.getFileContext().getUgi().doAs(new PrivilegedExceptionAction<SeekableInputStream>() {
                @Override
                public SeekableInputStream run() throws IOException {
                  // Disable the FileSystem cache. The FileSystem will be closed when the InputStream is closed
                  String scheme = lf.getHomeLocation().toURI().getScheme();
                  Configuration hConf = new Configuration(lf.getConfiguration());
                  hConf.set(String.format("fs.%s.impl.disable.cache", scheme), "true");
                  FileSystem fs = FileSystem.get(hConf);
                  return new DFSSeekableInputStream(dataInput,
                                                    createDFSStreamSizeProvider(fs, true,
                                                                                new Path(location.toURI()), dataInput));
                }
              });
            }

            // This shouldn't happen
            // Assumption is if the FS is not a HDFS fs, the location length tells the stream size
            return new DFSSeekableInputStream(dataInput, location::length);
          }

          throw new IOException("Failed to create SeekableInputStream from location " + location);
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
   * @param recursive {@code true} if this method should be called on the directory {@link Location}s found from
   *                  {@code startLocation}. If the {@code startLocation} is a directory, all the locations under it
   *                  will be processed, regardless of the value of {@code recursive}
   * @param processor used to process locations. If the {@link Processor#process} method returns false on any
   *                  {@link Location} object processed, this method will return the current result of the processor.
   * @param <R> Type of the return value
   * @throws IOException if the locations could not be read
   */
  public static <R> R processLocations(Location startLocation, boolean recursive,
                                       Processor<LocationStatus, R> processor) throws IOException {
    boolean topLevel = true;
    LocationFactory lf = startLocation.getLocationFactory();
    LinkedList<LocationStatus> statusStack = new LinkedList<>();
    statusStack.push(getLocationStatus(startLocation));
    while (!statusStack.isEmpty()) {
      LocationStatus status = statusStack.poll();
      if (!processor.process(status)) {
        return processor.getResult();
      }
      if (status.isDir() && (topLevel || recursive)) {
        topLevel = false;
        RemoteIterator<LocationStatus> itor = listLocationStatus(lf.create(status.getUri()));
        while (itor.hasNext()) {
          statusStack.add(0, itor.next());
        }
      }
    }
    return processor.getResult();
  }

  /**
   * Tries to create a hardlink to the given {@link Location} if it is on the local file system. If creation
   * of the hardlink failed or if the Location is not local, it will copy the the location to the given target path.
   * Unlike {@link #linkOrCopy(Location, File)} will try to overwrite target file if it already exists.
   *
   * @param location location to hardlink or copy from
   * @param targetPath the target file path
   * @return the target path
   * @throws IOException if copying failed
   */
  public static File linkOrCopyOverwrite(Location location, File targetPath) throws IOException {
    targetPath.delete();
    return linkOrCopy(location, targetPath);
  }

  /**
   * Tries to create a hardlink to the given {@link Location} if it is on the local file system. If creation
   * of the hardlink failed or if the Location is not local, it will copy the the location to the given target path.
   *
   * @param location location to hardlink or copy from
   * @param targetPath the target file path
   * @return the target path
   * @throws IOException if copying failed or file already exists
   */
  public static File linkOrCopy(Location location, File targetPath) throws IOException {
    Files.createDirectories(targetPath.toPath().getParent());
    URI uri = location.toURI();
    if ("file".equals(uri.getScheme())) {
      try {
        Files.createLink(targetPath.toPath(), Paths.get(uri));
        return targetPath;
      } catch (Exception e) {
        // Ignore. Fallback to copy
      }
    }
    if (location instanceof LinkableLocation) {
      try {
        if (((LinkableLocation) location).tryLink(targetPath.toPath())) {
          return targetPath;
        }
      } catch (Exception e) {
        // Ignore. Fallback to copy
      }
    }

    try (InputStream is = location.getInputStream()) {
      Files.copy(is, targetPath.toPath());
    }

    return targetPath;
  }

  /**
   * Unpack a {@link Location} that represents a archive file into the given target directory.
   *
   * @param archive an archive file. Only .zip, .jar, .tar.gz, .tgz, and .tar are supported
   * @param targetDir the target directory to have the archive file unpacked into
   * @throws IOException if failed to unpack the archive file
   */
  public static void unpack(Location archive, File targetDir) throws IOException {
    DirUtils.mkdirs(targetDir);

    String extension = FileUtils.getExtension(archive.getName()).toLowerCase();
    switch (extension) {
      case "zip":
      case "jar":
        BundleJarUtil.unJar(archive, targetDir);
        break;
      case "gz":
        // gz is not recommended for archiving multiple files together. So we only support .tar.gz
        Preconditions.checkArgument(archive.getName().endsWith(".tar.gz"), "'.gz' format is not supported for " +
          "archiving multiple files. Please use 'zip', 'jar', '.tar.gz', 'tgz' or 'tar'.");
        try (InputStream is = archive.getInputStream()) {
          expandTgz(is, targetDir);
        }
        break;
      case "tgz":
        try (InputStream is = archive.getInputStream()) {
          expandTgz(is, targetDir);
        }
        break;
      case "tar":
        try (InputStream is = archive.getInputStream()) {
          expandTar(is, targetDir);
        }
        break;
      default:
        throw new IOException(String.format("Unsupported compression type '%s'. Only 'zip', 'jar', " +
                                              "'tar.gz', 'tgz' and 'tar'  are supported.", extension));
    }
  }

  /**
   * Unpacks a tar input stream to the given target directory.
   */
  private static void expandTar(InputStream archiveStream, File targetDir) throws IOException {
    try (TarArchiveInputStream tis = new TarArchiveInputStream(archiveStream)) {
      expandTarStream(tis, targetDir);
    }
  }

  /**
   * Unpacks a tar gz input stream to the given target directory.
   */
  private static void expandTgz(InputStream archiveStream, File targetDir) throws IOException {
    try (TarArchiveInputStream tis = new TarArchiveInputStream(new GZIPInputStream(archiveStream))) {
      expandTarStream(tis, targetDir);
    }
  }

  /**
   * Unpacks a tar input stream to the given target directory.
   */
  private static void expandTarStream(TarArchiveInputStream tis, File targetDir) throws IOException {
    TarArchiveEntry entry = tis.getNextTarEntry();
    while (entry != null) {
      File output = new File(targetDir, new File(entry.getName()).getName());
      if (entry.isDirectory()) {
        DirUtils.mkdirs(output);
      } else {
        DirUtils.mkdirs(output.getParentFile());
        ByteStreams.copy(tis, com.google.common.io.Files.newOutputStreamSupplier(output));
      }
      entry = tis.getNextTarEntry();
    }
  }

  /**
   * Returns a {@link LocationStatus} describing the status of the given {@link Location}.
   */
  private static LocationStatus getLocationStatus(Location location) throws IOException {
    LocationFactory lf = location.getLocationFactory();
    if (lf instanceof FileContextLocationFactory) {
      return FILE_STATUS_TO_LOCATION_STATUS.apply(
        ((FileContextLocationFactory) lf).getFileContext().getFileLinkStatus(new Path(location.toURI())));
    }
    return LOCATION_TO_LOCATION_STATUS.apply(location);
  }

  /**
   * Returns {@link RemoteIterator} of {@link LocationStatus} under a directory
   * represented by the given {@link Location}.
   */
  private static RemoteIterator<LocationStatus> listLocationStatus(Location location) throws IOException {
    LocationFactory lf = location.getLocationFactory();
    if (lf instanceof FileContextLocationFactory) {
      FileContext fc = ((FileContextLocationFactory) lf).getFileContext();
      return transform(fc.listStatus(new Path(location.toURI())), FILE_STATUS_TO_LOCATION_STATUS);
    }
    return transform(asRemoteIterator(location.list().iterator()), LOCATION_TO_LOCATION_STATUS);
  }

  /**
   * Converts a {@link Iterator} into {@link RemoteIterator}.
   */
  private static <E> RemoteIterator<E> asRemoteIterator(final Iterator<? extends E> itor) {
    return new RemoteIterator<E>() {
      @Override
      public boolean hasNext() {
        return itor.hasNext();
      }

      @Override
      public E next() {
        return itor.next();
      }
    };
  }

  /**
   * Transform a {@link RemoteIterator} using a {@link FunctionWithException}.
   */
  private static <F, T> RemoteIterator<T> transform(final RemoteIterator<F> itor,
                                                    final FunctionWithException<F, T, IOException> transform) {
    return new RemoteIterator<T>() {
      @Override
      public boolean hasNext() throws IOException {
        return itor.hasNext();
      }

      @Override
      public T next() throws IOException {
        return transform.apply(itor.next());
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
    return location::getOutputStream;
  }

  /**
   * Return whether the location is the root location, meaning it has no parents.
   *
   * @param location the location to check
   * @return true if the location is the root location, false if not
   */
  public static boolean isRoot(Location location) {
    String path = location.toURI().getPath();
    return path == null || path.isEmpty() || "/".equals(path);
  }

  /**
   * Creates a {@link Location} instance which represents the parent of the given location.
   * Note: CDAP-13765 this method can return an invalid location if the parent is the root directory depending
   * on the implementation of the input Location. Consider calling {@link #isRoot(Location)} before
   * calling this.
   *
   * @param location location to extra parent from.
   * @return an instance representing the parent location or {@code null} if there is no parent.
   */
  @Nullable
  public static Location getParent(Location location) {
    // If it is root, return null
    if (isRoot(location)) {
      return null;
    }

    URI source = location.toURI();
    URI resolvedParent = URI.create(source.toString() + "/..").normalize();

    // TODO: (CDAP-13765) move this logic to a better place
    // NOTE: if there is a trailing slash at the end, rename(), getName() and other operations on file
    // does not work in MapR. so we remove the trailing slash (if any) at the end.
    // However, don't remove the trailing slash if this is a root directory, as a URI without a path is an invalid
    // location
    if (resolvedParent.toString().endsWith("/") && !"/".equals(resolvedParent.getPath())) {
      String parent = resolvedParent.toString();
      resolvedParent = URI.create(parent.substring(0, parent.length() - 1));
    }
    return location.getLocationFactory().create(resolvedParent);
  }

  /**
   * Get a Location using a specified location factory and absolute path
   * @param locationFactory locationFactory to create Location from given Path
   * @param absolutePath Path to be used for Location
   * @return Location resulting from absolute locationPath
   */
  public static Location getLocationFromAbsolutePath(LocationFactory locationFactory, String absolutePath) {
    URI homeURI = locationFactory.getHomeLocation().toURI();
    try {
      // Make the path absolute and normalize it so that it follows URI spec RFC-2396 before passing to location factory
      // Also replace multiple "/" with one "/" since URI spec actually doesn't allow multiples and it can messes
      // up the URI parsing
      String path = URI.create("/").resolve(absolutePath.replaceAll("/+", "/")).normalize().getPath();
      URI uri = new URI(homeURI.getScheme(), homeURI.getAuthority(), path, null, null);
      return locationFactory.create(uri);
    } catch (URISyntaxException e) {
      // Should not happen.
      throw Throwables.propagate(e);
    }
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
      throw new IOException("Failed to create directory at " + location);
    }
  }

  public static void deleteQuietly(Location location) {
    deleteQuietly(location, false);
  }

  public static void deleteQuietly(Location location, boolean recursive) {
    try {
      location.delete(recursive);
    } catch (IOException e) {
      LOG.error("IOException while deleting location {}", location, e);
    }
  }

  /**
   * Deletes the content of the given location, but keeping the location itself.
   */
  public static void deleteContent(Location location) {
    try {
      for (Location child : location.list()) {
        deleteQuietly(child, true);
      }
    } catch (IOException e) {
      LOG.error("IOException while deleting content of {}", location, e);
    }
  }

  /**
   * Converts the given file into a local {@link Location}.
   */
  public static Location toLocation(File file) {
    return LOCAL_LOCATION_FACTORY.create(file.getAbsoluteFile().toURI());
  }

  /**
   * Converts the given file into a local {@link Location}.
   */
  public static Location toLocation(java.nio.file.Path path) {
    return toLocation(path.toFile());
  }

  /**
   * Creates a {@link StreamSizeProvider} for determining the size of the given {@link FSDataInputStream}.
   */
  private static StreamSizeProvider createDFSStreamSizeProvider(final FileSystem fs, final boolean ownFileSystem,
                                                                final Path path, FSDataInputStream input) {

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

    return new CloseableStreamSizeProvider() {
      @Override
      public long size() throws IOException {
        // Try to determine the size using default provider
        long size = sizeFromFileSystem();
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

      @Override
      public void close() throws IOException {
        if (ownFileSystem) {
          fs.close();
        }
      }

      /**
       * @return the size of the file path as reported by the {@link FileSystem} or {@link -1} if the file is still
       *         opened.
       */
      private long sizeFromFileSystem() throws IOException {
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
  }

  /**
   * A tagging interface that extends from both {@link StreamSizeProvider} and {@link Closeable}.
   */
  private interface CloseableStreamSizeProvider extends StreamSizeProvider, Closeable {
    // no method defined in this interface
  }

  private Locations() {
  }
}
